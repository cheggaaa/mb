// Package mb - queue with message batching feature
package mb

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed is returned when you add message to closed queue
var ErrClosed = errors.New("mb: MB closed")

// ErrTooManyMessages means that adding more messages (at one call) than the limit
var ErrTooManyMessages = errors.New("mb: too many messages")

// ErrOverflowed means new messages can't be added until there is free space in the queue
var ErrOverflowed = errors.New("mb: overflowed")

var defaultPriority float64 = 1

// New returns a new MB with given queue size.
// size <= 0 means unlimited
func New[T any](size int) *MB[T] {
	mb := &MB[T]{
		size: size,
	}
	return mb
}

func newWaiter[T any]() *waiter[T] {
	return &waiter[T]{
		data: make(chan []T, 1),
	}
}

type waiter[T any] struct {
	data     chan []T
	priority float64
	min      int
	max      int
	inUse    bool
}

// MB - message batching object
type MB[T any] struct {
	buf  []T
	mu   sync.Mutex
	size int

	addUnlock chan struct{}

	waiters []*waiter[T]

	paused, closed bool

	addCount, getCount         int64
	addMsgsCount, getMsgsCount int64
}

// Wait until anybody add message
// Returning array of accumulated messages
// When queue will be closed length of array will be 0
func (mb *MB[T]) Wait(ctx context.Context) (msgs []T, err error) {
	return mb.WaitMinMax(ctx, 0, 0)
}

// WaitOne it's an alias for WaitMax(1)
func (mb *MB[T]) WaitOne(ctx context.Context) (msg T, err error) {
	msgs, err := mb.WaitMax(ctx, 1)
	if err != nil {
		return
	}
	return msgs[0], nil
}

// WaitMax it's Wait with limit of maximum returning array size
func (mb *MB[T]) WaitMax(ctx context.Context, max int) (msgs []T, err error) {
	return mb.WaitMinMax(ctx, 0, max)
}

// WaitMin it's Wait with limit of minimum returning array size
func (mb *MB[T]) WaitMin(ctx context.Context, min int) (msgs []T, err error) {
	return mb.WaitMinMax(ctx, min, 0)
}

// WaitMinMax it's Wait with limit of minimum and maximum returning array size
// value <= 0 means no limit
func (mb *MB[T]) WaitMinMax(ctx context.Context, min, max int) (msgs []T, err error) {
	return mb.PriorityWaitMinMax(ctx, defaultPriority, min, max)
}

// PriorityWait waits new messages
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWait(ctx context.Context, priority float64) (msgs []T, err error) {
	return mb.PriorityWaitMinMax(ctx, priority, 1, 0)
}

// PriorityWaitOne it's an alias for PriorityWaitMax(1)
func (mb *MB[T]) PriorityWaitOne(ctx context.Context, priority float64) (msg T, err error) {
	msgs, err := mb.PriorityWaitMax(ctx, priority, 1)
	if err != nil {
		return
	}
	return msgs[0], nil
}

// PriorityWaitMin waits new message with given min limit
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMin(ctx context.Context, priority float64, min int) (msgs []T, err error) {
	return mb.PriorityWaitMinMax(ctx, priority, min, 0)
}

// PriorityWaitMax waits new message with given max limit
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMax(ctx context.Context, priority float64, max int) (msgs []T, err error) {
	return mb.PriorityWaitMinMax(ctx, priority, 1, max)
}

// PriorityWaitMinMax waits new messages with given params
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMinMax(ctx context.Context, priority float64, min, max int) (msgs []T, err error) {
	mb.mu.Lock()
	if min < 1 {
		min = 1
	}
checkBuf:
	if !mb.paused {
		select {
		case <-ctx.Done():
			mb.mu.Unlock()
			return nil, ctx.Err()
		default:
		}
		// check the work without waiter
		bufLen := len(mb.buf)
		if min <= bufLen {
			if max <= 0 || max >= bufLen {
				msgs = mb.buf
				mb.buf = nil
				mb.getMsgsCount += int64(len(msgs))
				mb.unlockAdd()
				mb.mu.Unlock()
				return
			} else {
				msgs = mb.buf[:max]
				mb.buf = mb.buf[max:]
				mb.getMsgsCount += int64(len(msgs))
				mb.unlockAdd()
				mb.mu.Unlock()
				return
			}
		}
	}
	if mb.closed {
		mb.mu.Unlock()
		return nil, ErrClosed
	}
	w := mb.allocWaiter()
	w.priority = priority
	w.min = min
	w.max = max
	mb.mu.Unlock()

wait:
	var timeLimit <-chan time.Time
	if dur := getMBTimeLimit(ctx); dur > 0 {
		timeLimit = time.After(dur)
	}
	var ok bool
	select {
	case msgs, ok = <-w.data:
		if !ok {
			return nil, ErrClosed
		}
		return msgs, nil
	case <-timeLimit:
		mb.mu.Lock()
		if len(mb.buf) > 0 {
			min = 1
			mb.releaseWaiter(w)
			goto checkBuf
		} else {
			w.min = 1
			mb.mu.Unlock()
			goto wait
		}
	case <-ctx.Done():
		mb.mu.Lock()
		mb.releaseWaiter(w)
		mb.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (mb *MB[T]) allocWaiter() *waiter[T] {
	for _, w := range mb.waiters {
		if !w.inUse {
			w.inUse = true
			return w
		}
	}
	w := newWaiter[T]()
	w.inUse = true
	mb.waiters = append(mb.waiters, w)
	return w
}

func (mb *MB[T]) releaseWaiter(w *waiter[T]) {
	if len(w.data) == 1 {
		// messages have been sent to the waiter, so let's return these to buf
		mb.buf = append(<-w.data, mb.buf...)
	}
	w.inUse = false
}

// GetAll return all messages and flush queue
// Works on closed queue
func (mb *MB[T]) GetAll() (msgs []T) {
	mb.mu.Lock()
	msgs = mb.buf
	mb.buf = nil
	mb.getCount++
	mb.getMsgsCount += int64(len(msgs))
	mb.mu.Unlock()
	return
}

// Add - adds new messages to queue.
// When queue is closed - returning ErrClosed
// When count messages bigger then queue size - returning ErrTooManyMessages
// When the queue is full - wait until will free place
func (mb *MB[T]) Add(ctx context.Context, msgs ...T) (err error) {
	for {
		mb.mu.Lock()
		if err = mb.add(msgs...); err != nil {
			if err == ErrOverflowed {
				err = nil
				if mb.addUnlock == nil {
					mb.addUnlock = make(chan struct{})
				}
				addUnlock := mb.addUnlock
				mb.mu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-addUnlock:
				}
				continue
			} else {
				mb.mu.Unlock()
				return
			}
		} else {
			mb.mu.Unlock()
			return
		}
	}
}

func (mb *MB[T]) unlockAdd() {
	if mb.addUnlock != nil {
		close(mb.addUnlock)
		mb.addUnlock = nil
	}
}

// TryAdd - adds new messages to queue.
// When queue is closed - returning ErrClosed
// When count messages bigger then queue size - returning ErrTooManyMessages
// When the queue is full - returning ErrOverflowed
func (mb *MB[T]) TryAdd(msgs ...T) (err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.add(msgs...)
}

func (mb *MB[T]) add(msgs ...T) (err error) {
	if mb.closed {
		return ErrClosed
	}
	if mb.size > 0 && len(msgs) > mb.size {
		return ErrTooManyMessages
	}
	if mb.size > 0 && len(msgs)+len(mb.buf) > mb.size {
		return ErrOverflowed
	}
	// add to buf
	mb.buf = append(mb.buf, msgs...)
	if !mb.paused {
		mb.trySendHeap()
	}
	return
}

func (mb *MB[T]) trySendHeap() {
	var bestWaiter *waiter[T]
	var sent bool
	for {
		heapLen := len(mb.buf)
		if heapLen == 0 {
			break
		}
		for _, w := range mb.waiters {
			if w.inUse && (bestWaiter == nil || w.priority > bestWaiter.priority) {
				if w.min > 0 && w.min <= heapLen {
					bestWaiter = w
				}
			}
		}
		if bestWaiter != nil {
			if bestWaiter.max <= 0 || bestWaiter.max >= heapLen {
				toSend := mb.buf
				bestWaiter.data <- toSend
				mb.getMsgsCount += int64(len(toSend))
				mb.buf = nil
				bestWaiter.inUse = false
			} else {
				toSend := mb.buf[:bestWaiter.max]
				bestWaiter.data <- toSend
				mb.getMsgsCount += int64(len(toSend))
				mb.buf = mb.buf[bestWaiter.max:]
				bestWaiter.inUse = false
			}
			sent = true
			bestWaiter = nil
		} else {
			break
		}
	}
	if sent {
		mb.unlockAdd()
	}
}

// Pause lock all "Wait" routines until call Resume
func (mb *MB[T]) Pause() {
	mb.mu.Lock()
	mb.paused = true
	mb.mu.Unlock()
}

// Resume release all "Wait" routines
func (mb *MB[T]) Resume() {
	mb.mu.Lock()
	mb.trySendHeap()
	mb.paused = false
	mb.mu.Unlock()
}

// Len returning current size of queue
func (mb *MB[T]) Len() (l int) {
	mb.mu.Lock()
	l = len(mb.buf)
	mb.mu.Unlock()
	return
}

// Stats returning current statistic of queue usage
// addCount - count of calls Add
// addMsgsCount - count of added messages
// getCount - count of calls Wait
// getMsgsCount - count of issued messages
func (mb *MB[T]) Stats() (addCount, addMsgsCount, getCount, getMsgsCount int64) {
	mb.mu.Lock()
	addCount, addMsgsCount, getCount, getMsgsCount =
		mb.addCount, mb.addMsgsCount, mb.getCount, mb.getMsgsCount
	mb.mu.Unlock()
	return
}

// Close closes the queue
// All added messages will be available for active Wait
// When queue is paused, messages do not be released for Wait (use GetAll for fetching them)
func (mb *MB[T]) Close() (err error) {
	mb.mu.Lock()
	if mb.closed {
		mb.mu.Unlock()
		return ErrClosed
	}
	mb.closed = true
	if !mb.paused {
		mb.trySendHeap()
	}
	for _, w := range mb.waiters {
		close(w.data)
	}
	mb.mu.Unlock()
	return
}
