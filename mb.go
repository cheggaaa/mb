// Package mb - queue with message batching feature
package mb

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrClosed is returned when you add message to closed queue
var ErrClosed = errors.New("mb: MB closed")

// ErrTooManyMessages means that adding more messages (at one call) than the limit
var ErrTooManyMessages = errors.New("mb: too many messages")

// ErrOverflowed means new messages can't be added until there is free space in the queue
var ErrOverflowed = errors.New("mb: overflowed")

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

// waiter states, guarded by MB.mu
const (
	// waiterFree - the waiter is in the pool and can be allocated
	waiterFree uint8 = iota
	// waiterInUse - allocated by a WaitCond call, no messages sent yet
	waiterInUse
	// waiterSent - a batch is sent to the data channel. The waiter must not be
	// reused until the channel is drained: either the consumer received the batch
	// (empty channel, checked by allocWaiter) or releaseWaiter returned the batch
	// to the buf. Otherwise a new consumer could steal a batch matched against
	// the previous condition, or trySendHeap could block on the full channel
	// while holding mb.mu.
	waiterSent
)

type waiter[T any] struct {
	data  chan []T
	state uint8
	cond  WaitCond[T]
}

type sortedWaiters[T any] []*waiter[T]

func (s sortedWaiters[T]) Len() int {
	return len(s)
}

func (s sortedWaiters[T]) Less(i, j int) bool {
	return s[i].cond.Priority > s[j].cond.Priority
}

func (s sortedWaiters[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// MB - message batching object
type MB[T any] struct {
	buf             []T
	mu              sync.Mutex
	size            int
	bufPrependCount int
	// addCount is kept on the same cache line as buf: both are written on every add,
	// so the hot Add path dirties a single line
	addCount int64

	addUnlock chan struct{}

	waiters []*waiter[T]
	// count of waiters in the inUse and not yet sent state; lets the hot Add path
	// skip the waiters scan entirely when nobody can receive
	readyWaiters int

	paused, closed bool

	getCount, getMsgsCount int64
}

// WaitCond describes condition for messages
type WaitCond[T any] struct {
	Priority float64
	Min      int
	Max      int
	Filter   func(v T) bool
	mb       *MB[T]
}

// WithMax adds max to conditions
func (wc WaitCond[T]) WithMax(max int) WaitCond[T] {
	c := wc
	c.Max = max
	return c
}

// WithMin adds min to conditions
func (wc WaitCond[T]) WithMin(min int) WaitCond[T] {
	c := wc
	c.Min = min
	return c
}

// WithPriority adds priority to conditions
func (wc WaitCond[T]) WithPriority(priority float64) WaitCond[T] {
	c := wc
	c.Priority = priority
	return c
}

// WithFilter adds filter to conditions
// filter function should return true for acceptable message and false for unacceptable
func (wc WaitCond[T]) WithFilter(f func(v T) bool) WaitCond[T] {
	c := wc
	c.Filter = f
	return c
}

// Wait waits messages with defined condition
func (wc WaitCond[T]) Wait(ctx context.Context) (msgs []T, err error) {
	return wc.mb.WaitCond(ctx, wc)
}

// WaitOne waits one message with defined condition
func (wc WaitCond[T]) WaitOne(ctx context.Context) (msg T, err error) {
	msgs, err := wc.mb.WaitCond(ctx, wc.WithMax(1))
	if err != nil {
		return
	}
	return msgs[0], nil
}

func (wc WaitCond[T]) getMessages(mb *MB[T]) (toReturn, keep []T) {
	keep = mb.buf
	if wc.Min < 1 {
		wc.Min = 1
	}
	bufLen := len(mb.buf)
	if wc.Min > bufLen {
		return
	}

	if wc.Filter == nil {
		if wc.Max <= 0 || wc.Max >= bufLen {
			toReturn = mb.buf
			keep = nil
			mb.bufPrependCount = 0
		} else {
			toReturn = mb.buf[:wc.Max]
			mb.bufPrependCount += wc.Max
			if mb.bufPrependCount > 10 {
				keep = make([]T, len(mb.buf[wc.Max:]))
				copy(keep, mb.buf[wc.Max:])
				mb.bufPrependCount = 0
			} else {
				keep = mb.buf[wc.Max:]
			}
		}
	} else {
		toReturn = make([]T, 0, bufLen)
		keep = make([]T, 0, bufLen)
		for _, v := range mb.buf {
			if (wc.Max <= 0 || len(toReturn) < wc.Max) && wc.Filter(v) {
				toReturn = append(toReturn, v)
			} else {
				keep = append(keep, v)
			}
		}
		if len(toReturn) > 0 && len(toReturn) < wc.Min {
			toReturn = nil
			keep = mb.buf
		}
	}
	return
}

// NewCond creates new condition object
func (mb *MB[T]) NewCond() WaitCond[T] {
	return WaitCond[T]{mb: mb}
}

// Wait until anybody add message
// Returning array of accumulated messages
func (mb *MB[T]) Wait(ctx context.Context) (msgs []T, err error) {
	return mb.WaitCond(ctx, WaitCond[T]{})
}

// WaitOne waits one message
func (mb *MB[T]) WaitOne(ctx context.Context) (msg T, err error) {
	return mb.NewCond().WaitOne(ctx)
}

// WaitCond waits new messages with given conditions
func (mb *MB[T]) WaitCond(ctx context.Context, cond WaitCond[T]) (msgs []T, err error) {
	mb.mu.Lock()

checkBuf:
	if !mb.paused {
		select {
		case <-ctx.Done():
			mb.mu.Unlock()
			return nil, ctx.Err()
		default:
		}
		// check the work without waiter
		if msgs, mb.buf = cond.getMessages(mb); len(msgs) > 0 {
			mb.getCount++
			mb.getMsgsCount += int64(len(msgs))
			mb.unlockAdd()
			mb.mu.Unlock()
			return
		}
	}
	if mb.closed {
		mb.mu.Unlock()
		return nil, ErrClosed
	}
	w := mb.allocWaiter()
	w.cond = cond
	// having waiters always priority sorted
	sort.Sort(sortedWaiters[T](mb.waiters))
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
		// the waiter is not touched here: after the batch is received,
		// allocWaiter reclaims it by observing the sent flag and the empty channel
		return msgs, nil
	case <-timeLimit:
		mb.mu.Lock()
		if len(mb.buf) > 0 {
			cond.Min = 1
			mb.releaseWaiter(w)
			goto checkBuf
		} else {
			w.cond.Min = 1
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
	mb.readyWaiters++
	for _, w := range mb.waiters {
		// released by the consumer, or the sent batch is received and the consumer
		// is gone: it never touches the waiter after the channel receive
		if w.state == waiterFree || (w.state == waiterSent && len(w.data) == 0) {
			w.state = waiterInUse
			return w
		}
	}
	w := newWaiter[T]()
	w.state = waiterInUse
	mb.waiters = append(mb.waiters, w)
	return w
}

func (mb *MB[T]) releaseWaiter(w *waiter[T]) {
	if w.state == waiterInUse {
		mb.readyWaiters--
	}
	if len(w.data) == 1 {
		// messages have been sent to the waiter but not received, so let's return these to buf
		msgs := <-w.data
		mb.buf = append(msgs, mb.buf...)
		mb.getCount--
		mb.getMsgsCount -= int64(len(msgs))
	}
	w.state = waiterFree
}

// GetAll return all messages and flush queue
// Works on closed queue
func (mb *MB[T]) GetAll() (msgs []T) {
	mb.mu.Lock()
	msgs = mb.buf
	mb.buf = nil
	mb.getCount++
	mb.getMsgsCount += int64(len(msgs))
	mb.unlockAdd()
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
	mb.addCount++
	if !mb.paused && mb.readyWaiters > 0 {
		mb.trySendHeap()
	}
	return
}

func (mb *MB[T]) trySendHeap() {
	var sent bool
	for {
		heapLen := len(mb.buf)
		if heapLen == 0 || mb.readyWaiters == 0 {
			break
		}
		for _, w := range mb.waiters {
			// they are already sorted by priority
			if (w.cond.Min < 1 || w.cond.Min <= heapLen) && w.state == waiterInUse {
				var msgs []T
				if msgs, mb.buf = w.cond.getMessages(mb); len(msgs) > 0 {
					mb.getCount++
					mb.getMsgsCount += int64(len(msgs))
					// an inUse waiter always has an empty channel, the send can't block
					w.data <- msgs
					w.state = waiterSent
					mb.readyWaiters--
					sent = true
					if len(mb.buf) == 0 || mb.readyWaiters == 0 {
						break
					}
					continue
				}
			}
		}
		// no messages processed
		if heapLen == len(mb.buf) {
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
	addCount, getCount, getMsgsCount = mb.addCount, mb.getCount, mb.getMsgsCount
	// every added message is either still in the buf or already counted as issued,
	// so the added messages count is derived instead of being tracked on the hot Add path
	addMsgsCount = getMsgsCount + int64(len(mb.buf))
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
	// wake up the Add calls blocked on the full queue, so they can return ErrClosed
	mb.unlockAdd()
	for _, w := range mb.waiters {
		close(w.data)
	}
	mb.mu.Unlock()
	return
}
