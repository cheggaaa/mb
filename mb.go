// Package mb - queue with message batching feature
package mb

import (
	"errors"
	"sync"
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
	mb.addCond = sync.NewCond(&mb.mu)
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
// Implements queue.
// Based on condition variables
type MB[T any] struct {
	heap []T
	mu   sync.Mutex
	size int

	addCond *sync.Cond

	waiters []*waiter[T]

	paused, closed bool

	addCount, getCount         int64
	addMsgsCount, getMsgsCount int64
}

// Wait until anybody add message
// Returning array of accumulated messages
// When queue will be closed length of array will be 0
func (mb *MB[T]) Wait() (msgs []T) {
	return mb.WaitMinMax(0, 0)
}

// WaitMax it's Wait with limit of maximum returning array size
func (mb *MB[T]) WaitMax(max int) (msgs []T) {
	return mb.WaitMinMax(0, max)
}

// WaitMin it's Wait with limit of minimum returning array size
func (mb *MB[T]) WaitMin(min int) (msgs []T) {
	return mb.WaitMinMax(min, 0)
}

// WaitMinMax it's Wait with limit of minimum and maximum returning array size
// value <= 0 means no limit
func (mb *MB[T]) WaitMinMax(min, max int) (msgs []T) {
	return mb.PriorityWaitMinMax(defaultPriority, min, max)
}

// PriorityWait waits new messages
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWait(priority float64) (msgs []T) {
	return mb.PriorityWaitMinMax(priority, 1, 0)
}

// PriorityWaitMin waits new message with given min limit
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMin(priority float64, min int) (msgs []T) {
	return mb.PriorityWaitMinMax(priority, min, 0)
}

// PriorityWaitMax waits new message with given max limit
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMax(priority float64, max int) (msgs []T) {
	return mb.PriorityWaitMinMax(priority, 1, max)
}

// PriorityWaitMinMax waits new messages with given params
// data will be released to waiter with higher priority
func (mb *MB[T]) PriorityWaitMinMax(priority float64, min, max int) (msgs []T) {
	mb.mu.Lock()
	defer mb.addCond.Broadcast()
	if min < 1 {
		min = 1
	}
	if !mb.paused {
		// check the work without waiter
		heapLen := len(mb.heap)
		if min <= heapLen {
			if max <= 0 || max >= heapLen {
				msgs = mb.heap
				mb.heap = nil
				mb.getMsgsCount += int64(len(msgs))
				mb.mu.Unlock()
				return
			} else {
				msgs = mb.heap[:max]
				mb.heap = mb.heap[max:]
				mb.getMsgsCount += int64(len(msgs))
				mb.mu.Unlock()
				return
			}
		}
	}
	if mb.closed {
		mb.mu.Unlock()
		return
	}
	w := mb.allocWaiter()
	w.priority = priority
	w.min = min
	w.max = max
	mb.mu.Unlock()
	msgs, _ = <-w.data
	return
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

// GetAll return all messages and flush queue
// Works on closed queue
func (mb *MB[T]) GetAll() (msgs []T) {
	mb.mu.Lock()
	msgs = mb.heap
	mb.heap = nil
	mb.getCount++
	mb.getMsgsCount += int64(len(msgs))
	mb.mu.Unlock()
	return
}

// Add - adds new messages to queue.
// When queue is closed - returning ErrClosed
// When count messages bigger then queue size - returning ErrTooManyMessages
// When the queue is full - wait until will free place
func (mb *MB[T]) Add(msgs ...T) (err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for {
		if err = mb.add(msgs...); err != nil {
			if err == ErrOverflowed {
				err = nil
				mb.addCond.Wait()
				continue
			} else {
				return
			}
		} else {
			return
		}
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
	if mb.size > 0 && len(msgs)+len(mb.heap) > mb.size {
		return ErrOverflowed
	}
	// add to heap
	mb.heap = append(mb.heap, msgs...)
	if !mb.paused {
		mb.trySendHeap()
	}
	return
}

func (mb *MB[T]) trySendHeap() {
	var bestWaiter *waiter[T]
	for {
		heapLen := len(mb.heap)
		if heapLen == 0 {
			return
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
				toSend := mb.heap
				bestWaiter.data <- toSend
				mb.getMsgsCount += int64(len(toSend))
				mb.heap = nil
				bestWaiter.inUse = false
			} else {
				toSend := mb.heap[:bestWaiter.max]
				bestWaiter.data <- toSend
				mb.getMsgsCount += int64(len(toSend))
				mb.heap = mb.heap[bestWaiter.max:]
				bestWaiter.inUse = false
			}
			bestWaiter = nil
		} else {
			return
		}
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
	l = len(mb.heap)
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
