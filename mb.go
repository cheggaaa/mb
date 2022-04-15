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

// New returns a new MB with given queue size.
// size <= 0 means unlimited
func New[T any](elType T, size int) *MB[T] {
	return &MB[T]{
		cond: sync.NewCond(&sync.Mutex{}),
		size: size,
		read: make(chan struct{}),
	}
}

// MB - message batching object
// Implements queue.
// Based on condition variables
type MB[T any] struct {
	msgs []T
	cond *sync.Cond
	size int
	wait int
	read chan struct{}

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
// value < 0 means no limit
func (mb *MB[T]) WaitMinMax(min, max int) (msgs []T) {
	if min <= 0 {
		min = 1
	}
	mb.cond.L.Lock()
try:
	if len(mb.msgs) < min || mb.paused {
		if mb.closed {
			mb.cond.L.Unlock()
			return
		}
		mb.cond.Wait()
		goto try
	}
	if max > 0 && len(mb.msgs) > max {
		msgs = mb.msgs[:max]
		mb.msgs = mb.msgs[max:]
	} else {
		msgs = mb.msgs
		mb.msgs = make([]T, 0)
	}
	mb.getCount++
	mb.getMsgsCount += int64(len(msgs))
	mb.unlockAdd()
	mb.cond.L.Unlock()
	return
}

// GetAll return all messages and flush queue
// Works on closed queue
func (mb *MB[T]) GetAll() (msgs []T) {
	mb.cond.L.Lock()
	msgs = mb.msgs
	mb.msgs = make([]T, 0)
	mb.getCount++
	mb.getMsgsCount += int64(len(msgs))
	mb.unlockAdd()
	mb.cond.L.Unlock()
	return
}

// Add - adds new messages to queue.
// When queue is closed - returning ErrClosed
// When count messages bigger then queue size - returning ErrTooManyMessages
// When the queue is full - wait until will free place
func (mb *MB[T]) Add(msgs ...T) (err error) {
add:
	mb.cond.L.Lock()
	// check for close
	if mb.closed {
		mb.cond.L.Unlock()
		return ErrClosed
	}
	if mb.size > 0 && len(mb.msgs)+len(msgs) > mb.size {
		if len(msgs) > mb.size {
			mb.cond.L.Unlock()
			return ErrTooManyMessages
		}
		// limit reached
		mb.wait++
		mb.cond.L.Unlock()
		<-mb.read
		goto add
	}
	mb.msgs = append(mb.msgs, msgs...)
	mb.addCount++
	mb.addMsgsCount += int64(len(msgs))
	paused := mb.paused
	mb.cond.L.Unlock()
	if !paused {
		mb.cond.Signal()
	}
	return
}

// TryAdd - adds new messages to queue.
// When queue is closed - returning ErrClosed
// When count messages bigger then queue size - returning ErrTooManyMessages
// When the queue is full - returning ErrOverflowed
func (mb *MB[T]) TryAdd(msgs ...T) (err error) {
	mb.cond.L.Lock()
	// check for close
	if mb.closed {
		mb.cond.L.Unlock()
		return ErrClosed
	}
	if mb.size > 0 && len(mb.msgs)+len(msgs) > mb.size {
		if len(msgs) > mb.size {
			mb.cond.L.Unlock()
			return ErrTooManyMessages
		}
		mb.cond.L.Unlock()
		return ErrOverflowed
	}
	mb.msgs = append(mb.msgs, msgs...)
	mb.addCount++
	mb.addMsgsCount += int64(len(msgs))
	paused := mb.paused
	mb.cond.L.Unlock()
	if !paused {
		mb.cond.Signal()
	}
	return
}

func (mb *MB[T]) unlockAdd() {
	if mb.wait > 0 {
		for i := 0; i < mb.wait; i++ {
			mb.read <- struct{}{}
		}
		mb.wait = 0
	}
}

// Pause lock all "Wait" routines until call Resume
func (mb *MB[T]) Pause() {
	mb.cond.L.Lock()
	mb.paused = true
	mb.cond.L.Unlock()
}

// Resume release all "Wait" routines
func (mb *MB[T]) Resume() {
	mb.cond.L.Lock()
	wasPaused := mb.paused
	mb.paused = false
	mb.cond.L.Unlock()
	if wasPaused {
		mb.cond.Broadcast()
	}
}

// Len returning current size of queue
func (mb *MB[T]) Len() (l int) {
	mb.cond.L.Lock()
	l = len(mb.msgs)
	mb.cond.L.Unlock()
	return
}

// Stats returning current statistic of queue usage
// addCount - count of calls Add
// addMsgsCount - count of added messages
// getCount - count of calls Wait
// getMsgsCount - count of issued messages
func (mb *MB[T]) Stats() (addCount, addMsgsCount, getCount, getMsgsCount int64) {
	mb.cond.L.Lock()
	addCount, addMsgsCount, getCount, getMsgsCount =
		mb.addCount, mb.addMsgsCount, mb.getCount, mb.getMsgsCount
	mb.cond.L.Unlock()
	return
}

// Close closes the queue
// All added messages will be available for Wait
// When queue paused messages do not be released for Wait (use GetAll for fetching them)
func (mb *MB[T]) Close() (err error) {
	mb.cond.L.Lock()
	if mb.closed {
		mb.cond.L.Unlock()
		return ErrClosed
	}
	mb.closed = true
	mb.unlockAdd()
	mb.cond.L.Unlock()
	mb.cond.Broadcast()
	return
}
