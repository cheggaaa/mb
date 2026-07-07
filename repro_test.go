package mb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestWaiterReuseStress stresses concurrent filtered waiters against a producer.
// Before waiter release was moved to the receiving side, trySendHeap marked a
// waiter reusable at send time, so a new consumer could be allocated the same
// waiter and steal a batch matched against the previous consumer's filter
// (data corruption), or trySendHeap could block on the full channel while
// holding mb.mu (deadlock). The test asserts full delivery with matching
// filters and fails by deadline instead of hanging if messages get lost.
func TestWaiterReuseStress(t *testing.T) {
	q := New[int](0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		consumers = 10
		num       = 1000
	)
	var mismatches, received int32
	for i := 0; i < consumers; i++ {
		go func(fv int) {
			for {
				msgs, err := q.NewCond().WithFilter(func(v int) bool {
					return v%consumers == fv
				}).Wait(ctx)
				if err != nil {
					return
				}
				for _, m := range msgs {
					if m%consumers != fv {
						atomic.AddInt32(&mismatches, 1)
					}
				}
				atomic.AddInt32(&received, int32(len(msgs)))
			}
		}(i)
	}

	for i := 0; i < num; i++ {
		if err := q.Add(ctx, i); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	deadline := time.After(5 * time.Second)
	for atomic.LoadInt32(&received) < num {
		select {
		case <-deadline:
			t.Fatalf("timeout: %d/%d messages delivered (lost messages or deadlock)", atomic.LoadInt32(&received), num)
		case <-time.After(time.Millisecond):
		}
	}
	if n := atomic.LoadInt32(&mismatches); n > 0 {
		t.Errorf("data corruption: %d messages delivered to a consumer whose filter did not match", n)
	}
}

// waitAddBlocked waits until an Add call is parked waiting for free space.
func waitAddBlocked(t *testing.T, q *MB[int]) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		q.mu.Lock()
		waiting := q.addUnlock != nil
		q.mu.Unlock()
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Add never blocked on the full queue")
		}
		time.Sleep(time.Millisecond)
	}
}

// assertBlockedAddUnblocks blocks an Add on a full queue, applies the trigger
// and checks that Add returns wantErr instead of hanging forever.
func assertBlockedAddUnblocks(t *testing.T, trigger func(q *MB[int]), wantErr error) {
	t.Helper()
	q := New[int](1)
	if err := q.Add(context.Background(), 1); err != nil {
		t.Fatalf("Add: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- q.Add(context.Background(), 2)
	}()
	waitAddBlocked(t, q)
	trigger(q)

	select {
	case err := <-errCh:
		if !errors.Is(err, wantErr) {
			t.Errorf("Add returned %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Error("Add is still blocked after the trigger")
	}
}

func TestAddUnblocksOnClose(t *testing.T) {
	assertBlockedAddUnblocks(t, func(q *MB[int]) { q.Close() }, ErrClosed)
}

func TestAddUnblocksOnGetAll(t *testing.T) {
	assertBlockedAddUnblocks(t, func(q *MB[int]) { q.GetAll() }, nil)
}

func TestStatsCounts(t *testing.T) {
	q := New[int](0)
	if err := q.Add(context.Background(), 1, 2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := q.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	addCount, addMsgsCount, getCount, getMsgsCount := q.Stats()
	if addCount != 1 {
		t.Errorf("expected addCount 1, got %d", addCount)
	}
	if addMsgsCount != 2 {
		t.Errorf("expected addMsgsCount 2, got %d", addMsgsCount)
	}
	if getCount != 1 {
		t.Errorf("expected getCount 1, got %d", getCount)
	}
	if getMsgsCount != 2 {
		t.Errorf("expected getMsgsCount 2, got %d", getMsgsCount)
	}
}
