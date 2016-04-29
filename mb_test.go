package mb

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestSync(t *testing.T) {
	b := New(0)
	if e := b.Add(1); e != nil {
		t.Error(e)
	}
	if e := b.Add(2, 3); e != nil {
		t.Error(e)
	}

	msgs := b.Wait()
	if len(msgs) != 3 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}

	if e := b.Add(4); e != nil {
		t.Error(e)
	}

	msgs = b.Wait()
	if len(msgs) != 1 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}

	if e := b.Close(); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

	if e := b.Add(4); e != ErrClosed {
		t.Errorf("Unexpected error value: %v", e)
	}

	msgs = b.Wait()
	if len(msgs) != 0 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}
	// close twice
	if e := b.Close(); e != ErrClosed {
		t.Errorf("Unexpected error value: %v", e)
	}
}

func TestLimits(t *testing.T) {
	b := New(5)
	if e := b.Add(1, 2); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}
	if e := b.Add(3, 4, 5, 6, 7, 8); e != ErrTooManyMessages {
		t.Errorf("Unexpected error value: %v", e)
	}
	if e := b.Add(3, 4); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

	for i := 1; i <= 4; i++ {
		msgs := b.WaitMax(1)
		if len(msgs) != 1 {
			t.Errorf("Unexpected batch len: %v", len(msgs))
		}
		if msgs[0].(int) != i {
			t.Error("Unexpected element:", msgs[0])
		}
	}
	if e := b.Close(); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

}

func TestAsync(t *testing.T) {
	test(t, New(0), 4, 4, time.Millisecond*5)
	test(t, New(10), 4, 4, time.Millisecond*5)
	test(t, New(100), 1, 4, time.Millisecond*5)
	test(t, New(100), 4, 1, time.Millisecond*5)
	test(t, New(1000), 16, 16, time.Millisecond*30)
}

func test(t *testing.T, b *MB, sc, rc int, dur time.Duration) {
	exit := make(chan bool)
	var addCount, recieveCount int64

	// start add workers
	for i := 0; i < sc; i++ {
		go func(w int) {
			for {
				if e := b.Add(w); e != nil {
					exit <- true
					return
				}
				atomic.AddInt64(&addCount, 1)
			}
		}(i)
	}

	// start read workers
	for i := 0; i < rc; i++ {
		go func(w int) {
			for {
				msgs := b.Wait()
				if len(msgs) == 0 {
					exit <- true
					return
				}
				atomic.AddInt64(&recieveCount, int64(len(msgs)))
			}
		}(i)
	}

	time.Sleep(dur)

	b.Close()

	for i := 0; i < sc+rc; i++ {
		<-exit
	}

	if addCount != recieveCount {
		t.Errorf("Add and recieve not equals: %v vs %v", addCount, recieveCount)
	}
	t.Logf("Added: %d", addCount)
	t.Logf("Recieved: %d", recieveCount)
}
