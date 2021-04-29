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

func TestMinMax(t *testing.T) {
	b := New(0)

	var resCh = make(chan []interface{})
	var quit = make(chan bool)
	go func() {
		var result []interface{}
		for {
			if result = b.WaitMinMax(2, 3); len(result) == 0 {
				quit <- true
				return
			}
			resCh <- result
		}
	}()

	b.Add(1)
	b.Add(2)
	result := <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(3, 4, 5, 6)
	result = <-resCh
	if len(result) != 3 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(7)
	result = <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Close()
	<-quit
}

func TestGetAll(t *testing.T) {
	var b = New(0)
	var quit = make(chan bool)
	go func() {
		for {
			if r := b.WaitMinMax(3, 3); len(r) == 0 {
				quit <- true
				return
			}
		}
	}()

	b.Add(2, 2)
	if res := b.GetAll(); len(res) != 2 {
		t.Errorf("Unexpected result: %v", res)
	}
	b.Add(2, 2)
	b.Close()
	if res := b.GetAll(); len(res) != 2 {
		t.Errorf("Unexpected result: %v", res)
	}
	<-quit
}

func TestTryAdd(t *testing.T) {
	b := New(3)
	if err := b.TryAdd(1, 2, 3); err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if err := b.TryAdd(4); err != ErrOverflowed {
		t.Errorf("unexpected err: %v; want ErrOverflowed", err)
	}
}

func TestPause(t *testing.T) {
	b := New(10)
	b.Add(1, 2, 3)
	var result = make(chan int)
	go func() {
		for {
			msgs := b.Wait()
			result <- len(msgs)
			if len(msgs) == 0 {
				return
			}
		}
	}()

	select {
	case l := <-result:
		if l != 3 {
			t.Errorf("Unexpected msgs len: %d vs %d", l, 3)
		}
	case <-time.After(time.Millisecond):
		t.Error("Can't receive msgs")
	}

	b.Add(1, 2)
	select {
	case l := <-result:
		if l != 2 {
			t.Errorf("Unexpected msgs len: %d vs %d", l, 2)
		}
	case <-time.After(time.Millisecond):
		t.Error("Can't receive msgs")
	}
	b.Pause()
	b.Add(1, 2, 3, 4)
	select {
	case <-result:
		t.Error("Pause do not work :-)")
	case <-time.After(time.Millisecond):
	}

	b.Resume()
	select {
	case l := <-result:
		if l != 4 {
			t.Errorf("Unexpected msgs len: %d vs %d", l, 4)
		}
	case <-time.After(time.Millisecond):
		t.Error("Resume do not work")
	}

	b.Pause()
	b.Add(1)
	select {
	case <-result:
		t.Error("Pause do not work :-)")
	case <-time.After(time.Millisecond):
	}

	b.Close()
	select {
	case l := <-result:
		if l != 0 {
			t.Errorf("Unexpected msgs len: %d vs %d", l, 0)
		}
	case <-time.After(time.Millisecond):
		t.Error("not closed")
	}
	if msgs := b.GetAll(); len(msgs) != 1 {
		t.Errorf("Unexpected msgs len: %d vs %d", len(msgs), 1)
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
	var addCount, receiveCount int64

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
				atomic.AddInt64(&receiveCount, int64(len(msgs)))
			}
		}(i)
	}

	time.Sleep(time.Microsecond * 10)
	b.Pause()
	time.Sleep(time.Millisecond)
	b.Resume()

	time.Sleep(dur)

	b.Close()

	for i := 0; i < sc+rc; i++ {
		<-exit
	}

	if addCount != receiveCount {
		t.Errorf("Add and receive not equals: %v vs %v", addCount, receiveCount)
	}
	t.Logf("Added: %d", addCount)
	t.Logf("received: %d", receiveCount)
}

func BenchmarkAdd(b *testing.B) {
	mb := New(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mb.Add(true)
	}
}

func BenchmarkWait0(b *testing.B) {
	benchmarkWait(b, 0)
}
func BenchmarkWait1(b *testing.B) {
	benchmarkWait(b, 1)
}
func BenchmarkWait10(b *testing.B) {
	benchmarkWait(b, 10)
}
func BenchmarkWait100(b *testing.B) {
	benchmarkWait(b, 100)
}
func BenchmarkWait1000(b *testing.B) {
	benchmarkWait(b, 1000)
}
func benchmarkWait(b *testing.B, max int) {
	mb := New(1000)
	b.StopTimer()
	b.ReportAllocs()
	go func() {
		for {
			if e := mb.Add(true); e != nil {
				return
			}
		}
	}()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mb.WaitMax(max)
	}
	b.StopTimer()
	mb.Close()
}
