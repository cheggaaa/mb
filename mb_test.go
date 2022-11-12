package mb

import (
	"math/rand"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"
)

func TestSync(t *testing.T) {
	b := New(0, 0)
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
	b := New(0, 5)
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
		if msgs[0] != i {
			t.Error("Unexpected element:", msgs[0])
		}
	}
	if e := b.Close(); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

}

func TestMinMax(t *testing.T) {
	b := New(0, 0)

	var resCh = make(chan []int)
	var quit = make(chan bool)
	go func() {
		var result []int
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
	var b = New(0, 0)
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
	b := New(0, 3)
	if err := b.TryAdd(1, 2, 3); err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if err := b.TryAdd(4); err != ErrOverflowed {
		t.Errorf("unexpected err: %v; want ErrOverflowed", err)
	}
}

func TestPause(t *testing.T) {
	b := New(0, 10)
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

func TestPriority(t *testing.T) {
	type result struct {
		priority float64
		count    int
	}
	var resultsCh = make(chan result)
	var n = 3
	mb := New(1, 0)

	for i := 0; i < n; i++ {
		go func(p float64) {
			var count int
			for {
				if l := len(mb.PriorityWaitMinMax(p, 1, 1)); l != 0 {
					count += l
					//fmt.Println("receive", l, p)
					time.Sleep(time.Millisecond * 10)

				} else {
					break
				}
			}
			resultsCh <- result{priority: p, count: count}
		}(float64(i))
	}
	time.Sleep(time.Millisecond * 10)

	for i := 0; i < 100; i++ {
		mb.Add(i)
		time.Sleep(time.Millisecond * 5)
	}
	mb.Close()
	resMap := make(map[float64]int)
	for i := 0; i < n; i++ {
		res := <-resultsCh
		resMap[res.priority] = res.count
	}
	t.Log(resMap)
}

func TestAsync(t *testing.T) {
	test(t, New(0, 0), 4, 4, time.Millisecond*5)
	test(t, New(0, 10), 4, 4, time.Millisecond*5)
	test(t, New(0, 100), 1, 4, time.Millisecond*5)
	test(t, New(0, 100), 4, 1, time.Millisecond*5)
	test(t, New(0, 1000), 16, 16, time.Millisecond*30)
}

func test(t *testing.T, b *MB[int], sc, rc int, dur time.Duration) {
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
	mb := New(true, 0)
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
	mb := New(true, 1000)
	go func() {
		for {
			if e := mb.Add(true); e != nil {
				return
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mb.WaitMax(max)
	}
	b.StopTimer()
	mb.Close()
}

func BenchmarkWaitPriority(b *testing.B) {
	mb := New(true, 1000)
	defer mb.Close()

	var receivedCh = make(chan struct{})

	for i := 0; i < 100; i++ {
		go func(p float64) {
			for {
				if msgs := mb.PriorityWaitMinMax(p, 1, 1); len(msgs) == 0 {
					return
				} else {
					receivedCh <- struct{}{}
				}
			}
		}(rand.Float64())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mb.Add(true)
		<-receivedCh
	}
}
