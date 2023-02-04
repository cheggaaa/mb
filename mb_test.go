package mb

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

var ctx = context.Background()

func TestSync(t *testing.T) {
	b := New[int](0)
	if e := b.Add(ctx, 1); e != nil {
		t.Error(e)
	}
	if e := b.Add(ctx, 2, 3); e != nil {
		t.Error(e)
	}

	msgs, err := b.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(msgs) != 3 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}

	if e := b.Add(ctx, 4); e != nil {
		t.Error(e)
	}

	msgs, err = b.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(msgs) != 1 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}

	if e := b.Close(); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

	if e := b.Add(ctx, 4); e != ErrClosed {
		t.Errorf("Unexpected error value: %v", e)
	}

	msgs, err = b.Wait(ctx)
	if len(msgs) != 0 {
		t.Errorf("unexpected message count in batch: %v", len(msgs))
	}
	if err != ErrClosed {
		t.Errorf("Unexpected error value: %v", err)
	}
	// close twice
	if e := b.Close(); e != ErrClosed {
		t.Errorf("Unexpected error value: %v", e)
	}
}

func TestLimits(t *testing.T) {
	b := New[int](5)
	if e := b.Add(ctx, 1, 2); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}
	if e := b.Add(ctx, 3, 4, 5, 6, 7, 8); e != ErrTooManyMessages {
		t.Errorf("Unexpected error value: %v", e)
	}
	if e := b.Add(ctx, 3, 4); e != nil {
		t.Errorf("Unexpected error value: %v", e)
	}

	for i := 1; i <= 4; i++ {
		msgs, err := b.NewCond().WithMax(1).Wait(ctx)
		if err != nil {
			t.Error(err)
		}
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

func TestMinMaxNoFilter(t *testing.T) {
	b := New[int](0)

	var resCh = make(chan []int)
	var quit = make(chan bool)
	go func() {
		var (
			result []int
			err    error
		)
		for {
			if result, err = b.NewCond().WithMin(2).WithMax(3).Wait(ctx); len(result) == 0 && err == ErrClosed {
				quit <- true
				return
			}
			resCh <- result
		}
	}()
	b.Add(ctx, 1)
	b.Add(ctx, 2)
	result := <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(ctx, 3, 4, 5, 6)
	result = <-resCh
	if len(result) != 3 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(ctx, 7)
	result = <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Close()
	<-quit
}

func TestMinMaxFilter(t *testing.T) {
	b := New[int](0)

	var resCh = make(chan []int)
	var quit = make(chan bool)
	go func() {
		var (
			result []int
			err    error
		)
		for {
			if result, err = b.NewCond().WithMin(2).WithMax(3).WithFilter(func(v int) bool {
				return v < 10
			}).Wait(ctx); len(result) == 0 && err == ErrClosed {
				quit <- true
				return
			}
			resCh <- result
		}
	}()
	b.Add(ctx, 1)
	b.Add(ctx, 2)
	result := <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(ctx, 3, 4, 5, 6)
	result = <-resCh
	if len(result) != 3 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(ctx, 7)
	result = <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Add(ctx, 8, 10)
	select {
	case result = <-resCh:
		t.Errorf("unexpected result, should not return")
	case <-time.After(time.Millisecond * 50):
		break
	}
	b.Add(ctx, 9)
	result = <-resCh
	if len(result) != 2 {
		t.Errorf("Unexpected result: %v", result)
	}
	b.Close()
	<-quit
}

func TestWaitInapplicableWaiter(t *testing.T) {
	var (
		b    = New[int](0)
		quit = make(chan error)
		cond = b.NewCond().WithFilter(func(v int) bool {
			return v > 5
		})
		ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
	)
	defer cancel()
	go func() {
		_, err := cond.WaitOne(ctx)
		quit <- err
	}()
	time.Sleep(50 * time.Millisecond)
	err := b.Add(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = <-quit
	if err != context.DeadlineExceeded {
		t.Fatalf("incorrect error: %v", err)
	}
}

func TestWaitInapplicableWaiterThenApplicable(t *testing.T) {
	type retVal struct {
		val int
		err error
	}
	var (
		b                = New[int](0)
		quit             = make(chan retVal)
		inapplicableCond = b.NewCond().WithPriority(10).WithFilter(func(v int) bool {
			return v > 5
		})
		applicableCond = b.NewCond().WithPriority(1).WithFilter(func(v int) bool {
			return v == 1
		})
	)

	go func() {
		_, err := inapplicableCond.WaitOne(ctx)
		quit <- retVal{-1, err}
	}()
	time.Sleep(50 * time.Millisecond)
	b.Add(ctx, 1)
	go func() {
		msg, err := applicableCond.WaitOne(ctx)
		quit <- retVal{msg, err}
	}()
	ret := <-quit
	if ret.val == -1 {
		t.Fatalf("incorrect waiter received, error: %v", ret.err)
	}
	if ret.err != nil {
		t.Fatalf("waiter should not return error: %v", ret.err)
	}
}

func TestWaitSimultaneousDifferentWaiters(t *testing.T) {
	var (
		midPriorityBatch = make(chan []int)
		lowPriorityBatch = make(chan []int)
		b                = New[int](0)

		highPriorityCond = b.NewCond().WithPriority(3).WithFilter(func(v int) bool {
			return v > 5
		})
		midPriorityCond = b.NewCond().WithPriority(2).WithMax(1).WithFilter(func(v int) bool {
			return v < 5
		})
		lowPriorityCond = b.NewCond().WithPriority(1).WithFilter(func(v int) bool {
			return v < 5
		})
	)
	eq := func(a, b []int) bool {
		if len(a) != len(b) {
			return false
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	go func() {
		highPriorityCond.WaitOne(ctx)
	}()
	go func() {
		msgs, err := midPriorityCond.Wait(ctx)
		if err == nil {
			midPriorityBatch <- msgs
		}
	}()
	go func() {
		msgs, err := lowPriorityCond.Wait(ctx)
		if err == nil {
			lowPriorityBatch <- msgs
		}
	}()
	time.Sleep(50 * time.Millisecond)
	b.Add(ctx, 1, 2, 3)

	midMsgs := <-midPriorityBatch
	lowMsgs := <-lowPriorityBatch

	if !eq(midMsgs, []int{1}) || !eq(lowMsgs, []int{2, 3}) {
		t.Errorf("incorrect order of readers")
	}
}

func TestReleaseWithNoWait(t *testing.T) {
	// this test is rather synthetic, because it is hard to reproduce the case
	var b = New[int](0)
	var quit = make(chan error)

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	go func() {
		_, err := b.NewCond().WaitOne(ctx)
		quit <- err
	}()
	// waiting for client to start reading
	time.Sleep(50 * time.Millisecond)
	// locking mx to prevent waiter continuing
	b.mu.Lock()
	<-ctx.Done()
	b.waiters[0].data <- []int{1}
	b.mu.Unlock()
	err := <-quit
	if err != context.DeadlineExceeded {
		t.Errorf("unexpected error: %v", err)
	}
	buf := b.GetAll()
	if len(buf) != 1 || buf[0] != 1 {
		t.Errorf("should get waiters buffer")
	}
}

func TestGetAll(t *testing.T) {
	var b = New[int](0)
	var quit = make(chan bool)
	go func() {
		for {
			if r, _ := b.NewCond().WithMin(3).WithMax(3).Wait(ctx); len(r) == 0 {
				quit <- true
				return
			}
		}
	}()

	b.Add(ctx, 2, 2)
	if res := b.GetAll(); len(res) != 2 {
		t.Errorf("Unexpected result: %v", res)
	}
	b.Add(ctx, 2, 2)
	b.Close()
	if res := b.GetAll(); len(res) != 2 {
		t.Errorf("Unexpected result: %v", res)
	}
	<-quit
}

func TestTryAdd(t *testing.T) {
	b := New[int](3)
	if err := b.TryAdd(1, 2, 3); err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if err := b.TryAdd(4); err != ErrOverflowed {
		t.Errorf("unexpected err: %v; want ErrOverflowed", err)
	}
}

func TestPause(t *testing.T) {
	b := New[int](10)
	b.Add(ctx, 1, 2, 3)
	var result = make(chan int)
	go func() {
		for {
			msgs, _ := b.Wait(ctx)
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

	b.Add(ctx, 1, 2)
	select {
	case l := <-result:
		if l != 2 {
			t.Errorf("Unexpected msgs len: %d vs %d", l, 2)
		}
	case <-time.After(time.Millisecond):
		t.Error("Can't receive msgs")
	}
	b.Pause()
	b.Add(ctx, 1, 2, 3, 4)
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
	b.Add(ctx, 1)
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
	mb := New[int](0)

	for i := 0; i < n; i++ {
		go func(p float64) {
			var count int
			for {
				msgs, err := mb.NewCond().WithPriority(p).WithMin(1).WithMax(1).Wait(ctx)
				if err != nil {
					break
				} else {
					count += len(msgs)
					time.Sleep(time.Millisecond * 10)
					break
				}
			}
			resultsCh <- result{priority: p, count: count}
		}(float64(i))
	}
	time.Sleep(time.Millisecond * 10)

	for i := 0; i < 100; i++ {
		mb.Add(ctx, i)
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

func TestTimeLimit(t *testing.T) {
	mb := New[int](0)
	defer mb.Close()
	mb.Add(ctx, 1, 3, 4, 5, 6)

	ctx = CtxWithTimeLimit(ctx, time.Millisecond*100)
	cond := mb.NewCond().WithMin(3).WithMax(3)
	res, err := cond.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(res) != 3 {
		t.Error("should be 3")
	}
	res, err = cond.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(res) != 2 {
		t.Error("should be 2")
	}
	var done = make(chan []int)
	go func() {
		res, _ = cond.Wait(ctx)
		done <- res
	}()
	time.Sleep(time.Millisecond * 200)
	mb.Add(ctx, 6)
	res = <-done
	if len(res) != 1 {
		t.Error("should be 1")
	}
}

func TestCtxWait(t *testing.T) {
	mb := New[int](0)
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	// wait deadline
	_, err := mb.Wait(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("should be deadline error, but got: %v", err)
	}
	// already deadlined context
	_, err = mb.Wait(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("should be deadline error, but got: %v", err)
	}
}

func TestCtxAdd(t *testing.T) {
	mb := New[int](5)
	mb.Add(ctx, 1, 2, 3)
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*10)
	defer cancel()
	err := mb.Add(ctx, 4, 5, 6)
	if err != context.DeadlineExceeded {
		t.Errorf("should be deadline error, but got: %v", err)
	}
}

func TestWaitOne(t *testing.T) {
	mb := New[int](5)
	mb.Add(ctx, 1)
	res, err := mb.WaitOne(ctx)
	if err != nil {
		t.Error(err)
	}
	if res != 1 {
		t.Errorf("expected 1, but got %d", res)
	}
	if err = mb.Close(); err != nil {
		t.Error(err)
	}
	_, err = mb.WaitOne(ctx)
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, but got %v", err)
	}
}

func TestFilter(t *testing.T) {
	mb := New[int](0)
	mb.Add(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	defer mb.Close()

	cond1 := mb.NewCond().WithFilter(func(v int) bool {
		return v < 5
	})
	cond2 := mb.NewCond().WithFilter(func(v int) bool {
		return v >= 5
	})

	res, err := cond2.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if fmt.Sprint(res) != "[5 6 7 8 9]" {
		t.Errorf("unexpected condition result: %v", res)
	}

	res, err = cond1.Wait(ctx)
	if err != nil {
		t.Error(err)
	}
	if fmt.Sprint(res) != "[1 2 3 4]" {
		t.Errorf("unexpected condition result: %v", res)
	}

}

func TestAsync(t *testing.T) {
	test(t, New[int](0), 4, 4, time.Millisecond*5)
	test(t, New[int](10), 4, 4, time.Millisecond*5)
	test(t, New[int](100), 1, 4, time.Millisecond*5)
	test(t, New[int](100), 4, 1, time.Millisecond*5)
	test(t, New[int](1000), 16, 16, time.Millisecond*30)
}

func test(t *testing.T, b *MB[int], sc, rc int, dur time.Duration) {
	exit := make(chan bool)
	var addCount, receiveCount int64

	// start add workers
	for i := 0; i < sc; i++ {
		go func(w int) {
			for {
				if e := b.Add(ctx, w); e != nil {
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
			var (
				msgs []int
				err  error
			)
			for {
				if rand.Intn(10) < 3 {
					ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
					msgs, err = b.Wait(ctx)
					cancel()
				} else {
					msgs, err = b.Wait(ctx)
				}
				if err == ErrClosed {
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
	mb := New[bool](0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mb.Add(ctx, true)
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
	mb := New[bool](1000)
	go func() {
		for {
			if e := mb.Add(ctx, true); e != nil {
				return
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	cond := mb.NewCond().WithMax(max)
	for i := 0; i < b.N; i++ {
		cond.Wait(ctx)
	}
	b.StopTimer()
	mb.Close()
}

func BenchmarkWaitPriority(b *testing.B) {
	mb := New[bool](1000)
	defer mb.Close()

	var receivedCh = make(chan struct{})

	for i := 0; i < 100; i++ {
		go func(p float64) {
			cond := mb.NewCond().WithPriority(p).WithMin(1).WithMax(1)
			for {
				if msgs, _ := cond.Wait(ctx); len(msgs) == 0 {
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
		mb.Add(ctx, true)
		<-receivedCh
	}
}
