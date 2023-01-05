package mb_test

import (
	"context"
	"fmt"
	"github.com/cheggaaa/mb/v3"
	"time"
)

type Item struct {
	Id int
}

// BatchInsert inserts items to db
func BatchInsert(items []Item) {
	time.Sleep(time.Millisecond * 200)
	fmt.Printf("inserted %d items\n", len(items))
}

func Example() {
	var ctx = context.Background()
	// bufSize - whole capacity of batcher
	var bufSize = 100
	// create the new batcher
	batcher := mb.New[Item](bufSize)

	// start goroutine that will wait items
	// it can be a lot of the wait goroutines
	var done = make(chan struct{})
	go func() {
		defer close(done)
		for {
			// wait items
			items, err := batcher.Wait(context.Background())
			if err != nil {
				fmt.Printf("waiter received error: %v; stop goroutine\n", err)
				return
			}
			// insert batch to db
			// while this func works, the batcher collects new item
			BatchInsert(items)
		}
	}()

	// add two items to batcher
	batcher.Add(ctx, Item{Id: 1}, Item{Id: 2})
	time.Sleep(time.Millisecond)
	// add more items to batcher
	for i := 0; i < 10; i++ {
		// it's safe to call Add from other goroutines
		batcher.Add(ctx, Item{Id: i + 3})
	}

	// close batcher
	batcher.Close()
	// and wait until inserter exits
	<-done

	// Output:
	// inserted 2 items
	// inserted 10 items
	// waiter received error: mb: MB closed; stop goroutine
}

func Example_withTimeLimit() {
	var ctx = context.Background()
	// bufSize - whole capacity of batcher
	var bufSize = 100
	// create the new batcher
	batcher := mb.New[Item](bufSize)

	// start goroutine that will wait items
	// it can be a lot of the wait goroutines
	var done = make(chan struct{})
	go func() {
		defer close(done)
		ctxWithTimeLimit := mb.CtxWithTimeLimit(ctx, time.Millisecond*200)
		cond := batcher.NewCond().WithMin(10).WithMax(15)
		for {
			// get at least 10 items or after 200 ms get at least 1 item
			items, err := cond.Wait(ctxWithTimeLimit)
			if err != nil {
				fmt.Printf("waiter received error: %v; stop goroutine\n", err)
				return
			}
			// insert batch to db
			// while this func works, the batcher collects new item
			BatchInsert(items)
		}
	}()
	// add two items to batcher
	batcher.Add(ctx, Item{Id: 1}, Item{Id: 2})
	time.Sleep(time.Millisecond * 300)
	// add more items to batcher
	for i := 0; i < 20; i++ {
		// it's safe to call Add from other goroutines
		batcher.Add(ctx, Item{Id: i + 3})
	}
	time.Sleep(time.Second)
	// close batcher
	batcher.Close()
	// and wait until inserter exits
	<-done

	// Output:
	// inserted 2 items
	// inserted 15 items
	// inserted 5 items
	// waiter received error: mb: MB closed; stop goroutine
}
