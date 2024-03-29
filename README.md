# Message batching queue
This package very useful for organizing batch messages.   
Can help you create batch inserts to a database for example. Thread safe and well tested.   

```Go
// create new queue object
batch := mb.New((any)(nil), 0)

// add new message to the queue
batch.Add(msg)

// wait until anybody add message/messages
// will return the slice of all queued messages. ([]T)
messages := batch.Wait(ctx)

// wait until count of messages will be more than 10
// if we have more than 100 messages, will be returned only 100
messages := batch.NewCond().WithMin(10).WithMax(100).Wait(ctx)

// when we have 0 messages returned that means the queue is closed.
if len(messages) == 0 {
	return
}

// close queue
// if the queue has messages all receivers will get remaining data.
batch.Close()
```

### Docs ###
https://godoc.org/github.com/cheggaaa/mb/v3
### Installation ###
```go get -u github.com/cheggaaa/mb/v3```

### Example ###

```Go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cheggaaa/mb/v3"
)

func main() {
	ctx := context.Background()

	// create the queue with 10 items capacity
	q := mb.New[int](10)

	// create the channel for showing when all work will be done
	done := make(chan bool)

	// start two workers
	go worker(ctx, "first", q, done)
	go worker(ctx, "second", q, done)

	// start two publishers
	go publisher(ctx, "first", q)
	go publisher(ctx, "second", q)

	// give time to work
	time.Sleep(time.Second)

	// close the queue
	q.Close()

	// and wait until all sent messages will be processed
	for i := 0; i < 2; i++ {
		<-done
	}
}

func publisher(ctx context.Context, name string, q *mb.MB[string]) {
	fmt.Printf("Publisher %s: started\n", name)
	var i int
	for {
		// will sending name and counter
		msg := fmt.Sprintf("%s - %d", name, i)
		// add
		if err := q.Add(ctx, msg); err != nil {
			// non-nil err mean that queue is closed
			break
		}
		// 10 messages per second
		time.Sleep(time.Second / 10)
		i++
	}
	fmt.Printf("Publisher %s: closed\n", name)
}

func worker(ctx context.Context, name string, q *mb.MB[string], done chan bool) {
	fmt.Printf("Worker %s: started\n", name)
	for {
		// getting messages
		msgs, err := q.Wait(ctx)
		if err != nil {
			break
		}

		msgsForPrint := ""
		for _, msg := range msgs {
			msgsForPrint += fmt.Sprintf("\t%s\n", msg)
		}
		fmt.Printf("Worker %s: %d messages received\n%s", name, len(msgs), msgsForPrint)

		// doing working, for example, send messages to remote server
		time.Sleep(time.Second / 3)
	}
	fmt.Printf("Worker %s: closed\n", name)
	done <- true
}

```

