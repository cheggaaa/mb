package mb

import (
	"context"
	"time"
)

type ctxKey uint

const (
	mbTimeLimit ctxKey = iota
)

// CtxWithTimeLimit makes child context with given timeLimit duration
// This context can be passed to all Wait methods.
// When a given min param can't be achieved within a time limit then a min param will be reseted
func CtxWithTimeLimit(ctx context.Context, timeLimit time.Duration) context.Context {
	return context.WithValue(ctx, mbTimeLimit, timeLimit)
}

func getMBTimeLimit(ctx context.Context) time.Duration {
	if val, ok := ctx.Value(mbTimeLimit).(time.Duration); ok {
		return val
	}
	return 0
}
