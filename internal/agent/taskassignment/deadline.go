package taskassignment

import (
	"context"
	"time"
)

func RemainingSeconds(ctx context.Context, maximum uint32) uint32 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return maximum
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 1
	}
	seconds := uint32((remaining + time.Second - 1) / time.Second)
	if seconds > maximum {
		return maximum
	}
	return seconds
}
