package controller

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeTaskExpiration struct {
	now   time.Time
	calls int
	err   error
}

func (expiration *fakeTaskExpiration) ExpireTimedOutTasks(_ context.Context, now time.Time) (int, error) {
	expiration.calls++
	expiration.now = now
	return 2, expiration.err
}

func TestSchedulerTickExpiresOverdueTasks(t *testing.T) {
	wantNow := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	expiration := &fakeTaskExpiration{}
	scheduler := &Scheduler{Server: &Server{}, Interval: time.Second, tasks: expiration, now: func() time.Time {
		return wantNow
	}}
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick() error = %v", err)
	}
	if expiration.calls != 1 || !expiration.now.Equal(wantNow) {
		t.Fatalf("ExpireTimedOutTasks() calls/time = %d/%s", expiration.calls, expiration.now)
	}
}

func TestSchedulerTickHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (&Scheduler{tasks: &fakeTaskExpiration{}, now: time.Now}).tick(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tick() error = %v, want context.Canceled", err)
	}
}
