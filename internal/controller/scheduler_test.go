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

type fakeIdempotencyPruning struct {
	now   time.Time
	calls int
	err   error
}

type fakeStaleAgentExpiration struct {
	now   time.Time
	calls int
	err   error
}

func (expiration *fakeStaleAgentExpiration) ExpireStaleAgentTasks(_ context.Context, now time.Time) (int, error) {
	expiration.calls++
	expiration.now = now
	return 1, expiration.err
}

func (pruning *fakeIdempotencyPruning) PruneExpired(_ context.Context, now time.Time) (int, error) {
	pruning.calls++
	pruning.now = now
	return 3, pruning.err
}

func (expiration *fakeTaskExpiration) ExpireTimedOutTasks(_ context.Context, now time.Time) (int, error) {
	expiration.calls++
	expiration.now = now
	return 2, expiration.err
}

func TestSchedulerTickExpiresOverdueTasks(t *testing.T) {
	wantNow := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	expiration := &fakeTaskExpiration{}
	pruning := &fakeIdempotencyPruning{}
	agents := &fakeStaleAgentExpiration{}
	now := wantNow
	scheduler := &Scheduler{
		Server: &Server{}, Interval: time.Second, tasks: expiration, idempotency: pruning, agents: agents,
		now: func() time.Time { return now },
	}
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick() error = %v", err)
	}
	if expiration.calls != 1 || !expiration.now.Equal(wantNow) ||
		pruning.calls != 1 || !pruning.now.Equal(wantNow) || agents.calls != 1 || !agents.now.Equal(wantNow) {
		t.Fatalf("ExpireTimedOutTasks() calls/time = %d/%s", expiration.calls, expiration.now)
	}
	now = wantNow.Add(dailyMaintenanceInterval - time.Second)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick(before daily prune) error = %v", err)
	}
	if expiration.calls != 2 || agents.calls != 2 || pruning.calls != 1 {
		t.Fatalf("before daily deadline expiration/stale/pruning calls = %d/%d/%d", expiration.calls, agents.calls, pruning.calls)
	}
	now = wantNow.Add(dailyMaintenanceInterval)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick(at daily prune) error = %v", err)
	}
	if expiration.calls != 3 || agents.calls != 3 || pruning.calls != 2 || !pruning.now.Equal(now) {
		t.Fatalf("at daily deadline expiration/stale/pruning calls/time = %d/%d/%d/%s", expiration.calls, agents.calls, pruning.calls, pruning.now)
	}
}

func TestSchedulerTickHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (&Scheduler{
		tasks: &fakeTaskExpiration{}, idempotency: &fakeIdempotencyPruning{},
		agents: &fakeStaleAgentExpiration{}, now: time.Now,
	}).tick(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tick() error = %v, want context.Canceled", err)
	}
}

func TestSchedulerRetriesFailedDailyPruningOnNextTick(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	failure := errors.New("prune failed")
	pruning := &fakeIdempotencyPruning{err: failure}
	scheduler := &Scheduler{
		tasks: &fakeTaskExpiration{}, idempotency: pruning, agents: &fakeStaleAgentExpiration{},
		now: func() time.Time { return now },
	}
	if err := scheduler.tick(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first tick error = %v, want prune failure", err)
	}
	pruning.err = nil
	now = now.Add(time.Second)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("retry tick error = %v", err)
	}
	if pruning.calls != 2 || !scheduler.nextPrune.Equal(now.Add(dailyMaintenanceInterval)) {
		t.Fatalf("pruning calls/next = %d/%s", pruning.calls, scheduler.nextPrune)
	}
}
