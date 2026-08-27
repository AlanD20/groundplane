package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeTaskExpiration struct {
	now        time.Time
	calls      int
	err        error
	pruneNow   time.Time
	pruneCalls int
	pruneErr   error
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

func (expiration *fakeTaskExpiration) PruneExpiredTasks(_ context.Context, now time.Time) (int, error) {
	expiration.pruneCalls++
	expiration.pruneNow = now
	return 1, expiration.pruneErr
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
		expiration.pruneCalls != 1 || !expiration.pruneNow.Equal(wantNow) ||
		pruning.calls != 1 || !pruning.now.Equal(wantNow) || agents.calls != 1 || !agents.now.Equal(wantNow) {
		t.Fatalf("ExpireTimedOutTasks() calls/time = %d/%s", expiration.calls, expiration.now)
	}
	now = wantNow.Add(dailyMaintenanceInterval - time.Second)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick(before daily prune) error = %v", err)
	}
	if expiration.calls != 2 || agents.calls != 2 || pruning.calls != 1 || expiration.pruneCalls != 1 {
		t.Fatalf(
			"before daily deadline expiration/stale/pruning calls = %d/%d/%d",
			expiration.calls,
			agents.calls,
			pruning.calls,
		)
	}
	now = wantNow.Add(dailyMaintenanceInterval)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("tick(at daily prune) error = %v", err)
	}
	if expiration.calls != 3 || agents.calls != 3 || pruning.calls != 2 || expiration.pruneCalls != 2 ||
		!pruning.now.Equal(now) || !expiration.pruneNow.Equal(now) {
		t.Fatalf(
			"at daily deadline expiration/stale/pruning calls/time = %d/%d/%d/%s",
			expiration.calls,
			agents.calls,
			pruning.calls,
			pruning.now,
		)
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

func TestSchedulerRetriesFailedTaskPruningOnNextTick(t *testing.T) {
	// Rationale: the daily deadline must advance only after marker and Task
	// pruning both succeed, preserving the required ordering on retry.
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	failure := errors.New("Task prune failed")
	tasks := &fakeTaskExpiration{pruneErr: failure}
	pruning := &fakeIdempotencyPruning{}
	scheduler := &Scheduler{
		tasks: tasks, idempotency: pruning, agents: &fakeStaleAgentExpiration{},
		now: func() time.Time { return now },
	}
	if err := scheduler.tick(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first tick error = %v, want Task prune failure", err)
	}
	if pruning.calls != 1 || tasks.pruneCalls != 1 || !scheduler.nextPrune.IsZero() {
		t.Fatalf("first marker/Task prune calls/next = %d/%d/%s", pruning.calls, tasks.pruneCalls, scheduler.nextPrune)
	}
	tasks.pruneErr = nil
	now = now.Add(time.Second)
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatalf("retry tick error = %v", err)
	}
	if pruning.calls != 2 || tasks.pruneCalls != 2 ||
		!scheduler.nextPrune.Equal(now.Add(dailyMaintenanceInterval)) {
		t.Fatalf("retry marker/Task prune calls/next = %d/%d/%s", pruning.calls, tasks.pruneCalls, scheduler.nextPrune)
	}
}

type backupScheduleIsolationRepository struct {
	candidates []etcd.BackupScheduleCandidate
	evaluation etcd.BackupScheduleEvaluation
	evaluated  []string
}

func (repository *backupScheduleIsolationRepository) ListBackupScheduleCandidates(
	context.Context,
) ([]etcd.BackupScheduleCandidate, error) {
	return repository.candidates, nil
}

func (repository *backupScheduleIsolationRepository) EvaluateBackupSchedule(
	_ context.Context,
	environmentID string,
	_ time.Time,
) (etcd.BackupScheduleEvaluation, error) {
	repository.evaluated = append(repository.evaluated, environmentID)
	return repository.evaluation, nil
}

func (*backupScheduleIsolationRepository) SkipScheduledBackup(
	context.Context,
	etcd.BackupScheduleEvaluation,
	time.Time,
) error {
	return nil
}

type backupScheduleIsolationRunner struct {
	calls int
}

func (runner *backupScheduleIsolationRunner) RunScheduledBackup(
	context.Context,
	string,
	int64,
	time.Time,
	time.Time,
	...int64,
) (etcd.IdempotencyResponse, error) {
	runner.calls++
	return etcd.IdempotencyResponse{Status: 202}, nil
}

// Rationale: one corrupt policy is observed but cannot abort the ordered pass
// before a later valid due candidate is dispatched.
func TestBackupScheduleServiceIsolatesBadPolicyAndContinues(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	badID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	goodID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	repository := &backupScheduleIsolationRepository{
		candidates: []etcd.BackupScheduleCandidate{
			{EnvironmentID: badID, Err: errs.New(errs.KindInternal, "corrupt policy")},
			{EnvironmentID: goodID},
		},
		evaluation: etcd.BackupScheduleEvaluation{
			EnvironmentID:  goodID,
			PolicyRevision: 7,
			ReadRevision:   11,
			ScheduledAt:    now.Add(-time.Hour),
			EvaluatedAt:    now,
			Due:            true,
		},
	}
	runner := &backupScheduleIsolationRunner{}
	service, err := NewBackupScheduleService(
		repository,
		runner,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.RunBackupSchedules(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(repository.evaluated) != 1 || repository.evaluated[0] != goodID || runner.calls != 1 {
		t.Fatalf("evaluated=%v run calls=%d", repository.evaluated, runner.calls)
	}
}
