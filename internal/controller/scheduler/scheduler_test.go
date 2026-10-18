package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	testbackup "github.com/AlanD20/groundplane/internal/controller/backup"
	testbackupscheduling "github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeSourceReleases struct{ calls int }

func (source *fakeSourceReleases) ResumeTaskSourceReleases(context.Context) (bool, error) {
	source.calls++
	return true, nil
}

type fakeIdempotencyPruning struct {
	now   time.Time
	calls int
	err   error
}

func (pruning *fakeIdempotencyPruning) PruneExpired(_ context.Context, now time.Time, remaining int) (int, error) {
	pruning.calls++
	pruning.now = now
	return min(3, remaining), pruning.err
}

type fakeBlueprintRevisionCleanup struct {
	now   time.Time
	limit int
	calls int
	err   error
}

func (cleanup *fakeBlueprintRevisionCleanup) CleanupEnvironmentBlueprintStaging(
	_ context.Context, now time.Time, limit int,
) (int, error) {
	cleanup.calls++
	cleanup.now, cleanup.limit = now, limit
	return 1, cleanup.err
}
func schedulerTestPolicy() config.MaintenancePolicy {
	policy := config.DefaultMaintenancePolicy()
	policy.CleanupBatchSize = 1
	return policy
}

// Rationale: source release and abandoned drafts must keep progressing between
// bounded replay-retention passes, without reintroducing Task timeout finalization.
func TestSchedulerSourceReleaseContinuesBetweenRetentionPasses(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	first := now
	sources, pruning := &fakeSourceReleases{}, &fakeIdempotencyPruning{}
	drafts := &fakeBlueprintRevisionCleanup{}
	scheduler := &Scheduler{
		maintenancePolicy: schedulerTestPolicy(), sources: sources, idempotency: pruning,
		blueprintRevisions: drafts, now: func() time.Time { return now },
	}
	for pass, offset := range []time.Duration{0, schedulerTestPolicy().CleanupInterval - time.Second, schedulerTestPolicy().CleanupInterval} {
		now = first.Add(offset)
		if err := scheduler.tick(t.Context()); err != nil {
			t.Fatal(err)
		}
		expectedPrunes := 1
		if pass == 2 {
			expectedPrunes = 2
		}
		if sources.calls != pass+1 || drafts.calls != pass+1 || pruning.calls != expectedPrunes ||
			!drafts.now.Equal(now) || drafts.limit != 16 {
			t.Fatalf("pass %d: source=%d drafts=%d prune=%d at=%v limit=%d",
				pass, sources.calls, drafts.calls, pruning.calls, drafts.now, drafts.limit)
		}
	}
	if !pruning.now.Equal(now) {
		t.Fatal("retention used an earlier pass timestamp")
	}
}

func TestSchedulerTickHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sources, pruning, drafts := &fakeSourceReleases{}, &fakeIdempotencyPruning{}, &fakeBlueprintRevisionCleanup{}
	err := (&Scheduler{
		maintenancePolicy: schedulerTestPolicy(), sources: sources, idempotency: pruning,
		blueprintRevisions: drafts, now: time.Now,
	}).tick(ctx)
	if !errors.Is(err, context.Canceled) || sources.calls != 0 || pruning.calls != 0 || drafts.calls != 0 {
		t.Fatalf(
			"cancelled pass: error=%v source=%d prune=%d drafts=%d",
			err,
			sources.calls,
			pruning.calls,
			drafts.calls,
		)
	}
}

// Rationale: failed cleanup must wait for its next scheduled pass, while
// independent source release and Backup scheduling continue in between.
func TestSchedulerFailedPruningWaitsWithoutStarvingOtherMaintenance(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	failure := errors.New("prune failed")
	pruning, sources, schedules := &fakeIdempotencyPruning{
		err: failure,
	}, &fakeSourceReleases{}, &nativeAdmissionBackupSchedules{}
	scheduler := &Scheduler{
		maintenancePolicy: schedulerTestPolicy(), sources: sources, idempotency: pruning,
		blueprintRevisions: &fakeBlueprintRevisionCleanup{}, backupSchedules: schedules, now: func() time.Time { return now },
	}
	if err := scheduler.tick(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("first pass=%v", err)
	}
	next := now.Add(schedulerTestPolicy().CleanupInterval)
	now = now.Add(time.Second)
	if err := scheduler.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if pruning.calls != 1 || sources.calls != 2 || schedules.calls != 2 || !scheduler.nextPrune.Equal(next) {
		t.Fatalf("failed cleanup looped or starved work: prune=%d source=%d schedules=%d next=%v",
			pruning.calls, sources.calls, schedules.calls, scheduler.nextPrune)
	}
	pruning.err, now = nil, next
	if err := scheduler.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if pruning.calls != 2 || sources.calls != 3 || schedules.calls != 3 ||
		!scheduler.nextPrune.Equal(now.Add(schedulerTestPolicy().CleanupInterval)) {
		t.Fatal("cleanup did not retry at its next scheduled pass")
	}
}

type backupScheduleIsolationRepository struct {
	candidates []testbackupscheduling.BackupScheduleCandidate
	evaluation testbackupscheduling.BackupScheduleEvaluation
	evaluated  []string
}

func (repository *backupScheduleIsolationRepository) ListBackupScheduleCandidates(
	context.Context,
) ([]testbackupscheduling.BackupScheduleCandidate, error) {
	return repository.candidates, nil
}

func (repository *backupScheduleIsolationRepository) EvaluateBackupSchedule(
	_ context.Context,
	environmentID string,
	_ time.Time,
) (testbackupscheduling.BackupScheduleEvaluation, error) {
	repository.evaluated = append(repository.evaluated, environmentID)
	return repository.evaluation, nil
}

func (*backupScheduleIsolationRepository) SkipScheduledBackup(
	context.Context, testbackupscheduling.BackupScheduleEvaluation,

	time.Time,
) error {
	return nil
}

type backupScheduleIsolationRunner struct {
	calls int
}

func (runner *backupScheduleIsolationRunner) RunScheduledBackup(
	context.Context, string,

	int64,
	time.Time,
	time.Time,
	...int64,
) (testidempotencyowner.IdempotencyResponse, error) {
	runner.calls++
	return testidempotencyowner.IdempotencyResponse{Status: 202}, nil
}

// Rationale: one corrupt policy is observed but cannot abort the ordered pass
// before a later valid due candidate is dispatched.
func TestBackupScheduleServiceIsolatesBadPolicyAndContinues(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	badID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	goodID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	repository := &backupScheduleIsolationRepository{
		candidates: []testbackupscheduling.BackupScheduleCandidate{
			{EnvironmentID: badID, Err: errs.New(errs.KindInternal, "corrupt policy")},
			{EnvironmentID: goodID},
		},
		evaluation: testbackupscheduling.BackupScheduleEvaluation{
			EnvironmentID:  goodID,
			PolicyRevision: 7,
			ReadRevision:   11,
			ScheduledAt:    now.Add(-time.Hour),
			EvaluatedAt:    now,
			Due:            true,
		},
	}
	runner := &backupScheduleIsolationRunner{}
	service, err := testbackup.NewBackupScheduleService(
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
