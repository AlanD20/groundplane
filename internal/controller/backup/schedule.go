package backup

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScheduledBackupRunner publishes one due occurrence through the same immutable
// run path as a manual backup, but in a disjoint system idempotency namespace.
type ScheduledBackupRunner interface {
	RunScheduledBackup(
		context.Context,
		string,
		int64,
		time.Time,
		time.Time,
		...int64,
	) (etcd.IdempotencyResponse, error)
}

// BackupScheduleRepository is the Controller-owned durable scheduling seam.
type BackupScheduleRepository interface {
	ListBackupScheduleCandidates(context.Context) ([]etcd.BackupScheduleCandidate, error)
	EvaluateBackupSchedule(context.Context, string, time.Time) (etcd.BackupScheduleEvaluation, error)
	SkipScheduledBackup(context.Context, etcd.BackupScheduleEvaluation, time.Time) error
}

// BackupScheduleService is called by the Controller singleton scheduler. It
// owns pass ordering and failure isolation; no policy owns a timer or goroutine.
type BackupScheduleService struct {
	runtime BackupScheduleRepository
	runs    ScheduledBackupRunner
	logger  *slog.Logger
}

func NewBackupScheduleService(
	runtime BackupScheduleRepository,
	runs ScheduledBackupRunner,
	logger *slog.Logger,
) (*BackupScheduleService, error) {
	if runtime == nil || runs == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "backup schedule dependencies are required")
	}
	return &BackupScheduleService{runtime: runtime, runs: runs, logger: logger}, nil
}

func (service *BackupScheduleService) RunBackupSchedules(ctx context.Context, now time.Time) error {
	if service == nil || service.runtime == nil || service.runs == nil || service.logger == nil {
		return errs.New(errs.KindInternal, "backup schedule service is not configured")
	}
	candidates, err := service.runtime.ListBackupScheduleCandidates(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.Err != nil {
			service.observe(candidate.EnvironmentID, "candidate", candidate.Err)
			continue
		}
		evaluation, evaluateErr := service.runtime.EvaluateBackupSchedule(
			ctx, candidate.EnvironmentID, now,
		)
		if evaluateErr != nil {
			// Replacement between range and evaluation is normal. Corrupt or
			// otherwise bad policy state is observed and does not starve later
			// Environment ids in the ordered range.
			service.observe(candidate.EnvironmentID, "evaluate", evaluateErr)
			continue
		}
		if !evaluation.Due {
			continue
		}
		if evaluation.Overlap {
			if skipErr := service.runtime.SkipScheduledBackup(
				ctx, evaluation, evaluation.EvaluatedAt,
			); skipErr != nil {
				service.observe(candidate.EnvironmentID, "skip_overlap", skipErr)
			}
			continue
		}
		if _, runErr := service.runs.RunScheduledBackup(
			ctx,
			evaluation.EnvironmentID,
			evaluation.PolicyRevision,
			evaluation.ScheduledAt,
			evaluation.EvaluatedAt,
			evaluation.ReadRevision,
		); runErr != nil {
			// Publication failures leave the occurrence unadvanced. A later pass
			// retries the same latest-only durable identity.
			service.observe(candidate.EnvironmentID, "dispatch", runErr)
		}
	}
	return nil
}

func (service *BackupScheduleService) observe(environmentID, phase string, err error) {
	service.logger.Error(
		"backup scheduler: policy failed",
		slog.String("environment_id", environmentID),
		slog.String("phase", phase),
		slog.Any("error", err),
	)
}
