package runner

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const ObservationLifetime = time.Minute

type maintenanceRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	ListRuntimeRunners(
		context.Context,
		etcdstore.PageRequest,
	) (etcdstore.Page[runnerrecord.RunnerRecord], error)
	GetRunnerDeletionTombstone(
		context.Context,
		string,
	) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error)
	GetRunnerRuntimeOwnership(
		context.Context,
		string,
	) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], bool, error)
	GetRunnerObservation(
		context.Context,
		string,
	) (etcdstore.Versioned[runnerrecord.RunnerObservationRecord], bool, error)
	PutRunnerObservation(
		context.Context,
		runnerrecord.RunnerObservationRecord,
		int64,
	) (etcdstore.Versioned[runnerrecord.RunnerObservationRecord], error)
	RefreshRuntime(
		context.Context,
		etcdstore.Versioned[runnerrecord.RunnerRecord],
		etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord],
		string,
		runnerrecord.RunnerRuntimeOwnershipRecord,
		runnerrecord.RunnerObservationRecord,
	) error
}

type readyJournal interface {
	ReadyPlan(context.Context, string, uint64, string) (runnerallocation.RuntimePlan, error)
}

type readyRuntime interface {
	Resume(context.Context, runnerallocation.RuntimePlan, string) (runnerallocation.RunnerRuntimeEvidence, error)
}

// Maintenance restores registered runtimes after boot and refreshes local
// observations. It shares the executor lock, so it cannot race host cleanup.
type Maintenance struct {
	repository maintenanceRepository
	journal    readyJournal
	runtime    readyRuntime
	executor   *Executor
	logger     *slog.Logger
}

func NewMaintenance(
	repository maintenanceRepository,
	journal readyJournal,
	runtime readyRuntime,
	executor *Executor,
	logger *slog.Logger,
) (*Maintenance, error) {
	if repository == nil || journal == nil || runtime == nil || executor == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "Runner maintenance dependencies are required")
	}
	return &Maintenance{
		repository: repository,
		journal:    journal,
		runtime:    runtime,
		executor:   executor,
		logger:     logger,
	}, nil
}

func (maintenance *Maintenance) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		if err := maintenance.refresh(ctx); err != nil && ctx.Err() == nil {
			maintenance.logger.Error("Runner maintenance failed", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (maintenance *Maintenance) refresh(ctx context.Context) error {
	page := etcdstore.PageRequest{Limit: 100}
	for {
		runners, err := maintenance.repository.ListRuntimeRunners(ctx, page)
		if err != nil {
			return err
		}
		for _, item := range runners.Items {
			if item.Record.ProvisioningState != runnerrecord.RunnerProvisioningReady {
				continue
			}
			if err := maintenance.refreshOne(ctx, item.Record.Desired.ID); err != nil && ctx.Err() == nil {
				maintenance.logger.Warn(
					"Runner runtime is unavailable",
					slog.String("runner_id", item.Record.Desired.ID),
					slog.Any("error", err),
				)
			}
		}
		if runners.NextCursor == "" {
			return nil
		}
		page.Cursor, page.Revision = runners.NextCursor, runners.Revision
	}
}

func (maintenance *Maintenance) refreshOne(ctx context.Context, runnerID string) error {
	maintenance.executor.mu.Lock()
	defer maintenance.executor.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	current, err := maintenance.repository.GetRunner(ctx, runnerID)
	if err != nil {
		return err
	}
	if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningReady {
		return nil
	}
	_, deleting, err := maintenance.repository.GetRunnerDeletionTombstone(ctx, runnerID)
	if err != nil || deleting {
		return err
	}
	prior, exists, err := maintenance.repository.GetRunnerRuntimeOwnership(ctx, runnerID)
	if err != nil {
		return err
	}
	if !exists || prior.Record.RuntimeEpoch != current.Record.RuntimeEpoch {
		return errs.New(errs.KindStateConflict, "Runner ready ownership is missing")
	}
	plan, err := maintenance.journal.ReadyPlan(ctx, runnerID, prior.Record.RuntimeEpoch, current.Record.CreateTaskID)
	if err != nil {
		return maintenance.offline(ctx, runnerID, err)
	}
	evidence, err := maintenance.runtime.Resume(ctx, plan, current.Record.ContainerID)
	if err != nil {
		return maintenance.offline(ctx, runnerID, err)
	}
	if !evidence.Valid() || evidence.ContainerID != current.Record.ContainerID {
		return maintenance.offline(
			ctx,
			runnerID,
			errs.New(errs.KindStateConflict, "Runner registered container changed"),
		)
	}
	updated := prior.Record
	updated.DaemonInstanceNonce, updated.SocketDevice, updated.SocketInode = evidence.DaemonNonce, evidence.SocketDevice, evidence.SocketInode
	return maintenance.repository.RefreshRuntime(
		ctx,
		current,
		prior,
		evidence.ContainerID,
		updated,
		runnerrecord.RunnerObservationRecord{
			RunnerID: runnerID, Online: true, ObservedAt: time.Now().UTC(),
		},
	)
}

func (maintenance *Maintenance) offline(ctx context.Context, runnerID string, cause error) error {
	prior, exists, err := maintenance.repository.GetRunnerObservation(ctx, runnerID)
	if err != nil {
		return err
	}
	revision := int64(0)
	if exists {
		revision = prior.Revision
	}
	_, err = maintenance.repository.PutRunnerObservation(ctx, runnerrecord.RunnerObservationRecord{
		RunnerID: runnerID, Online: false, ObservedAt: time.Now().UTC(),
	}, revision)
	if err != nil {
		return err
	}
	return cause
}
