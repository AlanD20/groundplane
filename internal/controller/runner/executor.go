package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerRunnerExecutor = "controller"

type creationJournal interface {
	LatestCreation(context.Context, string) (runnerallocation.RunnerRuntimeProgress, bool, error)
}

type executionRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	GetRunnerRuntimeOwnership(
		context.Context,
		string,
	) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], bool, error)
	AttestRunnerRuntimeOwnership(
		context.Context,
		etcdstore.Versioned[runnerrecord.RunnerRecord],
		string,
		runnerrecord.RunnerRuntimeOwnershipRecord,
	) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], error)
	RecordRunnerReadinessProof(
		context.Context,
		string,
	) (etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord], error)
	DeleteRunnerRuntimeOwnershipAfterCleanup(
		context.Context,
		etcdstore.Versioned[runnerrecord.RunnerRecord],
		runnerrecord.RunnerRuntimeOwnershipRecord,
	) (int64, error)
}

// Executor owns the native Controller portion of Runner creation. Host effects
// remain inside Lifecycle; this module binds them to durable Runner ownership.
type Executor struct {
	mu         sync.Mutex
	repository executionRepository
	lifecycle  *Lifecycle
	journal    creationJournal
	broker     *TokenBroker
	allocation runnerallocation.RunnerAllocationConfig
	policy     corerunner.IsolationPolicy
	now        func() time.Time
}

func NewExecutor(
	repository executionRepository,
	lifecycle *Lifecycle,
	journal creationJournal,
	broker *TokenBroker,
	allocation runnerallocation.RunnerAllocationConfig,
	policy corerunner.IsolationPolicy,
) (*Executor, error) {
	if repository == nil || lifecycle == nil || journal == nil || broker == nil {
		return nil, errs.New(errs.KindInternal, "Runner executor is not configured")
	}
	if _, err := allocation.Validate(); err != nil {
		return nil, err
	}
	return &Executor{
		repository: repository, lifecycle: lifecycle, journal: journal, broker: broker,
		allocation: allocation, policy: policy, now: time.Now,
	}, nil
}

func (executor *Executor) ExecuteCreate(ctx context.Context, task etcd.TaskRecord) error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if ctx == nil || task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskCreate ||
		ids.Validate(ids.KindTask, task.ID) != nil || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 2 || task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner ||
		task.Params[runnerrecord.RunnerRegistrationTokenPresentParam] != "true" {
		return errs.New(errs.KindValidationFailed, "Controller Task Runner creation is invalid")
	}
	current, err := executor.repository.GetRunner(ctx, task.Target)
	if err != nil {
		return err
	}
	if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningProvisioning ||
		current.Record.CreateTaskID != task.ID || current.Record.RuntimeEpoch == ^uint64(0) {
		return errs.New(errs.KindStateConflict, "Runner creation Task does not own provisioning")
	}
	previous, found, err := executor.journal.LatestCreation(ctx, task.Target)
	if err != nil {
		return err
	}
	var plan corerunner.Plan
	if found && previous.TaskID == task.ID {
		plan = previous.Plan
	} else {
		if found {
			if task.RetryOf == "" || previous.RuntimeEpoch > current.Record.RuntimeEpoch {
				return errs.New(errs.KindStateConflict, "Runner creation journal does not belong to this retry")
			}
			if err := executor.cleanupAttempt(ctx, task.ID, current.Record, previous.Plan); err != nil {
				return err
			}
			ownership, exists, err := executor.repository.GetRunnerRuntimeOwnership(ctx, task.Target)
			if err != nil {
				return err
			}
			if exists {
				if ownership.Record.RuntimeEpoch != previous.RuntimeEpoch {
					return errs.New(errs.KindStateConflict, "Runner retry cleanup ownership changed")
				}
				if _, err := executor.repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, current, ownership.Record); err != nil {
					return err
				}
			}
		}
		plan, err = executor.runtimePlan(current.Record, current.Record.RuntimeEpoch+1)
		if err != nil {
			return err
		}
	}
	attempt := Attempt{
		TaskID: task.ID, Executor: controllerRunnerExecutor,
		OwnershipNonce: runnerOwnershipNonce(task.ID, plan.Digest()), Plan: plan,
	}
	token, _ := executor.broker.Consume(TokenKey{TaskID: task.ID, Attempt: 1})
	evidence, err := executor.lifecycle.CreateWithEvidence(ctx, attempt, token)
	if err != nil {
		return err
	}
	ownership := runnerrecord.RunnerRuntimeOwnershipRecord{
		RunnerID: current.Record.Desired.ID, RuntimeEpoch: plan.RuntimeEpoch,
		DaemonSocketEndpoint: "unix://" + plan.Paths.RawSocket, DaemonInstanceNonce: evidence.DaemonNonce,
		SocketDevice: evidence.SocketDevice, SocketInode: evidence.SocketInode,
		CreatedAt: executor.now().UTC(),
	}
	prior, exists, err := executor.repository.GetRunnerRuntimeOwnership(ctx, task.Target)
	if err != nil {
		return err
	}
	if exists {
		// Readiness publication can be interrupted after ownership commits.
		// Replay its original timestamp, never manufacture a different record.
		ownership.CreatedAt = prior.Record.CreatedAt
		if ownership != prior.Record || current.Record.ContainerID != evidence.ContainerID {
			return errs.New(errs.KindStateConflict, "Runner creation replay ownership changed")
		}
	}
	_, err = executor.repository.AttestRunnerRuntimeOwnership(
		ctx,
		current,
		evidence.ContainerID,
		ownership,
	)
	if err != nil {
		return err
	}
	_, err = executor.repository.RecordRunnerReadinessProof(ctx, task.ID)
	return err
}

func (executor *Executor) ExecuteRemove(ctx context.Context, task etcd.TaskRecord) error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if ctx == nil || task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindTask, task.ID) != nil || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 7 || task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner {
		return errs.New(errs.KindValidationFailed, "Controller Task Runner removal is invalid")
	}
	current, err := executor.repository.GetRunner(ctx, task.Target)
	if err != nil {
		return err
	}
	if task.Params[runnerrecord.RunnerTenantIDParam] != current.Record.Desired.TenantID ||
		task.Params[runnerrecord.RunnerParentProjectIDParam] != current.Record.Desired.ParentProjectID ||
		task.Params[runnerrecord.RunnerOwnerKindParam] != string(current.Record.Desired.OwnerKind) ||
		task.Params[runnerrecord.RunnerOwnerIDParam] != current.Record.Desired.OwnerID {
		return errs.New(errs.KindStateConflict, "Runner removal Task target changed")
	}
	ownership, exists, err := executor.repository.GetRunnerRuntimeOwnership(ctx, task.Target)
	if err != nil {
		return err
	}
	previous, found, err := executor.journal.LatestCreation(ctx, task.Target)
	if err != nil {
		return err
	}
	if !exists {
		if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningFailed ||
			current.Record.ContainerID != "" {
			return errs.New(errs.KindStateConflict, "Runner removal lost runtime ownership")
		}
		if !found {
			return nil // Every host effect requires a published creation journal.
		}
		return executor.cleanupAttempt(ctx, task.ID, current.Record, previous.Plan)
	}
	if current.Record.ContainerID == "" || ownership.Record.RunnerID != current.Record.Desired.ID ||
		ownership.Record.RuntimeEpoch >= current.Record.RuntimeEpoch || !found ||
		previous.Plan.RuntimeEpoch != ownership.Record.RuntimeEpoch {
		return errs.New(errs.KindStateConflict, "Runner removal ownership changed")
	}
	if err := executor.cleanupAttempt(ctx, task.ID, current.Record, previous.Plan); err != nil {
		return err
	}
	_, err = executor.repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, current, ownership.Record)
	return err
}

func (executor *Executor) cleanupAttempt(
	ctx context.Context,
	taskID string,
	record runnerrecord.RunnerRecord,
	plan corerunner.Plan,
) error {
	if plan.RunnerID != record.Desired.ID || plan.Allocation != record.Allocation ||
		plan.RuntimeEpoch > record.RuntimeEpoch {
		return errs.New(errs.KindStateConflict, "Runner cleanup journal does not own this allocation")
	}
	return executor.lifecycle.Remove(ctx, Attempt{
		TaskID: taskID, Executor: controllerRunnerExecutor,
		OwnershipNonce: runnerOwnershipNonce(taskID, plan.Digest()), Plan: plan,
	})
}

func (executor *Executor) runtimePlan(record runnerrecord.RunnerRecord, epoch uint64) (corerunner.Plan, error) {
	return corerunner.NewPlan(corerunner.Target{
		RunnerID: record.Desired.ID, TenantID: record.Desired.TenantID,
		OwnerKind: corerunner.OwnerKind(record.Desired.OwnerKind), OwnerID: record.Desired.OwnerID,
		GitHubURL: record.Desired.GitHubURL, Labels: record.Desired.Labels,
		ImageRef: record.Desired.ImageRef, RuntimeEpoch: epoch,
		AllocationConfig: executor.allocation, Allocation: record.Allocation,
	}, executor.policy)
}

func runnerOwnershipNonce(taskID string, planDigest string) string {
	digest := sha256.Sum256([]byte("groundplane.runner-ownership.v1\x00" + taskID + "\x00" + planDigest))
	return hex.EncodeToString(digest[:])
}
