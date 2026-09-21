package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerRunnerExecutor = "controller"

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
	repository executionRepository
	lifecycle  *Lifecycle
	broker     *TokenBroker
	allocation runnerallocation.RunnerAllocationConfig
	policy     corerunner.IsolationPolicy
	now        func() time.Time
}

func NewExecutor(
	repository executionRepository,
	lifecycle *Lifecycle,
	broker *TokenBroker,
	allocation runnerallocation.RunnerAllocationConfig,
	policy corerunner.IsolationPolicy,
) (*Executor, error) {
	if repository == nil || lifecycle == nil || broker == nil {
		return nil, errs.New(errs.KindInternal, "Runner executor is not configured")
	}
	if _, err := allocation.Validate(); err != nil {
		return nil, err
	}
	return &Executor{
		repository: repository, lifecycle: lifecycle, broker: broker,
		allocation: allocation, policy: policy, now: time.Now,
	}, nil
}

func (executor *Executor) ExecuteCreate(ctx context.Context, task etcd.TaskRecord) error {
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
	plan, err := executor.runtimePlan(current.Record, current.Record.RuntimeEpoch+1)
	if err != nil {
		return err
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
	_, err = executor.repository.AttestRunnerRuntimeOwnership(
		ctx,
		current,
		evidence.ContainerID,
		runnerrecord.RunnerRuntimeOwnershipRecord{
			RunnerID: current.Record.Desired.ID, RuntimeEpoch: plan.RuntimeEpoch,
			DaemonSocketEndpoint: plan.Paths.RawSocket, DaemonInstanceNonce: evidence.DaemonNonce,
			SocketDevice: evidence.SocketDevice, SocketInode: evidence.SocketInode,
			CreatedAt: executor.now().UTC(),
		},
	)
	if err != nil {
		return err
	}
	_, err = executor.repository.RecordRunnerReadinessProof(ctx, task.ID)
	return err
}

func (executor *Executor) ExecuteRemove(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil || task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindTask, task.ID) != nil || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 6 || task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner {
		return errs.New(errs.KindValidationFailed, "Controller Task Runner removal is invalid")
	}
	current, err := executor.repository.GetRunner(ctx, task.Target)
	if err != nil {
		return err
	}
	if task.Params[runnerrecord.RunnerTenantIDParam] != current.Record.Desired.TenantID ||
		task.Params[runnerrecord.RunnerOwnerKindParam] != string(current.Record.Desired.OwnerKind) ||
		task.Params[runnerrecord.RunnerOwnerIDParam] != current.Record.Desired.OwnerID {
		return errs.New(errs.KindStateConflict, "Runner removal Task target changed")
	}
	ownership, exists, err := executor.repository.GetRunnerRuntimeOwnership(ctx, task.Target)
	if err != nil {
		return err
	}
	if !exists {
		if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningFailed || current.Record.ContainerID != "" {
			return errs.New(errs.KindStateConflict, "Runner removal lost runtime ownership")
		}
		return nil
	}
	if current.Record.ContainerID == "" || ownership.Record.RunnerID != current.Record.Desired.ID ||
		ownership.Record.RuntimeEpoch >= current.Record.RuntimeEpoch {
		return errs.New(errs.KindStateConflict, "Runner removal ownership changed")
	}
	plan, err := executor.runtimePlan(current.Record, ownership.Record.RuntimeEpoch)
	if err != nil {
		return err
	}
	attempt := Attempt{
		TaskID: task.ID, Executor: controllerRunnerExecutor,
		OwnershipNonce: runnerOwnershipNonce(task.ID, plan.Digest()), Plan: plan,
	}
	if err := executor.lifecycle.Remove(ctx, attempt); err != nil {
		return err
	}
	_, err = executor.repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, current, ownership.Record)
	return err
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
