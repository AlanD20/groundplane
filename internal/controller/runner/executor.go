package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type executionJournal interface {
	ReadAttempt(context.Context, runnerallocation.RunnerRuntimeAttempt, runnerallocation.RuntimeOperation) (runnerallocation.RunnerRuntimeProgress, bool, error)
}

// Executor runs only captured native Runner work. The local journal accounts for
// issued host effects; operation results and Runner state settle through etcd.
type Executor struct {
	mu         sync.Mutex
	repository *etcd.RunnerRepository
	inputs     *runnerwork.Repository
	lifecycle  *Lifecycle
	journal    executionJournal
	broker     *TokenBroker
	now        func() time.Time
}

func NewExecutor(repository *etcd.RunnerRepository, inputs *runnerwork.Repository,
	lifecycle *Lifecycle, journal executionJournal, broker *TokenBroker,
) (*Executor, error) {
	if repository == nil || inputs == nil || lifecycle == nil || journal == nil || broker == nil {
		return nil, errs.New(errs.KindInternal, "Runner execution dependencies are required")
	}
	return &Executor{repository: repository, inputs: inputs, lifecycle: lifecycle, journal: journal, broker: broker, now: time.Now}, nil
}

type runnerEffectAuthority struct {
	records *runnerwork.Repository
	attempt workexecution.Attempt
}

func (authority runnerEffectAuthority) Grant(ctx context.Context, step runnerallocation.RunnerRuntimeStep, at time.Time) error {
	return authority.records.Grant(ctx, authority.attempt, step, at)
}

func (executor *Executor) loadDelivery(ctx context.Context, delivery workqueue.Delivery) (keyvalue.Versioned[runnerwork.Input], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.ActiveAttemptID != attempt.ID ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID || attempt.Number != item.AttemptNumber ||
		attempt.Executor.Executor != workexecution.Controller || attempt.Executor.HostID != item.Placement.HostID ||
		attempt.FinishedAt != nil || item.Input == nil || *item.Input != attempt.Input {
		return keyvalue.Versioned[runnerwork.Input]{}, errs.New(errs.KindStateConflict, "Runner delivery no longer owns this attempt")
	}
	input, err := executor.inputs.Load(ctx, item.ID, 0)
	if err != nil {
		return keyvalue.Versioned[runnerwork.Input]{}, err
	}
	if err := input.Record.MatchWork(item); err != nil {
		return keyvalue.Versioned[runnerwork.Input]{}, err
	}
	if err := corerunner.ValidatePlan(input.Record.Plan); err != nil {
		return keyvalue.Versioned[runnerwork.Input]{}, err
	}
	return input, nil
}

func runtimeAttempt(input runnerwork.Input, attempt workexecution.Attempt) Attempt {
	digest := sha256.Sum256([]byte("groundplane.runner-attempt.v1\x00" + attempt.ID + "\x00" + input.Plan.Digest()))
	return Attempt{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
		AttemptNumber: attempt.Number, Executor: attempt.Executor, OwnershipNonce: hex.EncodeToString(digest[:]),
		Plan: input.Plan.Clone()}
}

func (executor *Executor) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	input, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "Runner execution has not been started")
	}
	if _, err := executor.inputs.LoadResult(ctx, attempt, 0); err == nil {
		return nil
	} else if kind, _ := errs.KindOf(err); kind != errs.KindRequestNotFound {
		return err
	}
	bound := runtimeAttempt(input.Record, attempt)
	authority := runnerEffectAuthority{records: executor.inputs, attempt: attempt}
	if attempt.Direction == workexecution.Recovery || input.Record.Operation == runnerallocation.RuntimeOperationRemove {
		err = executor.lifecycle.Remove(ctx, bound, authority)
	} else {
		token, _ := executor.broker.Consume(TokenKey{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, Attempt: attempt.Number})
		_, err = executor.lifecycle.CreateWithEvidence(ctx, bound, token, authority)
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	resultErr := executor.recordObserved(recordCtx, input.Record, attempt, err)
	if err != nil && resultErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, resultErr)
	}
	if resultErr != nil {
		return resultErr
	}
	return err
}

// Reconcile never invokes a forward capability. An incomplete issued operation
// becomes unresolved and can run only its separately assigned captured cleanup.
func (executor *Executor) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	input, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return executor.reject(ctx, delivery, input.Record)
	}
	if _, err := executor.inputs.LoadResult(ctx, attempt, 0); err == nil {
		return nil
	} else if kind, _ := errs.KindOf(err); kind != errs.KindRequestNotFound {
		return err
	}
	return executor.recordObserved(ctx, input.Record, attempt, errs.New(errs.KindStateConflict, "Runner attempt was interrupted"))
}

func (executor *Executor) recordObserved(ctx context.Context, input runnerwork.Input, attempt workexecution.Attempt, cause error) error {
	operation := input.Operation
	if attempt.Direction == workexecution.Recovery {
		operation = runnerallocation.RuntimeOperationRemove
	}
	progress, exists, err := executor.journal.ReadAttempt(ctx, runtimeAttempt(input, attempt), operation)
	if err != nil {
		return err
	}
	result := runnerwork.Result{Attempt: attempt, Outcome: workexecution.Failed, Effects: runnerwork.EffectsUnknown,
		Failure: runnerFailure(cause), RecordedAt: executor.now().UTC()}
	if !exists || progress.NextStep == 0 && progress.ActiveStep == nil {
		if attempt.Direction == workexecution.Forward {
			result.Effects = runnerwork.NoEffects
		}
	} else if progress.Status == StatusReady && operation == OperationCreate {
		result.Outcome, result.Effects, result.Evidence, result.Failure = workexecution.Succeeded, runnerwork.RuntimeReady, progress.Evidence, ""
	} else if progress.Status == StatusRemoved && operation == OperationRemove {
		result.Outcome, result.Effects, result.Cleanup, result.Failure = workexecution.Succeeded, runnerwork.RuntimeRemoved, progress.CleanupReceipts, ""
	}
	return executor.inputs.RecordResult(ctx, result)
}

func runnerFailure(cause error) string {
	if errors.Is(cause, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(cause, context.Canceled) {
		return "cancelled"
	}
	return "runtime_failed"
}

func (executor *Executor) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	if _, err := executor.loadDelivery(ctx, delivery); err != nil {
		return workqueue.Change{}, err
	}
	return executor.inputs.PrepareStart(ctx, delivery.Attempt.Attempt)
}

func (executor *Executor) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	input, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	return executor.reject(ctx, delivery, input.Record)
}
func (executor *Executor) reject(ctx context.Context, delivery workqueue.Delivery, input runnerwork.Input) error {
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "Runner attempt already started; inspect its effects")
	}
	executor.broker.Drop(TokenKey{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, Attempt: attempt.Number})
	outcome, failure := workexecution.Cancelled, "cancelled"
	if !executor.now().UTC().Before(attempt.Deadline) {
		outcome, failure = workexecution.Failed, "deadline"
	}
	effects := runnerwork.NoEffects
	if attempt.Direction == workexecution.Recovery {
		effects = runnerwork.EffectsUnknown
	}
	return executor.inputs.RecordResult(ctx, runnerwork.Result{Attempt: attempt, Outcome: outcome,
		Effects: effects, Failure: failure, RecordedAt: executor.now().UTC()})
}
