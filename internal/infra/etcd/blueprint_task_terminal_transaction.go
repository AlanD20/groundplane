package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintTaskTerminalTransaction is a closed terminal envelope. Only the
// owning repository can construct its authority; callers cannot supply a budget.
type BlueprintTaskTerminalTransaction struct {
	taskID         string
	conditions     []etcdstore.Condition
	mutations      []etcdstore.Mutation
	projectionOnly bool
}

type blueprintTaskTerminalStore interface {
	ValidateBlueprintTaskTerminal(context.Context, BlueprintTaskTerminalTransaction) error
	TransactBlueprintTaskTerminal(context.Context, BlueprintTaskTerminalTransaction) (etcdstore.TransactionResult, error)
}

// Operations exposes validated copies to implementations of the persistence
// seam. It cannot construct or change a terminal envelope or select its budget.
func (envelope BlueprintTaskTerminalTransaction) Operations() ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if envelope.projectionOnly {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint terminal budget projection cannot execute")
	}
	if err := envelope.validate(); err != nil {
		return nil, nil, err
	}
	return append([]etcdstore.Condition(nil), envelope.conditions...), cloneBlueprintCandidateMutations(envelope.mutations), nil
}

// TaskRepositoryStore exposes the closed Blueprint completion path separately
// from ordinary Store transactions. Desired publication is not consumed here.
type TaskRepositoryStore interface {
	etcdstore.Store
	ValidateBlueprintTaskTerminal(context.Context, BlueprintTaskTerminalTransaction) error
	TransactBlueprintTaskTerminal(context.Context, BlueprintTaskTerminalTransaction) (etcdstore.TransactionResult, error)
}

// EnvironmentBlueprintStore combines the store's closed Blueprint capabilities
// for composition; the Task repository consumes only TaskRepositoryStore.
type EnvironmentBlueprintStore interface {
	TaskRepositoryStore
	TransactEnvironmentBlueprint(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// NewTaskRepository wires terminal persistence explicitly at composition time.
// Ordinary internal repository users do not acquire the Blueprint capability.
func NewTaskRepository(store TaskRepositoryStore) (*TaskRepository, error) {
	repository, err := newTaskRepository(store)
	if err != nil {
		return nil, err
	}
	repository.blueprintTerminalStore = store
	return repository, nil
}

func isBlueprintCandidateTerminalTask(task TaskRecord) bool {
	return task.Type == TaskUpdate && task.Executor == TaskExecutorAgent &&
		(task.Status == TaskStatusCompleted || task.Status == TaskStatusFailed || task.Status == TaskStatusAborted) &&
		task.Params[TaskReleasePublicationParam] != ""
}

func (mutationContext *ordinaryEnvironmentMutationContext) bindTaskLifecycle(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	advanceEpoch bool,
) (*ordinaryEnvironmentMutationBinding, error) {
	if isVolumeRemovalTerminalTask(task) {
		// Volume completion binds its held removal lock and ancestry in the
		// terminal owner; it is not a new ordinary desired-state mutation.
		return nil, nil
	}
	if !isBlueprintCandidateTerminalTask(task) {
		return mutationContext.bind(ctx, store, conditions, mutations, advanceEpoch)
	}
	binding, err := mutationContext.prepareBinding(ctx, store, conditions, mutations, advanceEpoch)
	if err != nil {
		return nil, err
	}
	if _, err := compileBlueprintTaskTerminalTransaction(task, binding.conditions, binding.mutations); err != nil {
		binding.clear()
		return nil, err
	}
	return binding, nil
}

func compileBlueprintTaskTerminalTransaction(
	task TaskRecord,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (BlueprintTaskTerminalTransaction, error) {
	if !isBlueprintCandidateTerminalTask(task) || validateTaskRecord(task) != nil ||
		task.TerminalAssignment == nil || task.Owner.EnvironmentID == "" ||
		task.Params[TaskMaterializationEnvironmentParam] != task.Owner.EnvironmentID {
		return BlueprintTaskTerminalTransaction{}, errs.New(
			errs.KindInternal,
			"Blueprint terminal Task authority is invalid",
		)
	}
	value, err := encodeTaskRecord(task)
	if err != nil {
		return BlueprintTaskTerminalTransaction{}, err
	}
	defer clear(value)
	matched := false
	for _, mutation := range mutations {
		if mutation.Key == taskKey(task.ID) && mutation.Type == etcdstore.MutationPut && !mutation.Prefix {
			matched = bytes.Equal(value, mutation.Value)
		}
	}
	compared := false
	for _, condition := range conditions {
		if condition.Key == taskKey(task.ID) && !condition.Prefix && condition.ModRevision > 0 {
			compared = true
		}
	}
	if !matched || !compared {
		return BlueprintTaskTerminalTransaction{}, errs.New(errs.KindInternal, "Blueprint terminal Task is not fenced")
	}
	envelope := BlueprintTaskTerminalTransaction{taskID: task.ID, conditions: conditions, mutations: mutations}
	if err := envelope.validate(); err != nil {
		return BlueprintTaskTerminalTransaction{}, err
	}
	return envelope, nil
}

func (envelope BlueprintTaskTerminalTransaction) validate() error {
	if envelope.taskID == "" || len(envelope.mutations) == 0 {
		return errs.New(errs.KindInternal, "Blueprint terminal envelope is missing")
	}
	if len(envelope.conditions) > maximumEnvironmentBlueprintTransactionOperationsPerArm ||
		len(envelope.mutations) > maximumEnvironmentBlueprintTransactionOperationsPerArm {
		return errs.Newf(
			errs.KindValidationFailed,
			"Blueprint terminal transaction exceeds a 256-operation arm (%d/%d/%d)",
			len(envelope.conditions),
			len(envelope.mutations),
			len(envelope.conditions),
		)
	}
	keys := make([]string, len(envelope.conditions))
	for index, condition := range envelope.conditions {
		keys[index] = condition.Key
	}
	mutationKeys := make([]string, len(envelope.mutations))
	for index, mutation := range envelope.mutations {
		mutationKeys[index] = mutation.Key
	}
	if transactionRequest(
		envelope.conditions,
		envelope.mutations,
		keys,
		mutationKeys,
	).Size() >
		etcdstore.MaximumBytes {
		return errs.New(errs.KindValidationFailed, "Blueprint terminal transaction exceeds the 1 MiB request limit")
	}
	return nil
}

func (s *store) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope BlueprintTaskTerminalTransaction,
) (etcdstore.TransactionResult, error) {
	if envelope.projectionOnly {
		return etcdstore.TransactionResult{}, errs.New(errs.KindInternal, "Blueprint terminal budget projection cannot execute")
	}
	if err := envelope.validate(); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	return s.transact(ctx, envelope.conditions, envelope.mutations)
}

// ValidateBudget checks the same physical request used by a commit. The prefix
// belongs to the persistence implementation; this method cannot execute work.
func (envelope BlueprintTaskTerminalTransaction) ValidateBudget(keyPrefix string) error {
	if err := envelope.validate(); err != nil {
		return err
	}
	s := &store{root: keyPrefix}
	_, err := s.prepareTransaction(envelope.conditions, envelope.mutations)
	return err
}

func (s *store) ValidateBlueprintTaskTerminal(ctx context.Context, envelope BlueprintTaskTerminalTransaction) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	return envelope.ValidateBudget(s.root)
}

func (repository *TaskRepository) transactTaskTerminal(
	ctx context.Context,
	task TaskRecord,
	phase zoneRemovalTransactionPhase,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	sourceAdvance *blueprintTerminalSourceAdvance,
) (etcdstore.TransactionResult, error) {
	if isVolumeRemovalTerminalTask(task) {
		return repository.transactVolumeRemovalTerminal(ctx, task, conditions, mutations)
	}
	if task.Type == TaskRemove && task.Params[TaskResourceKindParam] == TaskResourceVolume {
		return repository.transactVolumeRemovalAttemptTerminal(ctx, task, conditions, mutations)
	}
	if !isBlueprintCandidateTerminalTask(task) {
		return repository.transactZoneRemovalTaskLifecycle(ctx, task, phase, conditions, mutations)
	}
	envelope, err := compileBlueprintTaskTerminalTransaction(task, conditions, mutations)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if repository.blueprintTerminalStore == nil {
		return etcdstore.TransactionResult{}, errs.New(errs.KindInternal, "Blueprint terminal transaction store is required")
	}
	if sourceAdvance != nil {
		envelope.projectionOnly = true
		if err := repository.blueprintTerminalStore.ValidateBlueprintTaskTerminal(ctx, envelope); err != nil {
			return etcdstore.TransactionResult{}, err
		}
		return etcdstore.TransactionResult{}, sourceAdvance.execute(ctx, repository)
	}
	transaction, err := repository.blueprintTerminalStore.TransactBlueprintTaskTerminal(ctx, envelope)
	if err == nil && !transaction.Succeeded {
		clearKeyValues(transaction.FailureReads)
		return etcdstore.TransactionResult{}, errs.New(errs.KindStateConflict, "blueprint terminal authority changed")
	}
	return transaction, err
}
