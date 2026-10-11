package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintChildTransaction is constructed only by the ready-unit publisher.
// Complete resource/source claims use the existing Blueprint transaction arms,
// not the smaller generic Store budget and not a caller-selected allowance.
type BlueprintChildTransaction struct {
	parentID      string
	childID       string
	planID        string
	environmentID string
	conditions    []keyvalue.Condition
	mutations     []keyvalue.Mutation
}

type blueprintChildTransactionStore interface {
	TransactBlueprintChild(context.Context, BlueprintChildTransaction) (keyvalue.TransactionResult, error)
}

func (transaction BlueprintChildTransaction) Operations() ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if err := transaction.validate(); err != nil {
		return nil, nil, err
	}
	return append(
			[]keyvalue.Condition(nil),
			transaction.conditions...), cloneBlueprintCandidateMutations(
			transaction.mutations,
		), nil
}

func (transaction BlueprintChildTransaction) validate() error {
	if ids.Validate(ids.KindTask, transaction.parentID) != nil ||
		ids.Validate(ids.KindTask, transaction.childID) != nil ||
		transaction.parentID == transaction.childID ||
		ids.Validate(ids.KindPlan, transaction.planID) != nil ||
		ids.Validate(ids.KindEnvironment, transaction.environmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint child transaction identity is invalid")
	}
	if err := validateEnvironmentBlueprintTransactionBudget(transaction.conditions, transaction.mutations); err != nil {
		return err
	}
	parentCompared, headCompared, childAbsent := false, false, false
	for _, condition := range transaction.conditions {
		if condition.Prefix {
			continue
		}
		switch condition.Key {
		case taskjournal.TaskStorageKey(transaction.parentID):
			parentCompared = condition.ModRevision > 0
		case blueprintunits.DesiredPlanKey(transaction.environmentID):
			headCompared = condition.ModRevision > 0
		case taskjournal.TaskStorageKey(transaction.childID):
			childAbsent = condition.ModRevision == 0
		}
	}
	if !parentCompared || !headCompared || !childAbsent {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint child transaction lost its parent, desired-plan or identity fence",
		)
	}
	childWritten := false
	for _, mutation := range transaction.mutations {
		if mutation.Key != taskjournal.TaskStorageKey(transaction.childID) {
			continue
		}
		if childWritten || mutation.Type != keyvalue.MutationPut {
			return errs.New(errs.KindValidationFailed, "Blueprint child transaction changes its Task more than once")
		}
		child, err := DecodeTaskRecord(mutation.Value)
		if err != nil || child.ID != transaction.childID || child.PlanID != transaction.planID ||
			child.Owner.EnvironmentID != transaction.environmentID || child.Params[taskjournal.TaskBlueprintParentParam] != transaction.parentID ||
			child.Executor != taskjournal.TaskExecutorAgent || child.Status != taskjournal.TaskStatusPending {
			return errs.New(errs.KindValidationFailed, "Blueprint child transaction differs from its sealed Task")
		}
		childWritten = true
	}
	if !childWritten {
		return errs.New(errs.KindValidationFailed, "Blueprint child transaction has no Task publication")
	}
	return nil
}

func (s *store) TransactBlueprintChild(
	ctx context.Context,
	transaction BlueprintChildTransaction,
) (keyvalue.TransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if err := transaction.validate(); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if err := validateEnvironmentBlueprintTransactionBudget(keyvalue.ImageSelectionConditions(ctx, transaction.conditions), transaction.mutations); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	// The shared executor still enforces exact physical serialized size. The
	// existing 256-operation Blueprint arm limit remains unchanged.
	return s.transact(ctx, transaction.conditions, transaction.mutations)
}
