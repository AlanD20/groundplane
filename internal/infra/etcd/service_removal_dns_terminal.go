package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// An unassigned direct removal has no workload effects to restore. Its native
// terminal transition releases the removal fence, not the resource or address.
// Authored children remain bounded and settled by their visible Apply parent.
func (repository *TaskRepository) finishPendingRemovalDNSWithdrawal(ctx context.Context,
	current keyvalue.Versioned[TaskRecord], at time.Time,
) (bool, error) {
	key := environmentchanges.ServiceRemovalIntentKey(current.Record.ID)
	zoneRemoval := current.Record.Params[taskjournal.TaskZoneRemovalOperationParam] != ""
	if zoneRemoval {
		_, operationID, err := zoneRemovalTaskIdentity(current.Record)
		if err != nil {
			return false, err
		}
		key = environmentchanges.ZoneRemovalIntentKey(operationID)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: current.ReadRevision},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "waiting removal intent is unavailable")
	}
	authored := false
	if zoneRemoval {
		intent, err := environmentchanges.DecodeZoneRemovalIntent(read.Values[0].Value)
		if err != nil {
			return false, err
		}
		if err := validateZoneRemovalTaskOwner(current.Record, intent); err != nil {
			return false, err
		}
		authored = intent.Authored != nil
	} else {
		intent, err := environmentchanges.DecodeServiceRemovalIntent(read.Values[0].Value)
		if err != nil {
			return false, err
		}
		if err := validateServiceRemovalTaskOwner(current.Record, intent); err != nil {
			return false, err
		}
		authored = intent.Authored != nil
	}
	if authored {
		return false, nil
	}
	status := taskjournal.TaskStatusTimedOut
	result := taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultCompose,
		Diagnostic: taskjournal.TaskResultDiagnosticDNSWithdrawalTimedOut}
	var sourceConditions []keyvalue.Condition
	if at.Before(current.Record.CreatedAt.Add(time.Duration(current.Record.TimeoutSeconds) * time.Second)) {
		purpose := resolverServingAcknowledgement
		if zoneRemoval {
			purpose = resolverWithdrawalBeforeEffects
		}
		ack, ready, err := readResolverStateAcknowledgement(ctx, repository.store, current.ReadRevision, purpose)
		if kind, _ := errs.KindOf(err); kind == errs.KindValidationFailed {
			return false, nil // Missing/disabled resolver remains bounded by removal's deadline.
		}
		if err != nil || !ready {
			return false, err
		}
		failedTaskID, conditions, err := failedResolverConfigurationRequest(
			ctx,
			repository.store,
			current.ReadRevision,
			ack,
		)
		if err != nil || failedTaskID == "" {
			return false, err
		}
		sourceConditions = conditions
		status = taskjournal.TaskStatusFailed
		result.Diagnostic = taskjournal.TaskResultDiagnosticDNSWithdrawalFailed
		result.DNSWithdrawalTaskID = failedTaskID
	}
	plan, err := repository.preparePendingTaskTerminal(ctx, current, status, at, &result)
	if err != nil {
		return false, err
	}
	defer plan.clear()
	var change removalTaskChange
	if zoneRemoval {
		change.conditions, change.mutations, err = repository.prepareZoneRemovalAcknowledgement(ctx, current.Record,
			status, *plan.record.FinishedAt, current.ReadRevision, taskassignments.TaskAssignmentRecord{})
		change.applies = err == nil
		for _, mutation := range change.mutations {
			if mutation.Type == keyvalue.MutationPut {
				change.values = append(change.values, mutation.Value)
			}
		}
	} else {
		change, err = repository.prepareServiceRemovalTaskAcknowledgement(ctx, current.Record,
			status, *plan.record.FinishedAt, current.ReadRevision, taskassignments.TaskAssignmentRecord{})
	}
	if err != nil {
		return false, err
	}
	defer clearRemovalTaskChange(change)
	if !change.applies {
		return false, errs.New(errs.KindInternal, "waiting removal has no native terminal transition")
	}
	conditions := append(plan.conditions, sourceConditions...)
	conditions = append(conditions, change.conditions...)
	byKey := make(map[string]keyvalue.Condition, len(conditions))
	unique := make([]keyvalue.Condition, 0, len(conditions))
	for _, condition := range conditions {
		if previous, exists := byKey[condition.Key]; exists {
			if previous != condition {
				return false, errs.New(errs.KindInternal, "DNS withdrawal terminal source comparisons disagree")
			}
			continue
		}
		byKey[condition.Key] = condition
		unique = append(unique, condition)
	}
	conditions = unique
	mutations := append(plan.mutations, change.mutations...)
	binding, err := repository.bindOrdinaryTaskEnvironmentMutation(ctx, current.Record,
		current.ReadRevision, conditions, mutations, false, false, !zoneRemoval)
	if err != nil {
		return false, err
	}
	if binding == nil {
		return false, errs.New(errs.KindInternal, "waiting removal has no Environment fence")
	}
	mutations = binding.Mutations()
	defer keyvalue.ClearMutationValues(mutations)
	transaction, err := repository.store.Transact(ctx, binding.Conditions(), mutations)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return false, err
	}
	// A conflict requires rediscovery at a fresh revision, never use old evidence.
	return true, nil
}
