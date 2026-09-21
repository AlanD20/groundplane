package etcd

import (
	"bytes"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupPruneTransactionPlan struct {
	conditions   []etcdstore.Condition
	mutations    []etcdstore.Mutation
	authority    *backupTaskPublicationAuthority
	readRevision int64
}

func (plan backupPruneTransactionPlan) composeTransaction(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	composedConditions := append(append([]etcdstore.Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]etcdstore.Mutation, 0, len(mutations)+len(plan.mutations))
	for _, mutation := range mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	for _, mutation := range plan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	if err := validateBackupRuntimeTransactionBounds(
		composedConditions,
		composedMutations,
	); err != nil {
		clearBackupRuntimeMutations(composedMutations)
		return nil, nil, err
	}
	return composedConditions, composedMutations, nil
}

func (plan *backupPruneTransactionPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
	plan.authority = nil
	plan.readRevision = 0
}

func (plan backupPruneTransactionPlan) taskIdempotencyPlan(
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
	initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	if plan.authority == nil || plan.authority.taskType != taskjournal.TaskBackupPrune {
		return nil, errs.New(errs.KindInternal, "backup prune Task publication authority is missing")
	}
	return prepareBackupTaskIdempotencyPlan(
		*plan.authority,
		plan.conditions,
		plan.mutations,
		record,
		sealed,
		marker,
		initiation,
	)
}

// Rationale: a prune retry must reacquire every operation-owned pending
// tombstone in the same idempotent transaction that publishes its new system
// Task. The terminal source Task remains immutable retry evidence.
func (plan backupPruneTransactionPlan) taskRetryIdempotencyPlan(
	source etcdstore.Versioned[TaskRecord],
	retry TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
) (*idempotencyMutationPlan, error) {
	authority := backupTaskPublicationAuthority{}
	if plan.authority != nil {
		authority = *plan.authority
	}
	if authority.retryOf == "" {
		authority.retryOf = source.Record.ID
	}
	if plan.authority == nil || plan.authority.taskType != taskjournal.TaskBackupPrune ||
		plan.readRevision <= 0 ||
		source.Revision <= 0 || source.ReadRevision != plan.readRevision ||
		source.Record.ID != authority.retryOf ||
		source.Record.Type != taskjournal.TaskBackupPrune ||
		source.Record.OperationID != plan.authority.operationID ||
		source.Record.Owner.EnvironmentID != plan.authority.environmentID ||
		source.Record.Target != plan.authority.environmentID {
		return nil, errs.New(errs.KindValidationFailed, "backup prune Task retry authority is invalid")
	}
	expected, err := cloneRetryTask(
		source.Record,
		authority.taskID,
		taskjournal.TaskActorSystem,
		authority.createdAt,
	)
	if err != nil {
		return nil, err
	}
	expected.PlanID = retry.PlanID
	expected.PlanHash = retry.PlanHash
	expected.RenderGeneration = retry.RenderGeneration
	expected.Steps = cloneTaskSteps(retry.Steps)
	expected.TimeoutSeconds = retry.TimeoutSeconds
	if !backupTaskRecordsEqual(expected, retry) {
		return nil, errs.New(errs.KindValidationFailed, "backup prune retry Task is invalid")
	}
	initiation, err := newInheritedTaskInitiation(source, taskjournal.TaskActorSystem)
	if err != nil {
		return nil, err
	}
	conditions := append([]etcdstore.Condition(nil), plan.conditions...)
	conditions = append(conditions, etcdstore.Condition{
		Key: taskjournal.TaskStorageKey(source.Record.ID), ModRevision: source.Revision,
	})
	return prepareBackupTaskIdempotencyPlan(
		authority,
		conditions,
		plan.mutations,
		retry,
		sealed,
		marker,
		initiation,
	)
}

func backupTaskRecordsEqual(left TaskRecord, right TaskRecord) bool {
	leftValue, leftErr := encodeTaskRecord(left)
	if leftErr != nil {
		return false
	}
	defer clear(leftValue)
	rightValue, rightErr := encodeTaskRecord(right)
	if rightErr != nil {
		return false
	}
	defer clear(rightValue)
	return bytes.Equal(leftValue, rightValue)
}
