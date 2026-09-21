package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

// backupRunPublicationPlan is the persistence-private seam used by future Task
// publication. Its lock, run, membership, exclusions, and epoch mutations are
// appended to the caller's Task mutations and committed once.
type backupRunPublicationPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     backupruntime.BackupRunRecord
	replay     func(context.Context, idempotencyrecord.IdempotencyMarker, int64, int64) error
}

func (plan backupRunPublicationPlan) composeTransaction(
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
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(
		composedConditions,
		composedMutations,
	); err != nil {
		etcdstore.ClearMutationValues(composedMutations)
		return nil, nil, err
	}
	return composedConditions, composedMutations, nil
}

func (plan *backupRunPublicationPlan) clear() {
	for index := range plan.mutations {
		clear(plan.mutations[index].Value)
		plan.mutations[index].Value = nil
	}
	plan.replay = nil
}

// taskIdempotencyPlan composes the real Task primary, owner indexes, runtime
// authority, and deferred idempotency envelope into one publication plan.
func (plan backupRunPublicationPlan) taskIdempotencyPlan(
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
	initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	idempotencyPlan, err := prepareBackupTaskIdempotencyPlan(
		backupTaskPublicationAuthority{
			taskID: plan.record.TaskID, operationID: plan.record.OperationID,
			environmentID: plan.record.EnvironmentID, taskType: taskjournal.TaskBackup,
			retryOf: plan.record.RetryOfTaskID, createdAt: plan.record.CreatedAt,
			validatePlan: func(value *agentpb.ExecutionPlan) error {
				return validateBackupRunExecutionPlan(plan.record, value)
			},
		},
		plan.conditions,
		plan.mutations,
		record,
		sealed,
		marker,
		initiation,
	)
	if err != nil {
		return nil, err
	}
	if plan.replay != nil {
		if err := idempotencyPlan.enforceExistingReplay(plan.replay); err != nil {
			return nil, err
		}
	}
	return idempotencyPlan, nil
}

type backupTaskPublicationAuthority struct {
	taskID        string
	operationID   string
	environmentID string
	taskType      taskjournal.TaskType
	retryOf       string
	createdAt     time.Time
	validatePlan  backupTaskPlanValidator
}

func prepareBackupTaskIdempotencyPlan(
	authority backupTaskPublicationAuthority,
	domainConditions []etcdstore.Condition,
	domainMutations []etcdstore.Mutation,
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
	initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	if record.ID != authority.taskID || record.OperationID != authority.operationID ||
		record.RetryOf != authority.retryOf ||
		record.Owner.EnvironmentID != authority.environmentID ||
		record.Type != authority.taskType || record.Target != authority.environmentID ||
		record.Executor != taskjournal.TaskExecutorAgent || record.Status != taskjournal.TaskStatusPending ||
		!record.CreatedAt.Equal(authority.createdAt) || marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending || marker.TaskID != record.ID ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != authority.environmentID ||
		!marker.CreatedAt.Equal(record.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup Task publication identity is invalid",
		)
	}
	if err := validateBackupTaskSealedPlan(authority, record, sealed); err != nil {
		return nil, err
	}
	record = cloneTaskRecord(record)
	if record.IdempotencyKey == "" {
		record.IdempotencyKey = marker.Locator.Key
	}
	record.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if ValidateTaskRecord(record) != nil || idempotencyrecord.ValidateIdempotencyMarker(marker) != nil ||
		validateTaskInitiation(record, initiation, true) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backup Task publication is invalid")
	}
	taskValue, err := EncodeTaskRecord(record)
	if err != nil {
		return nil, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(record.ID)
	if err != nil {
		return nil, err
	}
	defer clear(reference)
	taskConditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(record.ID)},
		{Key: taskjournal.TaskOperationIndexKey(record.OperationID, record.ID)},
		{Key: taskjournal.TaskActiveOperationKey(record.OperationID)},
		{Key: taskjournal.TaskQueueKey(record.Executor, record.ID)},
	}
	taskMutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(record.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(record.OperationID, record.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(record.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(record.Executor, record.ID), Value: reference},
	}
	domain := backupRunPublicationPlan{conditions: domainConditions, mutations: domainMutations}
	conditions, mutations, err := domain.composeTransaction(taskConditions, taskMutations)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	taskClassifier := classifyTaskCreateConflict(record.OperationID)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "backup Task publication evidence is incomplete")
		}
		if err := taskClassifier(revision, values[:len(taskConditions)]); err != nil {
			return err
		}
		return errs.New(errs.KindStateConflict, "backup run publication state changed")
	}
	idempotencyPlan, err := newTaskIdempotencyMutationPlan(
		record,
		initiation,
		conditions,
		mutations,
		classify,
	)
	if err != nil {
		return nil, err
	}
	if err := idempotencyPlan.enforceTransactionBounds(
		backupruntime.ValidateBackupRuntimeTransactionBounds,
	); err != nil {
		return nil, err
	}
	return idempotencyPlan, nil
}
