package etcd

import (
	"bytes"
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumBackupRuntimeListLimit = 96
const maximumBackupPruneBatch = 11

type BackupRuntimeListRequest struct {
	Limit          int
	StartExclusive string
	Revision       int64
}

type BackupRuntimePage[T any] struct {
	Items    []etcdstore.Versioned[T]
	Next     string
	Revision int64
}

// BackupRecoveryPointPageRequest is the stable-id public paging seam. The
// repository alone translates AfterID to its private inverted index key.
type BackupRecoveryPointPageRequest struct {
	Limit    int
	AfterID  string
	Revision int64
}

// BackupRecoveryPointPage never exposes an etcd key or inverted-id layout.
type BackupRecoveryPointPage struct {
	Items    []etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]
	NextID   string
	Revision int64
}

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
	if plan.authority == nil || plan.authority.taskType != TaskBackupPrune {
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
	if plan.authority == nil || plan.authority.taskType != TaskBackupPrune ||
		plan.readRevision <= 0 ||
		source.Revision <= 0 || source.ReadRevision != plan.readRevision ||
		source.Record.ID != authority.retryOf ||
		source.Record.Type != TaskBackupPrune ||
		source.Record.OperationID != plan.authority.operationID ||
		source.Record.Owner.EnvironmentID != plan.authority.environmentID ||
		source.Record.Target != plan.authority.environmentID {
		return nil, errs.New(errs.KindValidationFailed, "backup prune Task retry authority is invalid")
	}
	expected, err := cloneRetryTask(
		source.Record,
		authority.taskID,
		TaskActorSystem,
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
	initiation, err := newInheritedTaskInitiation(source, TaskActorSystem)
	if err != nil {
		return nil, err
	}
	conditions := append([]etcdstore.Condition(nil), plan.conditions...)
	conditions = append(conditions, etcdstore.Condition{
		Key: taskKey(source.Record.ID), ModRevision: source.Revision,
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

func (repository *BackupRuntimeRepository) GetBackupOrphan(
	ctx context.Context,
	recoveryPointID string,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	primaryKey := backupruntime.BackupOrphanKey(recoveryPointID)
	initial, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if initial == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup orphan read is empty",
		)
	}
	if initial.Entry == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{ReadRevision: initial.ReadRevision}, false, nil
	}
	defer clear(initial.Entry.Value)
	record, err := backupruntime.DecodeBackupOrphanRecord(initial.Entry.Value)
	if err != nil || record.Point.ID != recoveryPointID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	membershipKey, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		record.Point.EnvironmentID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorMembershipKey, err := backupruntime.BackupOrphanConnectorIndexKey(
		record.Point.ConnectorID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{primaryKey, membershipKey, connectorMembershipKey},
		Revision: initial.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if authority == nil || authority.ReadRevision != initial.ReadRevision ||
		len(authority.Values) != 3 || authority.Values[0] == nil ||
		authority.Values[1] == nil || authority.Values[2] == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(authority.Values)
	stored, err := backupruntime.DecodeBackupOrphanRecord(authority.Values[0].Value)
	expectedVersion := int64(1)
	if stored.State == backupruntime.BackupOrphanDelete {
		expectedVersion = 2
	}
	if err != nil || stored != record ||
		authority.Values[0].ModRevision != initial.Entry.ModRevision ||
		authority.Values[1].Key != membershipKey ||
		authority.Values[2].Key != connectorMembershipKey ||
		authority.Values[0].Version != expectedVersion ||
		authority.Values[1].Version != expectedVersion ||
		authority.Values[2].Version != expectedVersion ||
		authority.Values[1].ModRevision != authority.Values[0].ModRevision ||
		authority.Values[2].ModRevision != authority.Values[0].ModRevision ||
		string(authority.Values[1].Value) != recoveryPointID ||
		string(authority.Values[2].Value) != recoveryPointID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record:       stored,
		Revision:     authority.Values[0].ModRevision,
		ReadRevision: authority.ReadRevision,
	}, true, nil
}

func (repository *BackupRuntimeRepository) CreateBackupOrphan(
	ctx context.Context,
	authority BackupAssignmentInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	ordinal uint32,
	orphan backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := changedBackupSourceOrdinal(current.Record, next)
	if int(ordinal) >= len(current.Record.Sources) || !changed || changedOrdinal != ordinal ||
		validateBackupRunTransition(current.Record, next, backupRunTransitionOrphanCreate) != nil ||
		!backupPointMatchesRunSource(orphan.Point, next, ordinal) || orphan.TaskID != next.TaskID ||
		orphan.State != backupruntime.BackupOrphanInspect {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan creation is invalid",
		)
	}
	orphan.Reconciliation = backupruntime.BackupOrphanReconciliationAuthority{
		OperationID:    next.OperationID,
		PolicyRevision: next.PolicyRevision,
		RetentionKeep:  next.RetentionKeep,
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(orphan)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Point.ConnectorID, orphan.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		orphan.Point.EnvironmentID,
		orphan.Point.ID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupOrphanKey(orphan.Point.ID)},
		{Key: connectorIndex},
		{Key: environmentIndex},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupOrphanKey(orphan.Point.ID), Value: value},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorIndex,
			Value: []byte(orphan.Point.ID),
		},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(orphan.Point.ID)},
	}
	return repository.replaceBackupRun(
		ctx,
		current,
		next,
		conditions,
		mutations,
		nil,
		&authority,
		nil,
	)
}

// TransitionReconciledBackupOrphan advances Controller-owned orphan repair
// without depending on the originating Task, assignment, or Backup run.
func (repository *BackupRuntimeRepository) TransitionReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	next backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], error) {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		next.State != backupruntime.BackupOrphanDelete || current.Record.Point != next.Point ||
		current.Record.TaskID != next.TaskID ||
		current.Record.Reconciliation != next.Reconciliation ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) || next.CreatedAt != current.Record.CreatedAt {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan reconciliation transition is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(next.Point.ConnectorID, next.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(next.Point.EnvironmentID, next.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	keys := []string{backupruntime.BackupOrphanKey(next.Point.ID), connectorIndex, environmentIndex}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] != nil {
		stored, decodeErr := backupruntime.DecodeBackupOrphanRecord(anchor.Values[0].Value)
		if decodeErr == nil && stored == next &&
			validateBackupOrphanCompanionEvidence(anchor.Values, next) == nil {
			return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
				Record: stored, Revision: anchor.Values[0].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	if err := validateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: value},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: []byte(next.Point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[2], Value: []byte(next.Point.ID)},
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// DeleteReconciledBackupOrphan consumes verified-absent Controller authority.
func (repository *BackupRuntimeRepository) DeleteReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
) error {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanDelete {
		return errs.New(errs.KindValidationFailed, "backup orphan reconciliation deletion is invalid")
	}
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(
		current.Record.Point.ConnectorID,
		current.Record.Point.ID,
	)
	if err != nil {
		return err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		current.Record.Point.EnvironmentID,
		current.Record.Point.ID,
	)
	if err != nil {
		return err
	}
	keys := []string{backupruntime.BackupOrphanKey(current.Record.Point.ID), connectorIndex, environmentIndex}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil && anchor.Values[1] == nil && anchor.Values[2] == nil {
		return nil
	}
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return errs.New(errs.KindStateConflict, "backup orphan reconciliation authority changed")
	}
	if err := validateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return err
	}
	result, err := repository.transact(ctx, []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
	})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		return errs.New(errs.KindStateConflict, "backup orphan reconciliation authority changed")
	}
	return nil
}

// AdoptReconciledBackupOrphan promotes a verified artifact and starts its
// captured retention sweep without consulting the originating Task or run.
func (repository *BackupRuntimeRepository) AdoptReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	point backupruntime.BackupRecoveryPointRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], error) {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		point.BackupRecoveryPointSnapshot != current.Record.Point ||
		sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.Revision != current.Record.Reconciliation.PolicyRevision ||
		sweep.Keep != current.Record.Reconciliation.RetentionKeep ||
		sweep.State != backupruntime.BackupRetentionPending || sweep.CreatedAt != point.VerifiedAt ||
		sweep.UpdatedAt != point.VerifiedAt {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan reconciliation adoption is invalid",
		)
	}
	pointValue, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer clear(pointValue)
	sweepValue, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer clear(sweepValue)
	orphanConnectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	orphanEnvironmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	keys := []string{
		backupruntime.BackupOrphanKey(point.ID), orphanConnectorIndex, orphanEnvironmentIndex,
		backupruntime.BackupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex,
		backupruntime.BackupRetentionKey(point.SourceID, point.ID),
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil {
		if anchor.Values[1] != nil || anchor.Values[2] != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		allTargetsAbsent := true
		for _, value := range anchor.Values[3:] {
			allTargetsAbsent = allTargetsAbsent && value == nil
		}
		if allTargetsAbsent {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup orphan reconciliation authority disappeared",
			)
		}
		if err := validateReconciledBackupPointReplay(anchor.Values[3:], point, sweep); err != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
		}
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
			Record: point, Revision: anchor.Values[3].ModRevision, ReadRevision: anchor.ReadRevision,
		}, nil
	}
	if anchor.Values[0].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	if err := validateBackupOrphanCompanionEvidence(anchor.Values[:3], current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	for _, value := range anchor.Values[3:] {
		if value != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup orphan adoption target already exists",
			)
		}
	}
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for position, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if position < 3 {
			condition.ModRevision = current.Revision
		}
		conditions = append(conditions, condition)
	}
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
		{Type: etcdstore.MutationPut, Key: keys[3], Value: pointValue},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[5], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[6], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[7], Value: sweepValue},
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
		Record: point, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func validateReconciledBackupPointReplay(
	values []*etcdstore.KeyValue,
	point backupruntime.BackupRecoveryPointRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) error {
	if len(values) != 5 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[3] == nil || values[4] == nil || values[0].Version != 1 ||
		values[1].Version != 1 || values[2].Version != 1 || values[3].Version != 1 ||
		values[4].Version != 1 || values[1].ModRevision != values[0].ModRevision ||
		values[2].ModRevision != values[0].ModRevision || values[3].ModRevision != values[0].ModRevision ||
		values[4].ModRevision != values[0].ModRevision || string(values[1].Value) != point.ID ||
		string(values[2].Value) != point.ID || string(values[3].Value) != point.ID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	storedPoint, pointErr := backupruntime.DecodeBackupRecoveryPointRecord(values[0].Value)
	storedSweep, sweepErr := backupruntime.DecodeBackupRetentionSweepRecord(values[4].Value)
	if pointErr != nil || sweepErr != nil ||
		storedPoint.BackupRecoveryPointSnapshot != point.BackupRecoveryPointSnapshot ||
		storedSweep.SourceID != storedPoint.SourceID ||
		storedSweep.TriggerRecoveryPointID != storedPoint.ID ||
		storedSweep.Keep != sweep.Keep || storedSweep.Revision != sweep.Revision ||
		storedSweep.State != backupruntime.BackupRetentionPending ||
		storedSweep.CreatedAt != storedPoint.VerifiedAt ||
		storedSweep.UpdatedAt != storedPoint.VerifiedAt {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if storedPoint != point || storedSweep != sweep {
		return errs.New(
			errs.KindStateConflict,
			"backup orphan adoption was completed by another reconciler",
		)
	}
	return nil
}

func (repository *BackupRuntimeRepository) TransitionBackupOrphan(
	ctx context.Context,
	authority BackupAssignmentInput,
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	next backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], error) {
	next.Reconciliation = current.Record.Reconciliation
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		next.State != backupruntime.BackupOrphanDelete ||
		current.Record.Point != next.Point ||
		current.Record.TaskID != next.TaskID ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) ||
		next.CreatedAt != current.Record.CreatedAt ||
		next.TaskID != run.Record.TaskID ||
		!runContainsOrphanedPoint(run.Record, current.Record) {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan transition is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(next.Point.ConnectorID, next.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		next.Point.EnvironmentID,
		next.Point.ID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	anchor, err := repository.readCurrentKeys(ctx, []string{
		backupruntime.BackupRunKey(
			run.Record.TaskID,
		),
		backupruntime.BackupOrphanKey(next.Point.ID),
		connectorIndex,
		environmentIndex,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != run.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan state changed",
		)
	}
	storedRun, decodeErr := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	if decodeErr != nil || !backupRunRecordsEqual(storedRun, run.Record) {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if authority.TaskID != run.Record.TaskID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan assignment does not match its run",
		)
	}
	assignmentConditions, err := repository.loadBackupAssignmentFence(
		ctx,
		authority,
		anchor.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if anchor.Values[1] != nil {
		storedOrphan, orphanErr := backupruntime.DecodeBackupOrphanRecord(anchor.Values[1].Value)
		if orphanErr == nil && storedOrphan == next &&
			validateBackupOrphanCompanionEvidence(anchor.Values[1:], next) == nil {
			return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
				Record: storedOrphan, Revision: anchor.Values[1].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if anchor.Values[1] == nil || anchor.Values[1].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan companion state changed",
		)
	}
	if err := validateBackupOrphanCompanionEvidence(anchor.Values[1:], current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	evidence, err := repository.loadOwnedEvidence(ctx, run.Record, anchor.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupRunKey(run.Record.TaskID), ModRevision: run.Revision},
		{Key: backupruntime.BackupOrphanKey(next.Point.ID), ModRevision: current.Revision},
		{Key: connectorIndex, ModRevision: current.Revision},
		{Key: environmentIndex, ModRevision: current.Revision},
	}
	conditions = append(conditions, evidence.fence.transactionConditions()...)
	conditions = append(conditions, assignmentConditions...)
	epoch, err := evidence.fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clear(epoch.Value)
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupOrphanKey(next.Point.ID), Value: value},
		{Type: etcdstore.MutationPut, Key: connectorIndex, Value: []byte(next.Point.ID)},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(next.Point.ID)},
		epoch,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan state changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func (repository *BackupRuntimeRepository) prepareBackupOrphanAbsentTerminal(
	ctx context.Context,
	checkpoint BackupCheckpointInput,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	orphan etcdstore.Versioned[backupruntime.BackupOrphanRecord],
) (backupRunPublicationPlan, error) {
	changedOrdinal, changed := changedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		orphan.Revision <= 0 ||
		orphan.Record.State != backupruntime.BackupOrphanDelete || orphan.Record.TaskID != currentRun.Record.TaskID ||
		orphan.Record.Point.ID != currentRun.Record.Sources[ordinal].RecoveryPointID ||
		validateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupRunTransitionOrphanDelete,
		) != nil {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan deletion is invalid",
		)
	}
	if checkpoint.Payload.Kind != BackupCheckpointRemoteObjectAbsent ||
		checkpoint.Payload.PointID != orphan.Record.Point.ID {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan absence checkpoint is invalid",
		)
	}
	return repository.prepareBackupRunTerminalPlan(ctx, currentRun, nextRun, &orphan, &checkpoint)
}

// advanceBackupRunAfterRetention is the only point-committed to cleanup seam.
// It pins the exact completed sweep under the owning Backup lock.
func (repository *BackupRuntimeRepository) advanceBackupRunAfterRetention(
	ctx context.Context,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	sweep etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := changedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		sweep.Revision <= 0 || sweep.Record.State != backupruntime.BackupRetentionCompleted ||
		!backupRetentionSweepMatchesRun(currentRun.Record, sweep.Record) ||
		sweep.Record.SourceID != currentRun.Record.Sources[ordinal].SourceID ||
		sweep.Record.TriggerRecoveryPointID != currentRun.Record.Sources[ordinal].RecoveryPointID ||
		validateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupRunTransitionRetentionComplete,
		) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup retention completion is invalid",
		)
	}
	key := backupruntime.BackupRetentionKey(sweep.Record.SourceID, sweep.Record.TriggerRecoveryPointID)
	return repository.replaceBackupRun(
		ctx,
		currentRun,
		nextRun,
		[]etcdstore.Condition{{Key: key, ModRevision: sweep.Revision}},
		nil,
		func(values []*etcdstore.KeyValue) error {
			if len(values) != 1 || values[0] == nil || values[0].ModRevision != sweep.Revision {
				return errs.New(errs.KindStateConflict, "backup retention authority changed")
			}
			stored, err := backupruntime.DecodeBackupRetentionSweepRecord(values[0].Value)
			if err != nil || stored != sweep.Record || stored.State != backupruntime.BackupRetentionCompleted ||
				!backupRetentionSweepMatchesRun(currentRun.Record, stored) {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
			return nil
		},
		nil,
		nil,
	)
}

func (repository *BackupRuntimeRepository) CommitBackupRecoveryPoint(
	ctx context.Context,
	authority BackupAssignmentInput,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	point backupruntime.BackupRecoveryPointRecord,
	orphan *etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	sweep backupruntime.BackupRetentionSweepRecord,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := changedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		validateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupRunTransitionPointCommit,
		) != nil ||
		!backupPointMatchesRunSource(point.BackupRecoveryPointSnapshot, nextRun, ordinal) ||
		sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.Revision != nextRun.PolicyRevision || sweep.Keep != nextRun.RetentionKeep ||
		sweep.State != backupruntime.BackupRetentionPending {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"recovery point commit is invalid",
		)
	}
	if currentRun.Record.Sources[ordinal].State == backupruntime.BackupSourceAttemptOrphaned {
		if orphan == nil || orphan.Revision <= 0 ||
			!backupOrphanMatchesRunSource(orphan.Record, currentRun.Record, ordinal) ||
			orphan.Record.Point != point.BackupRecoveryPointSnapshot ||
			orphan.Record.State != backupruntime.BackupOrphanInspect ||
			orphan.Record.TaskID != currentRun.Record.TaskID {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"orphan-backed Recovery Point commit is invalid",
			)
		}
	} else if orphan != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"direct Recovery Point commit cannot carry an orphan",
		)
	}
	pointValue, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(pointValue)
	sweepValue, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(sweepValue)
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	orphanConnectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	orphanEnvironmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{
			Key:         connectorrecord.RecordKey(point.ConnectorID),
			ModRevision: currentRun.Record.ConnectorRevision,
		},
		{
			Key:         connectorrecord.CredentialValueKey(point.ConnectorID),
			ModRevision: currentRun.Record.ConnectorCredentialsRevision,
		},
		{Key: backupruntime.BackupRecoveryPointKey(point.ID)},
		{Key: environmentIndex},
		{Key: sourceIndex},
		{Key: connectorIndex},
		{Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: sweepValue},
	}
	if orphan != nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: backupruntime.BackupOrphanKey(point.ID), ModRevision: orphan.Revision},
			etcdstore.Condition{Key: orphanConnectorIndex, ModRevision: orphan.Revision},
			etcdstore.Condition{Key: orphanEnvironmentIndex, ModRevision: orphan.Revision},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: backupruntime.BackupOrphanKey(point.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: orphanConnectorIndex},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: orphanEnvironmentIndex},
		)
	}
	validateCompanions := func(values []*etcdstore.KeyValue) error {
		if err := validateBackupConnectorSnapshotEvidence(
			values[:2],
			currentRun.Record,
		); err != nil {
			return err
		}
		if orphan == nil {
			return nil
		}
		return validateBackupOrphanCompanionEvidence(values[len(values)-3:], orphan.Record)
	}
	updatedRun, err := repository.replaceBackupRun(
		ctx,
		currentRun,
		nextRun,
		conditions,
		mutations,
		validateCompanions,
		&authority,
		nil,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
		Record: point, Revision: updatedRun.Revision, ReadRevision: updatedRun.ReadRevision,
	}, updatedRun, nil
}

func (repository *BackupRuntimeRepository) GetBackupRecoveryPoint(
	ctx context.Context,
	recoveryPointID string,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	record, found, err := getOptionalBackupRuntimeRecord(
		ctx,
		repository.store,
		backupruntime.BackupRecoveryPointKey(recoveryPointID),
		recoveryPointID,
		backupruntime.DecodeBackupRecoveryPointRecord,
		func(point backupruntime.BackupRecoveryPointRecord) string { return point.ID },
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindRecoveryPointNotFound,
			"recovery point was not found",
		)
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(
		record.Record.EnvironmentID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(record.Record.SourceID, recoveryPointID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(
		record.Record.ConnectorID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backupruntime.BackupRecoveryPointKey(recoveryPointID),
			backupruntime.BackupRecoveryPointPruneKey(recoveryPointID),
			environmentIndex,
			sourceIndex,
			connectorIndex,
		},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if authority == nil || authority.ReadRevision != record.ReadRevision ||
		len(authority.Values) != 5 ||
		authority.Values[0] == nil ||
		authority.Values[0].Version != 1 ||
		authority.Values[0].ModRevision != record.Revision ||
		authority.Values[2] == nil || authority.Values[3] == nil || authority.Values[4] == nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(authority.Values)
	for index, expectedKey := range []string{environmentIndex, sourceIndex, connectorIndex} {
		value := authority.Values[index+2]
		if value.Key != expectedKey || value.Version != 1 || value.ModRevision != record.Revision ||
			string(value.Value) != recoveryPointID {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	if authority.Values[1] != nil {
		prune, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(authority.Values[1].Value)
		if decodeErr != nil || prune.Point != record.Record.BackupRecoveryPointSnapshot ||
			prune.State == backupruntime.BackupPruneVerifiedAbsent {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindRecoveryPointNotFound,
			"recovery point was not found",
		)
	}
	return record, nil
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointEnvironmentPrefix+environmentID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.EnvironmentID == environmentID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, point.ID)
		},
	)
}

// ListVerifiedRecoveryPointsByEnvironment keeps every storage-layout detail
// inside the repository while preserving the verified-only fixed revision.
type backupRecoveryPointPageReader func(
	context.Context,
	string,
	BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error)

func (repository *BackupRuntimeRepository) ListVerifiedRecoveryPointsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRecoveryPointPageRequest,
) (BackupRecoveryPointPage, error) {
	return collectVerifiedRecoveryPointPage(
		ctx,
		environmentID,
		request,
		repository.ListBackupRecoveryPointsByEnvironment,
	)
}

func collectVerifiedRecoveryPointPage(
	ctx context.Context,
	environmentID string,
	request BackupRecoveryPointPageRequest,
	read backupRecoveryPointPageReader,
) (BackupRecoveryPointPage, error) {
	storageRequest := BackupRuntimeListRequest{Limit: request.Limit, Revision: request.Revision}
	if request.AfterID != "" {
		boundary, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, request.AfterID)
		if err != nil {
			return BackupRecoveryPointPage{}, err
		}
		storageRequest.StartExclusive = boundary
	}

	result := BackupRecoveryPointPage{}
	for {
		storageRequest.Limit = request.Limit - len(result.Items)
		page, err := read(ctx, environmentID, storageRequest)
		if err != nil {
			return BackupRecoveryPointPage{}, err
		}
		if page.Revision <= 0 {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list returned no fixed revision",
			)
		}
		if result.Revision == 0 {
			result.Revision = page.Revision
		} else if page.Revision != result.Revision {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list changed fixed revision",
			)
		}
		if len(page.Items) > storageRequest.Limit {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list exceeded the visible page limit",
			)
		}
		result.Items = append(result.Items, page.Items...)
		if len(result.Items) == request.Limit || page.Next == "" {
			if page.Next != "" {
				result.NextID, err = backupruntime.BackupRecoveryPointIDFromEnvironmentIndexKey(environmentID, page.Next)
				if err != nil {
					return BackupRecoveryPointPage{}, err
				}
			}
			return result, nil
		}
		if page.Next == storageRequest.StartExclusive {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list boundary did not advance",
			)
		}
		storageRequest.StartExclusive = page.Next
		storageRequest.Revision = result.Revision
	}
}

func (repository *BackupRuntimeRepository) ListBackupRunsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRunRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	prefix := backupruntime.BackupRunEnvironmentPrefix + environmentID + "/"
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindInternal,
			"backup run environment index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	for position, item := range index.Values {
		taskID := string(item.Value)
		if recordcodec.ValidateID(ids.KindTask, taskID) != nil ||
			item.Key != prefix+taskID {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys[position] = backupruntime.BackupRunKey(taskID)
	}
	return repository.readBackupRunMembershipPage(ctx, environmentID, keys, index)
}

func (repository *BackupRuntimeRepository) readBackupRunMembershipPage(
	ctx context.Context,
	environmentID string,
	keys []string,
	index *etcdstore.RangeResult,
) (BackupRuntimePage[backupruntime.BackupRunRecord], error) {
	page := BackupRuntimePage[backupruntime.BackupRunRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(primaries.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupRunRecord], len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		record, decodeErr := backupruntime.DecodeBackupRunRecord(value.Value)
		expectedIndex, keyErr := backupruntime.BackupRunEnvironmentIndexKey(environmentID, record.TaskID)
		if decodeErr != nil || keyErr != nil || record.EnvironmentID != environmentID ||
			expectedIndex != index.Values[position].Key || index.Values[position].Version != 1 ||
			index.Values[position].ModRevision > value.ModRevision ||
			string(index.Values[position].Value) != record.TaskID {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[backupruntime.BackupRunRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}

func (repository *BackupRuntimeRepository) ListBackupOrphansByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupOrphanRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	prefix := backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/"
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindInternal,
			"backup orphan environment index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		expected, keyErr := backupruntime.BackupOrphanEnvironmentIndexKey(environmentID, pointID)
		if keyErr != nil || expected != item.Key {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys[position] = backupruntime.BackupOrphanKey(pointID)
		pointIDs[position] = pointID
	}
	page := BackupRuntimePage[backupruntime.BackupOrphanRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(primaries.Values)
	records := make([]backupruntime.BackupOrphanRecord, len(keys))
	connectorKeys := make([]string, len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		record, decodeErr := backupruntime.DecodeBackupOrphanRecord(value.Value)
		expectedVersion := int64(1)
		if record.State == backupruntime.BackupOrphanDelete {
			expectedVersion = 2
		}
		if decodeErr != nil || record.Point.ID != pointIDs[position] ||
			record.Point.EnvironmentID != environmentID || value.Version != expectedVersion ||
			index.Values[position].Version != expectedVersion ||
			index.Values[position].ModRevision != value.ModRevision {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorKey, keyErr := backupruntime.BackupOrphanConnectorIndexKey(
			record.Point.ConnectorID,
			record.Point.ID,
		)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		records[position] = record
		connectorKeys[position] = connectorKey
	}
	connectors, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: connectorKeys, Revision: index.ReadRevision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if connectors == nil || connectors.ReadRevision != index.ReadRevision ||
		len(connectors.Values) != len(connectorKeys) {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(connectors.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupOrphanRecord], len(keys))
	for position, connector := range connectors.Values {
		record := records[position]
		value := primaries.Values[position]
		expectedVersion := int64(1)
		if record.State == backupruntime.BackupOrphanDelete {
			expectedVersion = 2
		}
		if connector == nil || connector.Key != connectorKeys[position] ||
			connector.Version != expectedVersion || connector.ModRevision != value.ModRevision ||
			string(connector.Value) != record.Point.ID {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsBySource(
	ctx context.Context,
	sourceID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointSourcePrefix+sourceID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.SourceID == sourceID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointSourceIndexKey(sourceID, point.ID)
		},
	)
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsByConnector(
	ctx context.Context,
	connectorID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindConnector, connectorID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointConnectorPrefix+connectorID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.ConnectorID == connectorID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointConnectorIndexKey(connectorID, point.ID)
		},
	)
}

func (repository *BackupRuntimeRepository) listBackupRecoveryPoints(
	ctx context.Context,
	prefix string,
	request BackupRuntimeListRequest,
	belongs func(backupruntime.BackupRecoveryPointRecord) bool,
	indexKey func(backupruntime.BackupRecoveryPointRecord) (string, error),
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, 0, len(index.Values)*2)
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys = append(keys, backupruntime.BackupRecoveryPointKey(pointID), backupruntime.BackupRecoveryPointPruneKey(pointID))
		pointIDs[position] = pointID
	}
	page := BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	points, err := repository.readBackupRecoveryPointPageChunks(ctx, keys, index.ReadRevision)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if points == nil || points.ReadRevision != index.ReadRevision ||
		len(points.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point fixed-revision page is incomplete",
		)
	}
	defer clearKeyValues(points.Values)
	records := make([]backupruntime.BackupRecoveryPointRecord, len(pointIDs))
	visible := make([]bool, len(pointIDs))
	companionKeys := make([]string, 0, len(pointIDs)*3)
	for position, pointID := range pointIDs {
		item := points.Values[position*2]
		if item == nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, decodeErr := backupruntime.DecodeBackupRecoveryPointRecord(item.Value)
		if decodeErr != nil || point.ID != pointID || !belongs(point) || item.Version != 1 ||
			index.Values[position].Version != 1 ||
			index.Values[position].ModRevision != item.ModRevision {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		expectedIndexKey, keyErr := indexKey(point)
		if keyErr != nil || expectedIndexKey != index.Values[position].Key {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		environmentIndex, keyErr := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		sourceIndex, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorIndex, keyErr := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		records[position] = point
		companionKeys = append(companionKeys, environmentIndex, sourceIndex, connectorIndex)
		if pruneValue := points.Values[position*2+1]; pruneValue != nil {
			prune, pruneErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			if pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
				prune.State == backupruntime.BackupPruneVerifiedAbsent {
				return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
			}
			continue
		}
		visible[position] = true
	}
	companions, err := repository.readBackupRecoveryPointPageChunks(
		ctx,
		companionKeys,
		index.ReadRevision,
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if companions == nil || companions.ReadRevision != index.ReadRevision ||
		len(companions.Values) != len(companionKeys) {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point companion page is incomplete",
		)
	}
	defer clearKeyValues(companions.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], 0, len(pointIDs))
	for position, point := range records {
		primary := points.Values[position*2]
		for offset := range 3 {
			companion := companions.Values[position*3+offset]
			expectedKey := companionKeys[position*3+offset]
			if companion == nil || companion.Key != expectedKey || companion.Version != 1 ||
				companion.ModRevision != primary.ModRevision || string(companion.Value) != point.ID {
				return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
			}
		}
		if visible[position] {
			page.Items = append(page.Items, etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
				Record: point, Revision: primary.ModRevision, ReadRevision: index.ReadRevision,
			})
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}

func (repository *BackupRuntimeRepository) readBackupRecoveryPointPageChunks(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "recovery point page chunk input is invalid")
	}
	combined := &etcdstore.GetManyResult{ReadRevision: revision, Values: make([]*etcdstore.KeyValue, 0, len(keys))}
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		chunk, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: keys[start:end], Revision: revision,
		})
		if err != nil {
			clearKeyValues(combined.Values)
			return nil, err
		}
		if chunk == nil || chunk.ReadRevision != revision || len(chunk.Values) != end-start {
			if chunk != nil {
				clearKeyValues(chunk.Values)
			}
			clearKeyValues(combined.Values)
			return nil, errs.New(
				errs.KindInternal,
				"recovery point fixed-revision page chunk is incomplete",
			)
		}
		for position, value := range chunk.Values {
			if value != nil && value.Key != keys[start+position] {
				clearKeyValues(chunk.Values)
				clearKeyValues(combined.Values)
				return nil, errs.New(
					errs.KindInternal,
					"recovery point fixed-revision page chunk is corrupt",
				)
			}
		}
		combined.Values = append(combined.Values, chunk.Values...)
		chunk.Values = nil
	}
	return combined, nil
}

func (repository *BackupRuntimeRepository) GetBackupRetentionSweep(
	ctx context.Context,
	sourceID string,
	triggerRecoveryPointID string,
) (etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord], bool, error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, triggerRecoveryPointID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, err
	}
	record, found, err := getOptionalBackupRuntimeRecord(
		ctx,
		repository.store,
		backupruntime.BackupRetentionKey(sourceID, triggerRecoveryPointID),
		triggerRecoveryPointID,
		backupruntime.DecodeBackupRetentionSweepRecord,
		func(record backupruntime.BackupRetentionSweepRecord) string {
			if record.SourceID != sourceID {
				return ""
			}
			return record.TriggerRecoveryPointID
		},
	)
	if err != nil || !found {
		return record, found, err
	}
	authority, err := repository.readFixedKeys(
		ctx,
		[]string{
			backupruntime.BackupRetentionKey(sourceID, triggerRecoveryPointID),
			backupruntime.BackupRecoveryPointKey(triggerRecoveryPointID),
		},
		record.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, err
	}
	defer clearKeyValues(authority.Values)
	if authority.Values[0] == nil || authority.Values[1] == nil ||
		authority.Values[0].ModRevision != record.Revision || authority.Values[1].Version != 1 {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(authority.Values[1].Value)
	if err != nil || point.ID != triggerRecoveryPointID || point.SourceID != sourceID {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	if record.Record.State == backupruntime.BackupRetentionPending {
		if authority.Values[0].Version != 1 || authority.Values[0].ModRevision != authority.Values[1].ModRevision {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
		}
	} else if authority.Values[0].Version < 2 ||
		authority.Values[0].ModRevision <= authority.Values[1].ModRevision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return record, true, nil
}

func (repository *BackupRuntimeRepository) ListBackupRetentionSweepsBySource(
	ctx context.Context,
	sourceID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRetentionSweepRecord], error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{}, err
	}
	prefix := backupruntime.BackupRetentionPrefix + sourceID + "/"
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{}, err
	}
	result, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{}, errs.New(
			errs.KindInternal,
			"backup retention page is incomplete",
		)
	}
	defer clearRangeValues(result.Values)
	page := BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{
		Items:    make([]etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord], len(result.Values)),
		Revision: result.ReadRevision,
	}
	for index, item := range result.Values {
		record, decodeErr := backupruntime.DecodeBackupRetentionSweepRecord(item.Value)
		if decodeErr != nil || record.SourceID != sourceID ||
			item.Key != backupruntime.BackupRetentionKey(sourceID, record.TriggerRecoveryPointID) {
			return BackupRuntimePage[backupruntime.BackupRetentionSweepRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		page.Items[index] = etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{
			Record: record, Revision: item.ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	if result.More {
		page.Next = result.Values[len(result.Values)-1].Key
	}
	return page, nil
}

// AdvanceBackupRetentionSweep scans one bounded newest-first source page,
// retains exactly the first Keep visible points, and atomically tombstones
// every older visible point in the page under the owning Backup lock.
func (repository *BackupRuntimeRepository) AdvanceBackupRetentionSweep(
	ctx context.Context,
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
	current etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord],
	advancedAt time.Time,
) (etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord], []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], error) {
	if current.Revision <= 0 ||
		(current.Record.State != backupruntime.BackupRetentionPending && current.Record.State != backupruntime.BackupRetentionScanning) ||
		!backupruntime.ValidBackupRuntimeInstant(advancedAt) || !advancedAt.After(current.Record.UpdatedAt) ||
		backupruntime.ValidateBackupRunRecord(run.Record) != nil || run.Revision <= 0 ||
		run.Record.State != backupruntime.BackupRunRunning ||
		!backupRetentionSweepMatchesRun(run.Record, current.Record) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindValidationFailed,
			"backup retention transition is invalid",
		)
	}
	sweepKey := backupruntime.BackupRetentionKey(current.Record.SourceID, current.Record.TriggerRecoveryPointID)
	anchor, err := repository.readCurrentKeys(
		ctx,
		[]string{backupruntime.BackupRunKey(run.Record.TaskID), sweepKey},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != run.Revision ||
		anchor.Values[1] == nil || anchor.Values[1].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindStateConflict,
			"backup retention authority changed",
		)
	}
	storedRun, runErr := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	storedSweep, sweepErr := backupruntime.DecodeBackupRetentionSweepRecord(anchor.Values[1].Value)
	if runErr != nil || sweepErr != nil || !backupRunRecordsEqual(storedRun, run.Record) ||
		storedSweep != current.Record || !backupRetentionSweepMatchesRun(storedRun, storedSweep) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	prefix := backupruntime.BackupRecoveryPointSourcePrefix + current.Record.SourceID + "/"
	startExclusive := ""
	if current.Record.Cursor != "" {
		startExclusive, err = backupruntime.BackupRecoveryPointSourceIndexKey(
			current.Record.SourceID,
			current.Record.Cursor,
		)
		if err != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
		}
	}
	selectionRevision := current.Record.SelectionRevision
	if selectionRevision == 0 {
		selectionRevision = anchor.ReadRevision
	}
	if selectionRevision <= 0 || selectionRevision > anchor.ReadRevision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: startExclusive, Limit: maximumBackupPruneBatch,
		Revision: selectionRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if index == nil || index.ReadRevision != selectionRevision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup retention point page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	authorityKeys := make([]string, 0, len(index.Values)*2)
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		expected, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(current.Record.SourceID, pointID)
		if keyErr != nil || expected != item.Key {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		pointIDs[position] = pointID
		authorityKeys = append(
			authorityKeys,
			backupruntime.BackupRecoveryPointKey(pointID),
			backupruntime.BackupRecoveryPointPruneKey(pointID),
		)
	}
	authority, err := repository.readBackupRecoveryPointPageChunks(
		ctx,
		authorityKeys,
		selectionRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if authority == nil || authority.ReadRevision != selectionRevision ||
		len(authority.Values) != len(authorityKeys) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup retention point authority is incomplete",
		)
	}
	defer clearKeyValues(authority.Values)
	points := make([]backupruntime.BackupRecoveryPointRecord, len(pointIDs))
	companionKeys := make([]string, 0, len(pointIDs)*3)
	for position, pointID := range pointIDs {
		pointValue := authority.Values[position*2]
		if pointValue == nil || pointValue.Version != 1 || index.Values[position].Version != 1 ||
			index.Values[position].ModRevision != pointValue.ModRevision {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, decodeErr := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
		if decodeErr != nil || point.ID != pointID ||
			point.EnvironmentID != run.Record.EnvironmentID ||
			point.SourceID != current.Record.SourceID {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		environmentIndex, keyErr := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if keyErr != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		sourceIndex, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if keyErr != nil || sourceIndex != index.Values[position].Key {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorIndex, keyErr := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if keyErr != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		points[position] = point
		companionKeys = append(companionKeys, environmentIndex, sourceIndex, connectorIndex)
	}
	companions, err := repository.readBackupRecoveryPointPageChunks(
		ctx,
		companionKeys,
		selectionRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	defer clearKeyValues(companions.Values)
	if companions.ReadRevision != selectionRevision || len(companions.Values) != len(companionKeys) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	for position, point := range points {
		pointRevision := authority.Values[position*2].ModRevision
		for offset := range 3 {
			companion := companions.Values[position*3+offset]
			if companion == nil || companion.Version != 1 || companion.ModRevision != pointRevision ||
				companion.Key != companionKeys[position*3+offset] || string(companion.Value) != point.ID {
				return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
		}
	}
	next := current.Record
	next.State = backupruntime.BackupRetentionScanning
	next.SelectionRevision = selectionRevision
	next.UpdatedAt = advancedAt
	if next.PruneOperationID == "" {
		next.PruneOperationID = ids.New(ids.KindOperation)
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupRunKey(run.Record.TaskID), ModRevision: run.Revision},
		{Key: sweepKey, ModRevision: current.Revision},
	}
	created := make([]backupruntime.BackupRecoveryPointPruneRecord, 0, len(index.Values))
	mutations := make([]etcdstore.Mutation, 0, len(index.Values)+2)
	for position, pointID := range pointIDs {
		pointValue := authority.Values[position*2]
		pruneValue := authority.Values[position*2+1]
		if pointValue == nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point := points[position]
		conditions = append(
			conditions,
			etcdstore.Condition{Key: companionKeys[position*3], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: companionKeys[position*3+1], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: companionKeys[position*3+2], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: backupruntime.BackupRecoveryPointKey(pointID), ModRevision: pointValue.ModRevision},
		)
		if pruneValue != nil {
			prune, pruneErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			if pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
				prune.State == backupruntime.BackupPruneVerifiedAbsent {
				return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: backupruntime.BackupRecoveryPointPruneKey(pointID), ModRevision: pruneValue.ModRevision,
			})
			next.Cursor = pointID
			continue
		}
		conditions = append(conditions, etcdstore.Condition{Key: backupruntime.BackupRecoveryPointPruneKey(pointID)})
		if next.RetainedCount < next.Keep {
			next.RetainedCount++
			next.Cursor = pointID
			continue
		}
		prune := backupruntime.BackupRecoveryPointPruneRecord{
			Point: point.BackupRecoveryPointSnapshot, PointRevision: pointValue.ModRevision,
			OperationID: next.PruneOperationID,
			State:       backupruntime.BackupPrunePending, CreatedAt: advancedAt, UpdatedAt: advancedAt,
		}
		encoded, encodeErr := backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: backupruntime.BackupRecoveryPointPruneKey(pointID), Value: encoded,
		})
		created = append(created, prune)
		next.Cursor = pointID
	}
	if !index.More {
		next.State = backupruntime.BackupRetentionCompleted
	}
	nextValue, err := backupruntime.EncodeBackupRetentionSweepRecord(next)
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	mutations = append(
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: sweepKey, Value: nextValue}},
		mutations...)
	evidence, err := repository.loadOwnedEvidence(ctx, run.Record, anchor.ReadRevision)
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	conditions = append(conditions, evidence.fence.transactionConditions()...)
	epoch, err := evidence.fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	mutations = append(mutations, epoch)
	result, err := repository.transact(ctx, conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindStateConflict,
			"backup retention authority changed",
		)
	}
	prunes := make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], len(created))
	for index, prune := range created {
		prunes[index] = etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
			Record: prune, Revision: result.Revision, ReadRevision: result.Revision,
		}
	}
	return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, prunes, nil
}

func backupRetentionSweepMatchesRun(
	run backupruntime.BackupRunRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) bool {
	if sweep.Revision != run.PolicyRevision || sweep.Keep != run.RetentionKeep {
		return false
	}
	for index := range run.Sources {
		source := run.Sources[index]
		if source.SourceID == sweep.SourceID {
			return source.RecoveryPointID == sweep.TriggerRecoveryPointID
		}
	}
	return false
}

// prepareBackupPrunePublication composes retained prune authority and the
// exact Environment lock with the future Task publication transaction.
func (repository *BackupRuntimeRepository) prepareBackupPrunePublication(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	lock backupruntime.BackupOperationLockRecord,
) (backupPruneTransactionPlan, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	if backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch) != nil ||
		len(pending) == 0 || len(pending) > maximumBackupPruneBatch ||
		len(pending) != len(dispatch.RecoveryPointIDs) ||
		lock.EnvironmentID != dispatch.EnvironmentID || lock.OperationID != dispatch.OperationID ||
		lock.TaskID != dispatch.TaskID || lock.Kind != backupruntime.BackupOperationPrune ||
		lock.CreatedAt != dispatch.CreatedAt || lock.UpdatedAt != dispatch.CreatedAt {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune publication is invalid",
		)
	}
	dispatchValue, err := backupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(dispatchValue)
		return backupPruneTransactionPlan{}, err
	}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID)
	keys := []string{dispatchKey}
	assignedValues := make([][]byte, len(pending))
	defer func() {
		for index := range assignedValues {
			clear(assignedValues[index])
		}
	}()
	for index := range pending {
		version := pending[index]
		record := version.Record
		if version.Revision <= 0 || record.State != backupruntime.BackupPrunePending || record.TaskID != "" ||
			record.OperationID != dispatch.OperationID ||
			record.Point.EnvironmentID != dispatch.EnvironmentID ||
			record.Point.ID != dispatch.RecoveryPointIDs[index] ||
			dispatch.CreatedAt.Before(record.UpdatedAt) {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune publication authority is invalid",
			)
		}
		authorityKeys, keyErr := backupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, keyErr
		}
		keys = append(keys, authorityKeys...)
		assigned := record
		assigned.State = backupruntime.BackupPruneAssigned
		assigned.TaskID = dispatch.TaskID
		assigned.UpdatedAt = dispatch.CreatedAt
		assignedValues[index], err = backupruntime.EncodeBackupRecoveryPointPruneRecord(assigned)
		if err != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, err
		}
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindStateConflict,
			"backup prune dispatch already exists",
		)
	}
	for index := range pending {
		start := 1 + index*5
		if err := validatePendingBackupPruneAuthority(
			anchor.Values[start:start+5],
			pending[index],
		); err != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, err
		}
	}
	planEvidence, err := repository.loadBackupPruneExecutionEvidence(
		ctx,
		pending,
		anchor.Values,
		anchor.ReadRevision,
	)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.EnvironmentID,
		anchor.ReadRevision,
	)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	conditions := []etcdstore.Condition{{Key: dispatchKey}}
	mutations := make([]etcdstore.Mutation, 0, len(pending)+3)
	for index := range pending {
		start := 1 + index*5
		for offset := range 5 {
			conditions = append(conditions, etcdstore.Condition{
				Key: keys[start+offset], ModRevision: anchor.Values[start+offset].ModRevision,
			})
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   keys[start],
			Value: append([]byte(nil), assignedValues[index]...),
		})
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: dispatchKey, Value: dispatchValue},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOperationLockKey(dispatch.EnvironmentID),
			Value: lockValue,
		},
	)
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{
		conditions: conditions,
		mutations:  mutations,
		authority: &backupTaskPublicationAuthority{
			taskID: dispatch.TaskID, operationID: dispatch.OperationID,
			environmentID: dispatch.EnvironmentID, taskType: TaskBackupPrune,
			createdAt: dispatch.CreatedAt,
			validatePlan: func(value *agentpb.ExecutionPlan) error {
				return validateBackupPruneExecutionPlan(dispatch, planEvidence, value)
			},
		},
		readRevision: anchor.ReadRevision,
	}, nil
}

func (repository *BackupRuntimeRepository) loadBackupPruneExecutionEvidence(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	authorityValues []*etcdstore.KeyValue,
	readRevision int64,
) ([]backupPruneExecutionEvidence, error) {
	evidence := make([]backupPruneExecutionEvidence, len(pending))
	for index, version := range pending {
		start := 1 + index*5
		if start+4 >= len(authorityValues) || authorityValues[start+1] == nil {
			return nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, err := backupruntime.DecodeBackupRecoveryPointRecord(authorityValues[start+1].Value)
		if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point {
			return nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		fixed, err := repository.readFixedKeys(ctx, []string{
			backuppolicy.BackupSourceKey(point.SourceID),
			hierarchyrecord.EnvironmentKey(point.EnvironmentID),
			connectorrecord.RecordKey(point.ConnectorID),
		}, readRevision)
		if err != nil {
			return nil, err
		}
		if len(fixed.Values) != 3 || fixed.Values[0] == nil || fixed.Values[1] == nil || fixed.Values[2] == nil {
			clearKeyValues(fixed.Values)
			return nil, errs.New(errs.KindStateConflict, "backup prune plan evidence is missing")
		}
		source, sourceErr := backuppolicy.DecodeBackupSourceRecord(fixed.Values[0].Value)
		environment, environmentErr := hierarchyrecord.DecodeEnvironment(fixed.Values[1].Value)
		connector, connectorErr := connectorrecord.DecodeRecord(fixed.Values[2].Value)
		if sourceErr != nil || environmentErr != nil || connectorErr != nil ||
			source.ID != point.SourceID || source.EnvironmentID != point.EnvironmentID ||
			environment.ID != point.EnvironmentID || connector.Connector.ID != point.ConnectorID ||
			connector.Connector.EnvironmentID != point.EnvironmentID {
			clearKeyValues(fixed.Values)
			return nil, errs.New(errs.KindStateConflict, "backup prune plan evidence changed")
		}
		evidence[index] = backupPruneExecutionEvidence{
			prune: version.Record, pruneRevision: version.Revision,
			pointRevision:       authorityValues[start+1].ModRevision,
			sourceRevision:      fixed.Values[0].ModRevision,
			environmentRevision: fixed.Values[1].ModRevision,
			connectorRevision:   fixed.Values[2].ModRevision,
			connectorEndpoint:   connector.Connector.Endpoint,
			connectorBucket:     connector.Connector.Bucket,
			connectorPrefix:     connector.Connector.Prefix,
			connectorRegion:     connector.Connector.Region,
			connectorPathStyle:  connector.Connector.PathStyle,
		}
		clearKeyValues(fixed.Values)
	}
	return evidence, nil
}

func (repository *BackupRuntimeRepository) MarkBackupRecoveryPointPruneVerifiedAbsent(
	ctx context.Context,
	checkpointInput BackupCheckpointInput,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	current etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	next backupruntime.BackupRecoveryPointPruneRecord,
	verifiedRemoteAbsent bool,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], error) {
	if !verifiedRemoteAbsent || dispatch.Revision <= 0 || current.Revision <= 0 ||
		backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		current.Record.State != backupruntime.BackupPruneAssigned || next.State != backupruntime.BackupPruneVerifiedAbsent ||
		current.Record.Point != next.Point || current.Record.OperationID != next.OperationID ||
		current.Record.TaskID != next.TaskID || current.Record.CreatedAt != next.CreatedAt ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) ||
		next.OperationID != dispatch.Record.OperationID || next.TaskID != dispatch.Record.TaskID ||
		next.Point.EnvironmentID != dispatch.Record.EnvironmentID ||
		!backupPruneDispatchContains(dispatch.Record, next.Point.ID) {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune absence checkpoint is invalid",
		)
	}
	if checkpointInput.TaskID != dispatch.Record.TaskID ||
		checkpointInput.Payload.Kind != BackupCheckpointRemoteObjectAbsent ||
		checkpointInput.Payload.PointID != next.Point.ID {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune checkpoint is invalid",
		)
	}
	checkpointOrdinal, found := backupPruneDispatchPointOrdinal(
		dispatch.Record,
		next.Point.ID,
	)
	if !found {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune checkpoint point order is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clear(value)
	authorityKeys, err := backupPruneAuthorityKeys(current.Record.Point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	keys := append(
		[]string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)},
		authorityKeys...)
	keys = append(keys, backupruntime.BackupRetentionKey(current.Record.Point.SourceID, current.Record.Point.ID))
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	checkpointPlan, err := repository.loadBackupCheckpointPlan(
		ctx,
		checkpointInput,
		anchor.ReadRevision,
		backupCheckpointBinding{
			taskType: TaskBackupPrune, ordinal: checkpointOrdinal, pointID: next.Point.ID,
		},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer checkpointPlan.clear()
	if anchor.Values[1] != nil {
		stored, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(anchor.Values[1].Value)
		if decodeErr == nil && stored == next && anchor.Values[1].ModRevision > current.Revision &&
			checkpointPlan.duplicate &&
			anchor.Values[1].ModRevision == checkpointPlan.commitRevision &&
			allBackupRuntimeValuesAbsent(anchor.Values[2:]) {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record:       stored,
				Revision:     anchor.Values[1].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if checkpointPlan.duplicate {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup prune checkpoint domain state is incomplete",
		)
	}
	if err := validatePendingBackupPruneAuthority(anchor.Values[1:6], current); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	if err := validateCompletedBackupRetentionSweep(
		anchor.Values[6],
		current.Record.Point,
		anchor.Values[2].ModRevision,
	); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: dispatch.Revision}}
	for index := 1; index < len(keys); index++ {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: keys[index], ModRevision: anchor.Values[index].ModRevision},
		)
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[1], Value: value}}
	for _, key := range keys[2:] {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clear(epoch.Value)
	mutations = append(mutations, epoch)
	conditions = append(conditions, checkpointPlan.conditions...)
	for _, mutation := range checkpointPlan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup prune authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// Rationale: a non-success prune Task may not leave Task-owned domain state.
// Surviving objects return to stable operation-owned pending tombstones while
// already verified-absent authorities are removed before the lock is released.
func (repository *BackupRuntimeRepository) prepareBackupPruneFailure(
	ctx context.Context,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	terminalAt time.Time,
) (backupPruneTransactionPlan, error) {
	if dispatch.Revision <= 0 || backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		len(prunes) != len(dispatch.Record.RecoveryPointIDs) ||
		!backupruntime.ValidBackupRuntimeInstant(terminalAt) || !terminalAt.After(dispatch.Record.CreatedAt) {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune failure is invalid",
		)
	}
	keys := []string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)}
	for index, prune := range prunes {
		if prune.Revision <= 0 ||
			(prune.Record.State != backupruntime.BackupPruneAssigned && prune.Record.State != backupruntime.BackupPruneVerifiedAbsent) ||
			prune.Record.OperationID != dispatch.Record.OperationID ||
			prune.Record.TaskID != dispatch.Record.TaskID ||
			prune.Record.Point.ID != dispatch.Record.RecoveryPointIDs[index] ||
			prune.Record.Point.EnvironmentID != dispatch.Record.EnvironmentID ||
			!terminalAt.After(prune.Record.UpdatedAt) {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune failure authority is invalid",
			)
		}
		authorityKeys, err := backupPruneAuthorityKeys(prune.Record.Point)
		if err != nil {
			return backupPruneTransactionPlan{}, err
		}
		keys = append(keys, authorityKeys...)
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	for index, prune := range prunes {
		start := 1 + index*5
		if prune.Record.State == backupruntime.BackupPruneAssigned {
			if err := validatePendingBackupPruneAuthority(anchor.Values[start:start+5], prune); err != nil {
				return backupPruneTransactionPlan{}, err
			}
			continue
		}
		if anchor.Values[start] == nil || anchor.Values[start].ModRevision != prune.Revision {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindStateConflict,
				"backup prune verified-absent authority changed",
			)
		}
		if !allBackupRuntimeValuesAbsent(anchor.Values[start+1 : start+5]) {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		stored, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(anchor.Values[start].Value)
		if decodeErr != nil || stored != prune.Record {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	conditions := make([]etcdstore.Condition, 0, len(keys)+len(fence.conditions))
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if anchor.Values[index] != nil {
			condition.ModRevision = anchor.Values[index].ModRevision
		}
		conditions = append(conditions, condition)
	}
	mutations := make([]etcdstore.Mutation, 0, len(prunes)+3)
	for index, prune := range prunes {
		key := keys[1+index*5]
		if prune.Record.State == backupruntime.BackupPruneVerifiedAbsent {
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		pending := prune.Record
		pending.State = backupruntime.BackupPrunePending
		pending.TaskID = ""
		pending.UpdatedAt = terminalAt
		value, encodeErr := backupruntime.EncodeBackupRecoveryPointPruneRecord(pending)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			return backupPruneTransactionPlan{}, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[0]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(dispatch.Record.EnvironmentID)},
	)
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{conditions: conditions, mutations: mutations}, nil
}

// prepareBackupPruneCompletion leaves room for the future Task terminal
// conditions and mutations while guaranteeing lock release in that same txn.
func (repository *BackupRuntimeRepository) prepareBackupPruneCompletion(
	ctx context.Context,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) (backupPruneTransactionPlan, error) {
	if dispatch.Revision <= 0 ||
		backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		len(prunes) != len(dispatch.Record.RecoveryPointIDs) {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune completion is invalid",
		)
	}
	keys := []string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)}
	for index, prune := range prunes {
		if prune.Revision <= 0 || prune.Record.State != backupruntime.BackupPruneVerifiedAbsent ||
			prune.Record.OperationID != dispatch.Record.OperationID ||
			prune.Record.TaskID != dispatch.Record.TaskID ||
			prune.Record.Point.ID != dispatch.Record.RecoveryPointIDs[index] ||
			prune.Record.Point.EnvironmentID != dispatch.Record.EnvironmentID {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune completion authority is invalid",
			)
		}
		authorityKeys, err := backupPruneAuthorityKeys(prune.Record.Point)
		if err != nil {
			return backupPruneTransactionPlan{}, err
		}
		keys = append(keys, authorityKeys...)
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	for index, prune := range prunes {
		start := 1 + index*5
		if anchor.Values[start] == nil || anchor.Values[start].ModRevision != prune.Revision {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindStateConflict,
				"backup prune completion checkpoint changed",
			)
		}
		if !allBackupRuntimeValuesAbsent(anchor.Values[start+1 : start+5]) {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		stored, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(anchor.Values[start].Value)
		if decodeErr != nil || stored != prune.Record {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: dispatch.Revision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: keys[0]}}
	for index, prune := range prunes {
		start := 1 + index*5
		conditions = append(conditions, etcdstore.Condition{Key: keys[start], ModRevision: prune.Revision})
		for offset := 1; offset < 5; offset++ {
			conditions = append(conditions, etcdstore.Condition{Key: keys[start+offset]})
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[start]})
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(dispatch.Record.EnvironmentID),
	})
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{conditions: conditions, mutations: mutations}, nil
}

func backupPruneAuthorityKeys(point backupruntime.BackupRecoveryPointSnapshot) ([]string, error) {
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return nil, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return nil, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return nil, err
	}
	return []string{
		backupruntime.BackupRecoveryPointPruneKey(point.ID),
		backupruntime.BackupRecoveryPointKey(point.ID),
		environmentIndex,
		sourceIndex,
		connectorIndex,
	}, nil
}

func validatePendingBackupPruneAuthority(
	values []*etcdstore.KeyValue,
	version etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) error {
	if len(values) != 5 {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if values[0] == nil || values[0].ModRevision != version.Revision {
		return errs.New(errs.KindStateConflict, "backup prune authority changed")
	}
	storedPrune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(values[0].Value)
	if err != nil || storedPrune != version.Record {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if values[1] == nil || values[2] == nil || values[3] == nil || values[4] == nil ||
		values[1].Version != 1 || values[2].Version != 1 || values[3].Version != 1 ||
		values[4].Version != 1 || values[1].ModRevision != version.Record.PointRevision ||
		values[2].ModRevision != version.Record.PointRevision ||
		values[1].ModRevision != values[3].ModRevision ||
		values[1].ModRevision != values[4].ModRevision ||
		string(values[2].Value) != version.Record.Point.ID ||
		string(values[3].Value) != version.Record.Point.ID ||
		string(values[4].Value) != version.Record.Point.ID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(values[1].Value)
	if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil || values[1].Key != backupruntime.BackupRecoveryPointKey(point.ID) ||
		values[2].Key != environmentIndex || values[3].Key != sourceIndex ||
		values[4].Key != connectorIndex {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateCompletedBackupRetentionSweep(
	value *etcdstore.KeyValue,
	point backupruntime.BackupRecoveryPointSnapshot,
	pointRevision int64,
) error {
	if value == nil || value.Version < 2 || value.ModRevision <= pointRevision {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	sweep, err := backupruntime.DecodeBackupRetentionSweepRecord(value.Value)
	if err != nil || sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.State != backupruntime.BackupRetentionCompleted {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateExactBackupPruneDispatchValue(
	value *etcdstore.KeyValue,
	expected etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
) error {
	if value == nil || value.ModRevision != expected.Revision {
		return errs.New(errs.KindStateConflict, "backup prune dispatch changed")
	}
	stored, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(value.Value)
	if err != nil || !backupPruneDispatchRecordsEqual(stored, expected.Record) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func backupPruneDispatchRecordsEqual(
	left backupruntime.BackupRecoveryPointPruneDispatchRecord,
	right backupruntime.BackupRecoveryPointPruneDispatchRecord,
) bool {
	if left.TaskID != right.TaskID || left.OperationID != right.OperationID ||
		left.EnvironmentID != right.EnvironmentID ||
		left.CreatedAt != right.CreatedAt || len(left.RecoveryPointIDs) != len(right.RecoveryPointIDs) {
		return false
	}
	for index := range left.RecoveryPointIDs {
		if left.RecoveryPointIDs[index] != right.RecoveryPointIDs[index] {
			return false
		}
	}
	return true
}

func backupPruneDispatchContains(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	recoveryPointID string,
) bool {
	for _, candidate := range dispatch.RecoveryPointIDs {
		if candidate == recoveryPointID {
			return true
		}
	}
	return false
}

func backupPruneDispatchPointOrdinal(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	pointID string,
) (uint32, bool) {
	for index, candidate := range dispatch.RecoveryPointIDs {
		if candidate == pointID {
			return uint32(index), true
		}
	}
	return 0, false
}

func allBackupRuntimeValuesAbsent(values []*etcdstore.KeyValue) bool {
	for _, value := range values {
		if value != nil {
			return false
		}
	}
	return true
}

func getOptionalBackupRuntimeRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	key string,
	stableID string,
	decode func([]byte) (T, error),
	id func(T) string,
) (etcdstore.Versioned[T], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	result, err := store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[T]{}, false, errs.New(
			errs.KindInternal,
			"backup runtime record read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[T]{ReadRevision: result.ReadRevision}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := decode(result.Entry.Value)
	if err != nil || id(record) != stableID {
		return etcdstore.Versioned[T]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func validateBackupRuntimeListRequest(prefix string, request BackupRuntimeListRequest) error {
	if request.Limit <= 0 || request.Limit > maximumBackupRuntimeListLimit ||
		request.Revision < 0 ||
		(request.StartExclusive != "" &&
			(request.Revision <= 0 || !validBackupRuntimeListCursor(prefix, request.StartExclusive))) {
		return errs.New(errs.KindValidationFailed, "backup runtime list request is invalid")
	}
	return nil
}

func validBackupRuntimeListCursor(prefix string, cursor string) bool {
	if !strings.HasPrefix(cursor, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(cursor, prefix)
	if suffix == "" || strings.Contains(suffix, "/") {
		return false
	}
	switch {
	case strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointSourcePrefix):
		_, ok := backupruntime.InvertBackupRecoveryPointULIDBody(suffix)
		return ok
	case strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointConnectorPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupOrphanEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRetentionPrefix):
		return recordcodec.ValidateID(ids.KindRecoveryPoint, suffix) == nil
	case strings.HasPrefix(prefix, backupruntime.BackupRunEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRestoreEnvironmentPrefix):
		return recordcodec.ValidateID(ids.KindTask, suffix) == nil
	default:
		return true
	}
}

func backupPointMatchesRunSource(
	point backupruntime.BackupRecoveryPointSnapshot,
	run backupruntime.BackupRunRecord,
	ordinal uint32,
) bool {
	if int(ordinal) >= len(run.Sources) || backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil {
		return false
	}
	source := run.Sources[ordinal]
	return point.ID == source.RecoveryPointID && point.EnvironmentID == run.EnvironmentID &&
		point.CreatedAt.Equal(source.RecoveryPointCreatedAt) &&
		point.SourceID == source.SourceID && point.SourceKind == source.Kind &&
		point.TargetID == source.TargetID && point.ConnectorID == run.ConnectorID &&
		point.ConnectorPrefix == run.ConnectorPrefix && point.ObjectKey == source.ObjectKey &&
		point.SourceFormat == source.Format &&
		point.Encryption == run.Encryption && point.KeyEra == run.KeyEra && point.Recipient == run.Recipient &&
		point.SizeBytes == source.SizeBytes && point.SHA256 == source.SHA256
}

func runContainsOrphanedPoint(run backupruntime.BackupRunRecord, orphan backupruntime.BackupOrphanRecord) bool {
	for ordinal, source := range run.Sources {
		if source.State == backupruntime.BackupSourceAttemptOrphaned &&
			backupOrphanMatchesRunSource(orphan, run, uint32(ordinal)) {
			return true
		}
	}
	return false
}

func backupOrphanMatchesRunSource(
	orphan backupruntime.BackupOrphanRecord,
	run backupruntime.BackupRunRecord,
	ordinal uint32,
) bool {
	return orphan.TaskID == run.TaskID &&
		orphan.Reconciliation == (backupruntime.BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			RetentionKeep:  run.RetentionKeep,
		}) &&
		backupPointMatchesRunSource(orphan.Point, run, ordinal)
}

func validateBackupOrphanCompanionEvidence(values []*etcdstore.KeyValue, expected backupruntime.BackupOrphanRecord) error {
	expectedVersion := int64(1)
	if expected.State == backupruntime.BackupOrphanDelete {
		expectedVersion = 2
	}
	if len(values) != 3 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].Version != expectedVersion || values[1].Version != expectedVersion ||
		values[2].Version != expectedVersion ||
		values[0].ModRevision != values[1].ModRevision ||
		values[0].ModRevision != values[2].ModRevision ||
		string(
			values[1].Value,
		) != expected.Point.ID || string(values[2].Value) != expected.Point.ID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	stored, err := backupruntime.DecodeBackupOrphanRecord(values[0].Value)
	if err != nil || stored != expected {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateBackupConnectorSnapshotEvidence(values []*etcdstore.KeyValue, run backupruntime.BackupRunRecord) error {
	if len(values) != 2 || values[0] == nil || values[0].ModRevision != run.ConnectorRevision {
		return errs.New(errs.KindStateConflict, "backup connector snapshot changed")
	}
	connector, err := connectorrecord.DecodeRecord(values[0].Value)
	if err != nil || connector.Connector.ID != run.ConnectorID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if connector.Connector.EnvironmentID != run.EnvironmentID {
		return errs.New(errs.KindStateConflict, "backup connector belongs to another environment")
	}
	if connector.Connector.Prefix != run.ConnectorPrefix {
		return errs.New(errs.KindStateConflict, "backup connector prefix changed")
	}
	hasDirectCredentials := connectorrecord.HasDirectCredentials(connector)
	if hasDirectCredentials != run.ConnectorHasDirectCredentials {
		return errs.New(errs.KindStateConflict, "backup connector credential mode changed")
	}
	if !run.ConnectorHasDirectCredentials {
		if run.ConnectorCredentialsRevision != 0 || values[1] != nil {
			return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
		}
		return nil
	}
	if values[1] == nil || values[1].ModRevision != run.ConnectorCredentialsRevision {
		return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(values[1].Value)
	defer clear(credentials.Ciphertext)
	if err != nil || credentials.ConnectorID != run.ConnectorID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func changedBackupSourceOrdinal(current backupruntime.BackupRunRecord, next backupruntime.BackupRunRecord) (uint32, bool) {
	if len(current.Sources) != len(next.Sources) {
		return 0, false
	}
	changed := -1
	for index := range current.Sources {
		if backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
			continue
		}
		if changed >= 0 {
			if terminalBackupRunState(next.State) && index > changed &&
				current.Sources[index].State == backupruntime.BackupSourceAttemptPending &&
				next.Sources[index].State == backupruntime.BackupSourceAttemptUnstarted &&
				current.Sources[index].SizeBytes == next.Sources[index].SizeBytes &&
				current.Sources[index].SHA256 == next.Sources[index].SHA256 &&
				current.Sources[index].FailureCode == "" && next.Sources[index].FailureCode == "" {
				continue
			}
			return 0, false
		}
		changed = index
	}
	if changed < 0 {
		return 0, false
	}
	return uint32(changed), true
}

func clearRangeValues(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}
