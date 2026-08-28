package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumBackupRuntimeTransactionBytes = 768 << 10

type BackupRuntimeRepository struct {
	store hierarchyStore
}

type backupRuntimeOwnedEvidence struct {
	fence environmentMutationFenceEvidence
}

// backupRunPublicationPlan is the persistence-private seam used by future Task
// publication. Its lock, run, membership, exclusions, and epoch mutations are
// appended to the caller's Task mutations and committed once.
type backupRunPublicationPlan struct {
	conditions []Condition
	mutations  []Mutation
	record     BackupRunRecord
	replay     func(context.Context, IdempotencyMarker, int64, int64) error
}

func (plan backupRunPublicationPlan) composeTransaction(
	conditions []Condition,
	mutations []Mutation,
) ([]Condition, []Mutation, error) {
	composedConditions := append(append([]Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]Mutation, 0, len(mutations)+len(plan.mutations))
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
	marker IdempotencyMarker,
	initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	idempotencyPlan, err := prepareBackupTaskIdempotencyPlan(
		backupTaskPublicationAuthority{
			taskID: plan.record.TaskID, operationID: plan.record.OperationID,
			environmentID: plan.record.EnvironmentID, taskType: TaskBackup,
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
	taskType      TaskType
	retryOf       string
	createdAt     time.Time
	validatePlan  backupTaskPlanValidator
}

func prepareBackupTaskIdempotencyPlan(
	authority backupTaskPublicationAuthority,
	domainConditions []Condition,
	domainMutations []Mutation,
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker IdempotencyMarker,
	initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	if record.ID != authority.taskID || record.OperationID != authority.operationID ||
		record.RetryOf != authority.retryOf ||
		record.Owner.EnvironmentID != authority.environmentID ||
		record.Type != authority.taskType || record.Target != authority.environmentID ||
		record.Executor != TaskExecutorAgent || record.Status != TaskStatusPending ||
		!record.CreatedAt.Equal(authority.createdAt) || marker.Kind != IdempotencyMarkerTask ||
		marker.State != IdempotencyMarkerPending || marker.TaskID != record.ID ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
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
	if validateTaskRecord(record) != nil || validateIdempotencyMarker(marker) != nil ||
		validateTaskInitiation(record, initiation, true) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backup Task publication is invalid")
	}
	taskValue, err := encodeTaskRecord(record)
	if err != nil {
		return nil, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(record.ID)
	if err != nil {
		return nil, err
	}
	defer clear(reference)
	taskConditions := []Condition{
		{Key: taskKey(record.ID)},
		{Key: taskOperationIndexKey(record.OperationID, record.ID)},
		{Key: taskActiveOperationKey(record.OperationID)},
		{Key: taskQueueKey(record.Executor, record.ID)},
	}
	taskMutations := []Mutation{
		{Type: MutationPut, Key: taskKey(record.ID), Value: taskValue},
		{
			Type:  MutationPut,
			Key:   taskOperationIndexKey(record.OperationID, record.ID),
			Value: reference,
		},
		{Type: MutationPut, Key: taskActiveOperationKey(record.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(record.Executor, record.ID), Value: reference},
	}
	domain := backupRunPublicationPlan{conditions: domainConditions, mutations: domainMutations}
	conditions, mutations, err := domain.composeTransaction(taskConditions, taskMutations)
	if err != nil {
		return nil, err
	}
	defer clearBackupRuntimeMutations(mutations)
	taskClassifier := classifyTaskCreateConflict(record.OperationID)
	classify := func(revision int64, values []*KeyValue) error {
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
		validateBackupRuntimeTransactionBounds,
	); err != nil {
		return nil, err
	}
	return idempotencyPlan, nil
}

func NewBackupRuntimeRepository(store Store) (*BackupRuntimeRepository, error) {
	return newBackupRuntimeRepository(store)
}

func newBackupRuntimeRepository(store hierarchyStore) (*BackupRuntimeRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup runtime store is required")
	}
	return &BackupRuntimeRepository{store: store}, nil
}

func (repository *BackupRuntimeRepository) GetBackupRun(
	ctx context.Context,
	taskID string,
) (Versioned[BackupRunRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if err := validateID(ids.KindTask, taskID); err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	primaryKey := backupRunKey(taskID)
	result, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if result == nil {
		return Versioned[BackupRunRecord]{}, errs.New(errs.KindInternal, "backup run read is empty")
	}
	if result.Entry == nil {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindTaskNotFound,
			"backup run was not found",
		)
	}
	defer clear(result.Entry.Value)
	record, err := decodeBackupRunRecord(result.Entry.Value)
	if err != nil || record.TaskID != taskID {
		return Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	membershipKey, err := backupRunEnvironmentIndexKey(record.EnvironmentID, taskID)
	if err != nil {
		return Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{primaryKey, membershipKey}, Revision: result.ReadRevision,
	})
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if authority == nil || authority.ReadRevision != result.ReadRevision ||
		len(authority.Values) != 2 || authority.Values[0] == nil || authority.Values[1] == nil {
		return Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	defer clearKeyValues(authority.Values)
	stored, err := decodeBackupRunRecord(authority.Values[0].Value)
	if err != nil || !backupRunRecordsEqual(stored, record) ||
		authority.Values[0].ModRevision != result.Entry.ModRevision ||
		authority.Values[1].Key != membershipKey || authority.Values[1].Version != 1 ||
		authority.Values[1].ModRevision > authority.Values[0].ModRevision ||
		string(authority.Values[1].Value) != taskID {
		return Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	return Versioned[BackupRunRecord]{
		Record:       stored,
		Revision:     authority.Values[0].ModRevision,
		ReadRevision: authority.ReadRevision,
	}, nil
}

func (repository *BackupRuntimeRepository) prepareBackupRunPublication(
	ctx context.Context,
	record BackupRunRecord,
	lock BackupOperationLockRecord,
	fixedRevision int64,
) (backupRunPublicationPlan, error) {
	return repository.prepareBackupRunPublicationWithRetry(
		ctx, record, lock, nil, fixedRevision,
	)
}

func (repository *BackupRuntimeRepository) prepareBackupRunPublicationWithRetry(
	ctx context.Context,
	record BackupRunRecord,
	lock BackupOperationLockRecord,
	retrySource *backupRunRetrySource,
	fixedRevision int64,
) (backupRunPublicationPlan, error) {
	if err := validateContext(ctx); err != nil {
		return backupRunPublicationPlan{}, err
	}
	if record.State != BackupRunQueued || fixedRevision <= 0 ||
		(record.RetryOfTaskID == "") != (retrySource == nil) ||
		lock.EnvironmentID != record.EnvironmentID ||
		lock.OperationID != record.OperationID || lock.TaskID != record.TaskID ||
		lock.Kind != BackupOperationBackup || lock.CreatedAt != record.CreatedAt ||
		lock.UpdatedAt != record.CreatedAt {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup run publication ownership is invalid",
		)
	}
	runValue, err := encodeBackupRunRecord(record)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	lockValue, err := encodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(runValue)
		return backupRunPublicationPlan{}, err
	}
	membershipKey, err := backupRunEnvironmentIndexKey(record.EnvironmentID, record.TaskID)
	if err != nil {
		clear(runValue)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	exclusions, err := backupRunExclusionRecords(record, record.CreatedAt)
	if err != nil {
		clear(runValue)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	keys := []string{backupRunKey(record.TaskID), membershipKey}
	mutations := []Mutation{
		{Type: MutationPut, Key: keys[0], Value: runValue},
		{Type: MutationPut, Key: keys[1], Value: []byte(record.TaskID)},
	}
	for _, exclusion := range exclusions {
		key, keyErr := backupSourceTargetExclusionKey(exclusion.TargetKind, exclusion.TargetID)
		if keyErr != nil {
			clearBackupRuntimeMutations(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, keyErr
		}
		value, encodeErr := encodeBackupSourceTargetExclusionRecord(exclusion)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, encodeErr
		}
		keys = append(keys, key)
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
	}
	anchor, err := repository.readFixedKeys(ctx, keys, fixedRevision)
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	connectorEvidence, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			connectorRecordKey(record.ConnectorID), connectorCredentialValueKey(record.ConnectorID),
		},
		Revision: fixedRevision,
	})
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	if connectorEvidence == nil || connectorEvidence.ReadRevision != anchor.ReadRevision ||
		len(connectorEvidence.Values) != 2 {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, errs.New(
			errs.KindInternal,
			"backup connector publication evidence is incomplete",
		)
	}
	defer clearKeyValues(connectorEvidence.Values)
	if err := validateBackupConnectorSnapshotEvidence(
		connectorEvidence.Values,
		record,
	); err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	snapshotConditions, snapshotMutations, err := repository.loadBackupRunPublicationEvidence(
		ctx,
		record,
		retrySource,
		fixedRevision,
	)
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	for index, value := range anchor.Values {
		if index == 1 {
			continue
		}
		if value != nil {
			clearBackupRuntimeMutations(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"backup run publication authority already exists",
			)
		}
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		record.EnvironmentID,
		anchor.ReadRevision,
	)
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	policyFence := []Condition(nil)
	if retrySource == nil {
		policyFence, err = repository.loadManualBackupPolicyFence(ctx, record, fixedRevision)
		if err != nil {
			clearBackupRuntimeMutations(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, err
		}
	}
	conditions := make([]Condition, 0, len(keys)+len(fence.conditions)+len(policyFence))
	for index, key := range keys {
		if index == 1 {
			continue
		}
		conditions = append(conditions, Condition{Key: key})
	}
	conditions = append(conditions, backupRunExternalConditions(record, snapshotConditions)...)
	conditions = append(conditions, fence.transactionConditions()...)
	conditions = append(conditions, policyFence...)
	if retrySource != nil {
		conditions = append(conditions,
			Condition{Key: taskKey(retrySource.task.Record.ID), ModRevision: retrySource.task.Revision},
			Condition{Key: backupRunKey(retrySource.run.Record.TaskID), ModRevision: retrySource.run.Revision},
			Condition{Key: backupTerminalReceiptKey(retrySource.task.Record.ID), ModRevision: retrySource.receiptRevision},
		)
	}
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: environmentOperationLockKey(record.EnvironmentID), Value: lockValue,
	})
	mutations = append(mutations, snapshotMutations...)
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupRunPublicationPlan{}, err
	}
	mutations = append(mutations, epoch)
	if retrySource == nil && record.Initiator == BackupRunInitiatorSchedule {
		scheduleConditions, scheduleMutations, scheduleErr := repository.prepareScheduledBackupPublication(
			ctx, record, fixedRevision,
		)
		if scheduleErr != nil {
			clearBackupRuntimeMutations(mutations)
			return backupRunPublicationPlan{}, scheduleErr
		}
		conditions = append(conditions, scheduleConditions...)
		mutations = append(mutations, scheduleMutations...)
	}
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupRunPublicationPlan{}, err
	}
	return backupRunPublicationPlan{
		conditions: conditions,
		mutations:  mutations,
		record:     record,
		replay:     repository.validateExistingBackupRunPublication,
	}, nil
}

// backupRunExternalConditions retains CAS for each source definition and for
// backing Services outside the consumer Environment. Source definitions may
// change independently of an Environment operation; the exact Environment
// epoch and operation lock serialize the remaining consumer-owned evidence.
func backupRunExternalConditions(
	run BackupRunRecord,
	conditions []Condition,
) []Condition {
	allowed := make(map[string]struct{}, len(run.Sources)*2+2)
	allowed[connectorRecordKey(run.ConnectorID)] = struct{}{}
	if run.ConnectorHasDirectCredentials {
		allowed[connectorCredentialValueKey(run.ConnectorID)] = struct{}{}
	}
	for _, source := range run.Sources {
		allowed[backupSourceKey(source.SourceID)] = struct{}{}
		if source.Snapshot.Postgres != nil {
			allowed[serviceKey(source.Snapshot.Postgres.BackingServiceID)] = struct{}{}
		}
		if source.Snapshot.Volume != nil {
			allowed[environmentComposeProjectionKey(source.Snapshot.Volume.EnvironmentID)] = struct{}{}
			for _, service := range source.Snapshot.Volume.Services {
				allowed[serviceKey(service.ServiceID)] = struct{}{}
			}
		}
		if source.Snapshot.Config != nil {
			snapshotID := source.Snapshot.Config.ConfigSnapshotID
			allowed[backupConfigSnapshotKey(snapshotID)] = struct{}{}
			allowed[backupConfigSnapshotTaskReferenceKey(run.RetryOfTaskID, snapshotID)] = struct{}{}
			allowed[backupConfigSnapshotReferenceTaskKey(snapshotID, run.RetryOfTaskID)] = struct{}{}
			allowed[backupConfigSnapshotTaskReferenceKey(run.TaskID, snapshotID)] = struct{}{}
			allowed[backupConfigSnapshotReferenceTaskKey(snapshotID, run.TaskID)] = struct{}{}
			allowed[environmentKey(run.EnvironmentID)] = struct{}{}
		}
	}
	result := make([]Condition, 0, len(allowed))
	for _, condition := range conditions {
		if _, keep := allowed[condition.Key]; keep {
			result = append(result, condition)
		}
	}
	return result
}

func (repository *BackupRuntimeRepository) exactBackupRunConfigCompanions(
	ctx context.Context,
	run BackupRunRecord,
	readRevision int64,
	resultRevision int64,
) bool {
	for _, source := range run.Sources {
		if source.Kind != BackupRuntimeSourceConfig {
			continue
		}
		snapshot := source.Snapshot.Config
		keys := []string{
			backupConfigSnapshotKey(snapshot.ConfigSnapshotID),
			backupConfigSnapshotTaskReferenceKey(run.TaskID, snapshot.ConfigSnapshotID),
			backupConfigSnapshotReferenceTaskKey(snapshot.ConfigSnapshotID, run.TaskID),
		}
		read, err := repository.readFixedKeys(ctx, keys, readRevision)
		if err != nil {
			return false
		}
		primaryRevisionValid := read.Values[0] != nil &&
			((run.RetryOfTaskID == "" && read.Values[0].ModRevision == resultRevision) ||
				(run.RetryOfTaskID != "" && read.Values[0].ModRevision > 0 &&
					read.Values[0].ModRevision < resultRevision))
		if !primaryRevisionValid || read.Values[1] == nil || read.Values[2] == nil ||
			read.Values[1].ModRevision != resultRevision ||
			read.Values[2].ModRevision != resultRevision ||
			string(read.Values[1].Value) != snapshot.ConfigSnapshotID ||
			string(read.Values[2].Value) != run.TaskID {
			clearKeyValues(read.Values)
			return false
		}
		stored, decodeErr := decodeBackupConfigSnapshotRecord(read.Values[0].Value)
		clearKeyValues(read.Values)
		createdAtValid := (run.RetryOfTaskID == "" && stored.CreatedAt.Equal(run.CreatedAt)) ||
			(run.RetryOfTaskID != "" && stored.CreatedAt.Before(run.CreatedAt))
		if decodeErr != nil || stored.SnapshotID != snapshot.ConfigSnapshotID ||
			stored.EnvironmentID != run.EnvironmentID || stored.SourceID != source.SourceID ||
			stored.State == BackupConfigSnapshotUninitialized ||
			stored.ReadRevision != snapshot.ReadRevision || !createdAtValid ||
			(run.RetryOfTaskID == "" && (!stored.UpdatedAt.Equal(run.CreatedAt) ||
				stored.State != BackupConfigSnapshotBuilding)) {
			return false
		}
	}
	return true
}

func (repository *BackupRuntimeRepository) TransitionBackupRun(
	ctx context.Context,
	authority BackupAssignmentInput,
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
) (Versioned[BackupRunRecord], error) {
	if terminalBackupRunState(next.State) {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run requires atomic Task completion",
		)
	}
	if err := validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	); err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if backupRunTransitionRequiresCheckpoint(current.Record, next) {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup source transition requires a checkpoint",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, &authority, nil)
}

func (repository *BackupRuntimeRepository) CheckpointBackupRun(
	ctx context.Context,
	checkpoint BackupCheckpointInput,
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
) (Versioned[BackupRunRecord], error) {
	if terminalBackupRunState(next.State) || validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	) != nil {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"checkpointed backup run transition is invalid",
		)
	}
	ordinal, changed := changedBackupSourceOrdinal(current.Record, next)
	if !changed || !backupRunCheckpointMatchesTransition(
		checkpoint.Payload,
		current.Record.Sources[ordinal],
		next.Sources[ordinal],
	) {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run checkpoint does not match its source transition",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, nil, &checkpoint)
}

func backupRunTransitionRequiresCheckpoint(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := changedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return (from.State == BackupSourceAttemptReady && to.State == BackupSourceAttemptStaged) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State &&
			from.Phase == BackupSourcePhaseUpload && to.Phase == BackupSourcePhaseHeadVerification) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State && from.Phase == BackupSourcePhaseHeadVerification &&
			to.Phase == BackupSourcePhasePointCommit) ||
		(from.State == BackupSourceAttemptCleanupPending && to.State == BackupSourceAttemptSucceeded)
}

func backupRunCheckpointMatchesTransition(
	payload BackupCheckpointPayload,
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
) bool {
	if current.State == BackupSourceAttemptReady && next.State == BackupSourceAttemptStaged {
		return payload.Kind == BackupCheckpointArtifactPrepared &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == BackupSourcePhaseUpload &&
		next.Phase == BackupSourcePhaseHeadVerification {
		return payload.Kind == BackupCheckpointUploadCompleted &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == BackupSourcePhaseHeadVerification &&
		next.Phase == BackupSourcePhasePointCommit {
		return payload.Kind == BackupCheckpointUploadVerified &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if current.State == BackupSourceAttemptCleanupPending &&
		next.State == BackupSourceAttemptSucceeded {
		return payload.Kind == BackupCheckpointSourceCleanupCompleted &&
			payload.PointID == next.RecoveryPointID
	}
	return false
}

// prepareBackupRunTerminal composes the terminal run checkpoint, complete
// exclusion release, exact operation-lock release, and Environment epoch
// advance with the caller's terminal Task and idempotency mutations.
func (repository *BackupRuntimeRepository) prepareBackupRunTerminal(
	ctx context.Context,
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
) (backupRunPublicationPlan, error) {
	return repository.prepareBackupRunTerminalPlan(ctx, current, next, nil, nil)
}

func (repository *BackupRuntimeRepository) prepareBackupRunTerminalPlan(
	ctx context.Context,
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
	absentOrphan *Versioned[BackupOrphanRecord],
	checkpoint *BackupCheckpointInput,
) (backupRunPublicationPlan, error) {
	transitionMode := backupRunTransitionOrdinary
	needsTerminalOrphan := backupRunNeedsTerminalOrphan(current.Record, next)
	retainsTerminalOrphan := absentOrphan == nil &&
		backupRunRetainsTerminalOrphan(current.Record, next)
	if absentOrphan != nil {
		transitionMode = backupRunTransitionOrphanDelete
	} else if needsTerminalOrphan {
		transitionMode = backupRunTransitionOrphanCreate
	} else if retainsTerminalOrphan {
		transitionMode = backupRunTransitionOrphanTerminal
	}
	if backupRunRequiresTerminalOrphan(current.Record) && !needsTerminalOrphan {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup must preserve upload intent as an orphan",
		)
	}
	if current.Revision <= 0 || !terminalBackupRunState(next.State) ||
		validateBackupRunTransition(current.Record, next, transitionMode) != nil {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run transition is invalid",
		)
	}
	if next.State == BackupRunCompleted &&
		!backupRunReadyForSuccessfulTerminal(current.Record, next) {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"successful backup terminal requires completed cleanup checkpoints",
		)
	}
	records, err := backupRunExclusionRecords(current.Record, current.Record.CreatedAt)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	keys := make([]string, len(records)+1)
	keys[0] = backupRunKey(current.Record.TaskID)
	for index, record := range records {
		keys[index+1], err = backupSourceTargetExclusionKey(record.TargetKind, record.TargetID)
		if err != nil {
			return backupRunPublicationPlan{}, err
		}
	}
	orphanOffset := len(keys)
	var retainedOrphan BackupOrphanRecord
	if retainsTerminalOrphan || absentOrphan != nil {
		ordinal, _ := changedBackupSourceOrdinal(current.Record, next)
		pointID := current.Record.Sources[ordinal].RecoveryPointID
		connectorIndex, keyErr := backupOrphanConnectorIndexKey(current.Record.ConnectorID, pointID)
		if keyErr != nil {
			return backupRunPublicationPlan{}, keyErr
		}
		environmentIndex, keyErr := backupOrphanEnvironmentIndexKey(
			current.Record.EnvironmentID,
			pointID,
		)
		if keyErr != nil {
			return backupRunPublicationPlan{}, keyErr
		}
		keys = append(keys, backupOrphanKey(pointID), connectorIndex, environmentIndex)
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return backupRunPublicationPlan{}, errs.New(errs.KindStateConflict, "backup run changed")
	}
	stored, err := decodeBackupRunRecord(anchor.Values[0].Value)
	if err != nil || !backupRunRecordsEqual(stored, current.Record) {
		return backupRunPublicationPlan{}, errs.New(errs.KindStateConflict, "backup run changed")
	}
	for index, expected := range records {
		value := anchor.Values[index+1]
		if value == nil {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindInternal,
				"backup source target exclusion set is incomplete",
			)
		}
		exclusion, decodeErr := decodeBackupSourceTargetExclusionRecord(value.Value)
		if decodeErr != nil || !sameBackupExclusionOwner(exclusion, expected) {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindResourceInUse,
				"backup source target exclusion ownership changed",
			)
		}
	}
	if retainsTerminalOrphan || absentOrphan != nil {
		values := anchor.Values[orphanOffset : orphanOffset+3]
		if values[0] == nil {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"terminal backup orphan authority changed",
			)
		}
		retainedOrphan, err = decodeBackupOrphanRecord(values[0].Value)
		ordinal, _ := changedBackupSourceOrdinal(current.Record, next)
		expectedState := BackupOrphanInspect
		if absentOrphan != nil {
			expectedState = BackupOrphanDelete
		}
		if err != nil || retainedOrphan.TaskID != current.Record.TaskID ||
			retainedOrphan.State != expectedState ||
			!backupOrphanMatchesRunSource(retainedOrphan, current.Record, ordinal) {
			return backupRunPublicationPlan{}, corruptBackupRuntimeRecord()
		}
		if absentOrphan != nil &&
			(absentOrphan.Revision <= 0 || absentOrphan.Revision != values[0].ModRevision ||
				absentOrphan.Record != retainedOrphan) {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"terminal backup orphan authority changed",
			)
		}
		if err := validateBackupOrphanCompanionEvidence(values, retainedOrphan); err != nil {
			return backupRunPublicationPlan{}, err
		}
	}
	var checkpointPlan backupCheckpointPlan
	if checkpoint != nil {
		if checkpoint.TaskID != current.Record.TaskID {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint task does not match its run",
			)
		}
		ordinal, changed := changedBackupSourceOrdinal(current.Record, next)
		if !changed {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"terminal backup checkpoint source is invalid",
			)
		}
		checkpointPlan, err = repository.loadBackupCheckpointPlan(
			ctx,
			*checkpoint,
			anchor.ReadRevision,
			backupRunCheckpointBinding(current.Record, ordinal),
		)
		if err != nil {
			return backupRunPublicationPlan{}, err
		}
		defer checkpointPlan.clear()
		if checkpointPlan.duplicate {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain state is incomplete",
			)
		}
	}
	evidence, err := repository.loadOwnedEvidence(ctx, current.Record, anchor.ReadRevision)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	value, err := encodeBackupRunRecord(next)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	conditions := []Condition{{Key: keys[0], ModRevision: current.Revision}}
	mutations := []Mutation{{Type: MutationPut, Key: keys[0], Value: value}}
	for index, key := range keys[1 : len(records)+1] {
		conditions = append(conditions, Condition{
			Key: key, ModRevision: anchor.Values[index+1].ModRevision,
		})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: key})
	}
	if retainsTerminalOrphan || absentOrphan != nil {
		for index, value := range anchor.Values[orphanOffset : orphanOffset+3] {
			conditions = append(conditions, Condition{
				Key: keys[orphanOffset+index], ModRevision: value.ModRevision,
			})
		}
		if absentOrphan != nil {
			for _, key := range keys[orphanOffset : orphanOffset+3] {
				mutations = append(mutations, Mutation{Type: MutationDelete, Key: key})
			}
		}
	}
	if transitionMode == backupRunTransitionOrphanCreate {
		ordinal, changed := changedBackupSourceOrdinal(current.Record, next)
		if !changed {
			clearBackupRuntimeMutations(mutations)
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"terminal backup orphan transition is invalid",
			)
		}
		orphan := backupOrphanRecordFromRun(next, ordinal)
		orphanValue, encodeErr := encodeBackupOrphanRecord(orphan)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			return backupRunPublicationPlan{}, encodeErr
		}
		connectorIndex, keyErr := backupOrphanConnectorIndexKey(
			orphan.Point.ConnectorID,
			orphan.Point.ID,
		)
		if keyErr != nil {
			clear(orphanValue)
			clearBackupRuntimeMutations(mutations)
			return backupRunPublicationPlan{}, keyErr
		}
		environmentIndex, keyErr := backupOrphanEnvironmentIndexKey(
			orphan.Point.EnvironmentID,
			orphan.Point.ID,
		)
		if keyErr != nil {
			clear(orphanValue)
			clearBackupRuntimeMutations(mutations)
			return backupRunPublicationPlan{}, keyErr
		}
		orphanKey := backupOrphanKey(orphan.Point.ID)
		conditions = append(
			conditions,
			Condition{Key: orphanKey},
			Condition{Key: connectorIndex},
			Condition{Key: environmentIndex},
		)
		mutations = append(
			mutations,
			Mutation{Type: MutationPut, Key: orphanKey, Value: orphanValue},
			Mutation{Type: MutationPut, Key: connectorIndex, Value: []byte(orphan.Point.ID)},
			Mutation{Type: MutationPut, Key: environmentIndex, Value: []byte(orphan.Point.ID)},
		)
	}
	conditions = append(conditions, evidence.fence.transactionConditions()...)
	conditions = append(conditions, checkpointPlan.conditions...)
	mutations = append(mutations, Mutation{
		Type: MutationDelete, Key: environmentOperationLockKey(current.Record.EnvironmentID),
	})
	epoch, err := evidence.fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupRunPublicationPlan{}, err
	}
	mutations = append(mutations, epoch)
	for _, mutation := range checkpointPlan.mutations {
		mutations = append(mutations, Mutation{
			Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
		})
	}
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupRunPublicationPlan{}, err
	}
	return backupRunPublicationPlan{conditions: conditions, mutations: mutations, record: next}, nil
}

func backupRunReadyForSuccessfulTerminal(current BackupRunRecord, next BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	for index := range current.Sources {
		if current.Sources[index].State != BackupSourceAttemptSucceeded ||
			next.Sources[index].State != BackupSourceAttemptSucceeded ||
			!backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
			return false
		}
	}
	return true
}

func backupRunNeedsTerminalOrphan(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := changedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return from.State == BackupSourceAttemptStaged &&
		(from.Phase == BackupSourcePhaseUpload ||
			from.Phase == BackupSourcePhaseHeadVerification ||
			from.Phase == BackupSourcePhasePointCommit) &&
		to.State == BackupSourceAttemptOrphaned && to.Phase == from.Phase
}

func backupRunRetainsTerminalOrphan(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := changedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return from.State == BackupSourceAttemptOrphaned && to.State == from.State &&
		to.Phase == from.Phase && from.FailureCode == "" && to.FailureCode != ""
}

func backupRunRequiresTerminalOrphan(run BackupRunRecord) bool {
	for index := range run.Sources {
		source := run.Sources[index]
		if source.State == BackupSourceAttemptStaged &&
			(source.Phase == BackupSourcePhaseUpload ||
				source.Phase == BackupSourcePhaseHeadVerification ||
				source.Phase == BackupSourcePhasePointCommit) {
			return true
		}
	}
	return false
}

func backupOrphanRecordFromRun(run BackupRunRecord, ordinal uint32) BackupOrphanRecord {
	source := run.Sources[ordinal]
	return BackupOrphanRecord{
		Point: BackupRecoveryPointSnapshot{
			ID:              source.RecoveryPointID,
			EnvironmentID:   run.EnvironmentID,
			SourceID:        source.SourceID,
			SourceKind:      source.Kind,
			TargetID:        source.TargetID,
			ConnectorID:     run.ConnectorID,
			ConnectorPrefix: run.ConnectorPrefix,
			ObjectKey:       source.ObjectKey,
			SourceFormat:    source.Format,
			Encryption:      run.Encryption,
			KeyEra:          run.KeyEra,
			Recipient:       run.Recipient,
			SizeBytes:       source.SizeBytes,
			SHA256:          source.SHA256,
			CreatedAt:       source.RecoveryPointCreatedAt,
		},
		TaskID: run.TaskID,
		Reconciliation: BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			RetentionKeep:  run.RetentionKeep,
		},
		State: BackupOrphanInspect, CreatedAt: run.UpdatedAt, UpdatedAt: run.UpdatedAt,
	}
}

func (repository *BackupRuntimeRepository) replaceBackupRun(
	ctx context.Context,
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
	extraConditions []Condition,
	extraMutations []Mutation,
	validateExtra func([]*KeyValue) error,
	authority *BackupAssignmentInput,
	checkpoint *BackupCheckpointInput,
) (Versioned[BackupRunRecord], error) {
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run version is invalid",
		)
	}
	value, err := encodeBackupRunRecord(next)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clear(value)
	anchorKeys := make([]string, 1, len(extraConditions)+1)
	anchorKeys[0] = backupRunKey(current.Record.TaskID)
	for _, condition := range extraConditions {
		anchorKeys = append(anchorKeys, condition.Key)
	}
	anchor, err := repository.readCurrentKeys(ctx, anchorKeys)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindTaskNotFound,
			"backup run was not found",
		)
	}
	stored, err := decodeBackupRunRecord(anchor.Values[0].Value)
	if err != nil {
		return Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	replay := false
	if anchor.Values[0].ModRevision != current.Revision ||
		!backupRunRecordsEqual(stored, current.Record) {
		if backupRunRecordsEqual(stored, next) {
			replay = true
		} else {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup run changed",
			)
		}
	}
	if !replay {
		for index, condition := range extraConditions {
			if !conditionMatchesRead(condition, anchor.Values[index+1]) {
				return Versioned[BackupRunRecord]{}, errs.New(
					errs.KindStateConflict,
					"backup runtime companion state changed",
				)
			}
		}
		if validateExtra != nil {
			if err := validateExtra(anchor.Values[1:]); err != nil {
				return Versioned[BackupRunRecord]{}, err
			}
		}
	}
	var checkpointPlan backupCheckpointPlan
	var assignmentConditions []Condition
	if authority != nil {
		if authority.TaskID != current.Record.TaskID {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup assignment task does not match its run",
			)
		}
		assignmentConditions, err = repository.loadBackupAssignmentFence(
			ctx,
			*authority,
			anchor.ReadRevision,
		)
		if err != nil {
			return Versioned[BackupRunRecord]{}, err
		}
	}
	if checkpoint != nil {
		if checkpoint.TaskID != current.Record.TaskID {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint task does not match its run",
			)
		}
		ordinal, changed := changedBackupSourceOrdinal(current.Record, next)
		if !changed {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint source is invalid",
			)
		}
		checkpointPlan, err = repository.loadBackupCheckpointPlan(
			ctx, *checkpoint, anchor.ReadRevision,
			backupRunCheckpointBinding(current.Record, ordinal),
		)
		if err != nil {
			return Versioned[BackupRunRecord]{}, err
		}
		defer checkpointPlan.clear()
		if checkpointPlan.duplicate && !replay {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain state is incomplete",
			)
		}
	}
	if replay {
		if checkpoint != nil && !checkpointPlan.duplicate {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint replay evidence is incomplete",
			)
		}
		if checkpoint != nil && anchor.Values[0].ModRevision != checkpointPlan.commitRevision {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain evidence changed",
			)
		}
		if !exactBackupRuntimeReplayCompanions(
			anchor.Values[1:],
			extraConditions,
			extraMutations,
			anchor.Values[0].ModRevision,
		) {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup runtime replay companion state changed",
			)
		}
		return Versioned[BackupRunRecord]{
			Record:       stored,
			Revision:     anchor.Values[0].ModRevision,
			ReadRevision: anchor.ReadRevision,
		}, nil
	}
	evidence, err := repository.loadOwnedEvidence(ctx, current.Record, anchor.ReadRevision)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	conditions := []Condition{
		{Key: backupRunKey(current.Record.TaskID), ModRevision: current.Revision},
	}
	conditions = append(conditions, extraConditions...)
	conditions = append(conditions, evidence.fence.transactionConditions()...)
	mutations := []Mutation{{Type: MutationPut, Key: backupRunKey(next.TaskID), Value: value}}
	mutations = append(mutations, extraMutations...)
	epoch, err := evidence.fence.epochRewriteMutation()
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clear(epoch.Value)
	mutations = append(mutations, epoch)
	conditions = append(conditions, checkpointPlan.conditions...)
	conditions = append(conditions, assignmentConditions...)
	for _, mutation := range checkpointPlan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		if len(result.FailureReads) != len(conditions) {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindInternal,
				"backup run transition compare evidence is incomplete",
			)
		}
		if result.FailureReads[0] == nil || result.FailureReads[0].ModRevision != current.Revision {
			return Versioned[BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup run changed",
			)
		}
		fenceStart := 1 + len(extraConditions)
		fenceEnd := fenceStart + len(evidence.fence.conditions)
		if err := evidence.fence.classifyCAS(result.FailureReads[fenceStart:fenceEnd]); err != nil {
			return Versioned[BackupRunRecord]{}, err
		}
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup runtime state changed",
		)
	}
	return Versioned[BackupRunRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func exactBackupRuntimeReplayCompanions(
	values []*KeyValue,
	conditions []Condition,
	mutations []Mutation,
	resultRevision int64,
) bool {
	if len(values) != len(conditions) || resultRevision <= 0 {
		return false
	}
	byKey := make(map[string]Mutation, len(mutations))
	for _, mutation := range mutations {
		if _, exists := byKey[mutation.Key]; exists {
			return false
		}
		byKey[mutation.Key] = mutation
	}
	for index, condition := range conditions {
		value := values[index]
		mutation, changed := byKey[condition.Key]
		if !changed {
			if !conditionMatchesRead(condition, value) {
				return false
			}
			continue
		}
		delete(byKey, condition.Key)
		switch mutation.Type {
		case MutationPut:
			if value == nil || value.ModRevision != resultRevision ||
				!bytes.Equal(value.Value, mutation.Value) {
				return false
			}
		case MutationDelete:
			if value != nil {
				return false
			}
		default:
			return false
		}
	}
	return len(byKey) == 0
}

func (repository *BackupRuntimeRepository) GetBackupSourceTargetExclusion(
	ctx context.Context,
	kind BackupSourceTargetKind,
	targetID string,
) (Versioned[BackupSourceTargetExclusionRecord], bool, error) {
	key, err := backupSourceTargetExclusionKey(kind, targetID)
	if err != nil {
		return Versioned[BackupSourceTargetExclusionRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return Versioned[BackupSourceTargetExclusionRecord]{}, false, err
	}
	if result == nil {
		return Versioned[BackupSourceTargetExclusionRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup source-target exclusion read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[BackupSourceTargetExclusionRecord]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := decodeBackupSourceTargetExclusionRecord(result.Entry.Value)
	if err != nil || record.TargetKind != kind || record.TargetID != targetID {
		return Versioned[BackupSourceTargetExclusionRecord]{}, false, corruptBackupRuntimeRecord()
	}
	return Versioned[BackupSourceTargetExclusionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *BackupRuntimeRepository) loadOwnedEvidence(
	ctx context.Context,
	run BackupRunRecord,
	revision int64,
) (backupRuntimeOwnedEvidence, error) {
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		run.EnvironmentID,
		revision,
		environmentMutationFenceOwner{
			Kind: BackupOperationBackup, OperationID: run.OperationID, TaskID: run.TaskID,
		},
	)
	if err != nil {
		return backupRuntimeOwnedEvidence{}, err
	}
	return backupRuntimeOwnedEvidence{fence: fence}, nil
}

func (repository *BackupRuntimeRepository) readCurrentKeys(
	ctx context.Context,
	keys []string,
) (*GetManyResult, error) {
	if len(keys) == 0 || len(keys) > maximumTransactionOperations {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read key count is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			clearKeyValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
}

func (repository *BackupRuntimeRepository) readFixedKeys(
	ctx context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	if len(keys) == 0 || len(keys) > maximumTransactionOperations || revision <= 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read input is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			clearKeyValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
}

func (repository *BackupRuntimeRepository) loadBackupRunPublicationEvidence(
	ctx context.Context,
	run BackupRunRecord,
	retrySource *backupRunRetrySource,
	fixedRevision int64,
) ([]Condition, []Mutation, error) {
	conditions := make([]Condition, 0, len(run.Sources)*2+3)
	mutations := make([]Mutation, 0, 3)
	conditionRevisions := make(map[string]int64, len(run.Sources)*2+3)
	addCondition := func(key string, revision int64) error {
		if previous, exists := conditionRevisions[key]; exists {
			if previous != revision {
				return errs.New(
					errs.KindStateConflict,
					"backup publication revision evidence differs",
				)
			}
			return nil
		}
		conditionRevisions[key] = revision
		conditions = append(conditions, Condition{Key: key, ModRevision: revision})
		return nil
	}
	if retrySource == nil {
		sourceIDs := make([]string, len(run.Sources))
		for index := range run.Sources {
			sourceIDs[index] = run.Sources[index].SourceID
		}
		policyRead, err := repository.readFixedKeys(
			ctx,
			[]string{backupPolicyKey(run.EnvironmentID)},
			fixedRevision,
		)
		if err != nil {
			return nil, nil, err
		}
		defer clearKeyValues(policyRead.Values)
		if policyRead.Values[0] == nil || policyRead.Values[0].ModRevision != run.PolicyRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "backup policy snapshot changed")
		}
		policy, decodeErr := decodeBackupPolicyRecord(policyRead.Values[0].Value)
		if decodeErr != nil {
			return nil, nil, corruptBackupRuntimeRecord()
		}
		policyKeep, keepErr := checkedBackupRuntimeRetentionKeep(policy.Keep)
		if keepErr != nil {
			return nil, nil, keepErr
		}
		if !policy.Enabled || policy.EnvironmentID != run.EnvironmentID ||
			policyKeep != run.RetentionKeep || policy.ConnectorID != run.ConnectorID ||
			policy.Encryption != string(run.Encryption) ||
			!slices.Equal(policy.SourceIDs, sourceIDs) {
			return nil, nil, errs.New(errs.KindStateConflict, "backup policy snapshot changed")
		}
		if err := addCondition(backupPolicyKey(run.EnvironmentID), run.PolicyRevision); err != nil {
			return nil, nil, err
		}
	}
	if run.Encryption == BackupRuntimeEncryptionAge {
		keyRead, readErr := repository.readFixedKeys(ctx, []string{
			backupKeyKey(run.EnvironmentID), backupKeyValueKey(run.EnvironmentID),
		}, fixedRevision)
		if readErr != nil {
			return nil, nil, readErr
		}
		defer clearKeyValues(keyRead.Values)
		if keyRead.Values[0] == nil || keyRead.Values[1] == nil ||
			keyRead.Values[0].ModRevision != run.BackupKeyRecordRevision ||
			keyRead.Values[1].ModRevision != run.BackupKeyValueRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "backup key snapshot changed")
		}
		keyRecord, decodeErr := decodeBackupKeyRecord(keyRead.Values[0].Value)
		keyValue, valueErr := decodeBackupKeyEncryptedValue(keyRead.Values[1].Value)
		defer clear(keyValue.Ciphertext)
		if decodeErr != nil || valueErr != nil || keyRecord.EnvironmentID != run.EnvironmentID ||
			keyValue.EnvironmentID != run.EnvironmentID || keyRecord.KeyEra != run.KeyEra ||
			keyValue.KeyEra != run.KeyEra || keyRecord.Recipient != run.Recipient {
			return nil, nil, errs.New(errs.KindStateConflict, "backup key snapshot changed")
		}
		if err := addCondition(
			backupKeyKey(run.EnvironmentID),
			run.BackupKeyRecordRevision,
		); err != nil {
			return nil, nil, err
		}
		if err := addCondition(
			backupKeyValueKey(run.EnvironmentID),
			run.BackupKeyValueRevision,
		); err != nil {
			return nil, nil, err
		}
	}
	for _, source := range run.Sources {
		sourceRead, readErr := repository.readFixedKeys(
			ctx,
			[]string{backupSourceKey(source.SourceID)},
			fixedRevision,
		)
		if readErr != nil {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, readErr
		}
		if sourceRead.Values[0] == nil ||
			sourceRead.Values[0].ModRevision != source.SourceRevision {
			clearKeyValues(sourceRead.Values)
			clearBackupRuntimeMutations(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		storedSource, decodeErr := decodeBackupSourceRecord(sourceRead.Values[0].Value)
		clearKeyValues(sourceRead.Values)
		if decodeErr != nil || storedSource.ID != source.SourceID ||
			storedSource.EnvironmentID != run.EnvironmentID ||
			string(
				storedSource.Kind,
			) != string(
				source.Kind,
			) || storedSource.TargetID != source.TargetID {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		if err := addCondition(
			backupSourceKey(source.SourceID),
			source.SourceRevision,
		); err != nil {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, err
		}
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			read, readErr := repository.readFixedKeys(ctx, []string{
				attachKey(source.TargetID),
				attachFactsKey(source.TargetID),
				projectKey(
					snapshot.BackingProjectID,
				),
				environmentKey(snapshot.BackingEnvironmentID),
				serviceKey(snapshot.BackingServiceID),
			}, fixedRevision)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if err := validateBackupPostgresPublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				clearKeyValues(read.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			clearKeyValues(read.Values)
			for _, fact := range []struct {
				key      string
				revision int64
			}{
				{attachKey(source.TargetID), source.TargetRevision},
				{attachFactsKey(source.TargetID), snapshot.AttachFactsRevision},
				{projectKey(snapshot.BackingProjectID), snapshot.BackingProjectRevision},
				{environmentKey(snapshot.BackingEnvironmentID), snapshot.BackingEnvironmentRevision},
				{serviceKey(snapshot.BackingServiceID), snapshot.BackingServiceRevision},
			} {
				if err := addCondition(fact.key, fact.revision); err != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, err
				}
			}
		case BackupRuntimeSourceVolume:
			snapshot := source.Snapshot.Volume
			keys := []string{
				environmentKey(snapshot.EnvironmentID),
				environmentBlueprintHeadKey(snapshot.EnvironmentID),
				environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
			}
			for _, service := range snapshot.Services {
				keys = append(keys, serviceKey(service.ServiceID))
			}
			read, readErr := repository.readFixedKeys(ctx, keys, fixedRevision)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if err := validateBackupVolumePublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				clearKeyValues(read.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			clearKeyValues(read.Values)
			if err := addCondition(environmentKey(snapshot.EnvironmentID), snapshot.EnvironmentRevision); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			if err := addCondition(environmentBlueprintHeadKey(snapshot.EnvironmentID), source.TargetRevision); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			if err := addCondition(
				environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
				snapshot.ProjectionRoot,
			); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			for _, service := range snapshot.Services {
				if err := addCondition(
					serviceKey(service.ServiceID),
					service.ServiceRevision,
				); err != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, err
				}
			}
		case BackupRuntimeSourceConfig:
			snapshot := source.Snapshot.Config
			if retrySource != nil {
				configConditions, configMutations, retryErr := repository.prepareBackupRetryConfigReferences(
					ctx, run, source, retrySource.run.Record, fixedRevision,
				)
				if retryErr != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, retryErr
				}
				for _, condition := range configConditions {
					if err := addCondition(condition.Key, condition.ModRevision); err != nil {
						clearBackupRuntimeMutations(configMutations)
						clearBackupRuntimeMutations(mutations)
						return nil, nil, err
					}
				}
				mutations = append(mutations, configMutations...)
				continue
			}
			if snapshot.ConfigSnapshotID != run.TaskID || snapshot.ReadRevision != fixedRevision {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, errs.New(
					errs.KindStateConflict,
					"backup config snapshot revision changed",
				)
			}
			targetRead, readErr := repository.readFixedKeys(
				ctx,
				[]string{environmentKey(run.EnvironmentID)},
				fixedRevision,
			)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if targetRead.Values[0] == nil ||
				targetRead.Values[0].ModRevision != source.TargetRevision {
				clearKeyValues(targetRead.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, errs.New(errs.KindStateConflict, "backup config target changed")
			}
			environment, decodeErr := decodeEnvironment(targetRead.Values[0].Value)
			clearKeyValues(targetRead.Values)
			if decodeErr != nil || environment.ID != run.EnvironmentID {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, corruptBackupRuntimeRecord()
			}
			config := BackupConfigSnapshotRecord{
				SnapshotID:    snapshot.ConfigSnapshotID,
				EnvironmentID: run.EnvironmentID,
				SourceID:      source.SourceID,
				State:         BackupConfigSnapshotBuilding,
				ReadRevision:  snapshot.ReadRevision,
				CreatedAt:     run.CreatedAt,
				UpdatedAt:     run.CreatedAt,
			}
			value, encodeErr := encodeBackupConfigSnapshotRecord(config)
			if encodeErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, encodeErr
			}
			mutations = append(
				mutations,
				Mutation{
					Type:  MutationPut,
					Key:   backupConfigSnapshotKey(snapshot.ConfigSnapshotID),
					Value: value,
				},
				Mutation{
					Type: MutationPut,
					Key: backupConfigSnapshotTaskReferenceKey(
						run.TaskID,
						snapshot.ConfigSnapshotID,
					),
					Value: []byte(snapshot.ConfigSnapshotID),
				},
				Mutation{
					Type: MutationPut,
					Key: backupConfigSnapshotReferenceTaskKey(
						snapshot.ConfigSnapshotID,
						run.TaskID,
					),
					Value: []byte(run.TaskID),
				},
			)
		}
	}
	return conditions, mutations, nil
}

func checkedBackupRuntimeRetentionKeep(keep int64) (int64, error) {
	if keep <= 0 || keep > MaximumBackupPolicyKeep {
		return 0, corruptBackupRuntimeRecord()
	}
	return keep, nil
}

func validateBackupPostgresPublicationEvidence(
	values []*KeyValue,
	source BackupRunSourceAttemptRecord,
	snapshot BackupPostgresSourceSnapshot,
) error {
	if len(values) != 5 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[3] == nil || values[4] == nil || values[0].ModRevision != source.TargetRevision ||
		values[1].ModRevision != snapshot.AttachFactsRevision ||
		values[2].ModRevision != snapshot.BackingProjectRevision ||
		values[3].ModRevision != snapshot.BackingEnvironmentRevision ||
		values[4].ModRevision != snapshot.BackingServiceRevision {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	attach, attachErr := decodeAttachRecord(values[0].Value)
	facts, factsErr := decodeAttachEncryptedFacts(values[1].Value)
	defer clear(facts.Ciphertext)
	project, projectErr := decodeProject(values[2].Value)
	environment, environmentErr := decodeEnvironment(values[3].Value)
	service, serviceErr := decodeServiceRecord(values[4].Value)
	if attachErr != nil || factsErr != nil || projectErr != nil || environmentErr != nil ||
		serviceErr != nil {
		return corruptBackupRuntimeRecord()
	}
	if attach.ID != source.TargetID || attach.EnvironmentID != snapshot.ConsumerEnvironmentID ||
		string(attach.Status) != "ready" || attach.BackingProjectID != snapshot.BackingProjectID ||
		attach.BackingEnvironmentID != snapshot.BackingEnvironmentID ||
		attach.BackingServiceID != snapshot.BackingServiceID || facts.AttachID != source.TargetID ||
		project.ID != snapshot.BackingProjectID || project.Kind != ProjectKindBacking ||
		environment.ID != snapshot.BackingEnvironmentID || environment.ProjectID != project.ID ||
		service.Desired.ID != snapshot.BackingServiceID ||
		service.EnvironmentID != snapshot.BackingEnvironmentID {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	return nil
}

func validateBackupVolumePublicationEvidence(
	values []*KeyValue,
	source BackupRunSourceAttemptRecord,
	snapshot BackupVolumeSourceSnapshot,
) error {
	const offset = 3
	if len(values) != len(snapshot.Services)+offset || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].ModRevision != snapshot.EnvironmentRevision ||
		values[1].ModRevision != source.TargetRevision || values[2].ModRevision != snapshot.ProjectionRoot {
		return errs.New(errs.KindStateConflict, "volume backup publication evidence changed")
	}
	environment, environmentErr := decodeEnvironment(values[0].Value)
	revisionID, headErr := decodeTaskReference(values[1].Value)
	seal, sealErr := decodeEnvironmentBlueprintSeal(values[2].Value)
	if environmentErr != nil || headErr != nil || sealErr != nil {
		return corruptBackupRuntimeRecord()
	}
	if environment.ID != snapshot.EnvironmentID || environment.VolumeDir != snapshot.AuthorizedVolumeDir ||
		revisionID != snapshot.DesiredRevisionID || seal.EnvironmentID != snapshot.EnvironmentID ||
		seal.RevisionID != snapshot.DesiredRevisionID || seal.RenderGeneration != snapshot.RenderGeneration ||
		hex.EncodeToString(seal.DependencyDigest[:]) != snapshot.DependencyDigest ||
		snapshot.VolumeID != source.TargetID || snapshot.DockerVolumeName != "gp_vol_"+source.TargetID {
		return errs.New(errs.KindStateConflict, "volume projection evidence changed")
	}
	for index, expected := range snapshot.Services {
		value := values[index+offset]
		if value == nil || value.ModRevision != expected.ServiceRevision {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
		service, decodeErr := decodeServiceRecord(value.Value)
		if decodeErr != nil {
			return corruptBackupRuntimeRecord()
		}
		if service.Desired.ID != expected.ServiceID ||
			service.EnvironmentID != snapshot.EnvironmentID ||
			string(service.Runtime.RuntimeIntent) != string(expected.PriorIntent) {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
	}
	return nil
}

func equalBackupMountPaths(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (repository *BackupRuntimeRepository) transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		return TransactionResult{}, err
	}
	return repository.store.Transact(ctx, conditions, mutations)
}

func validateBackupRuntimeTransactionBounds(conditions []Condition, mutations []Mutation) error {
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return errs.New(
			errs.KindInternal,
			"backup runtime transaction exceeds the 96-operation limit",
		)
	}
	size := 0
	for _, condition := range conditions {
		size += len(condition.Key) + 64
	}
	for _, mutation := range mutations {
		size += len(mutation.Key) + len(mutation.Value) + 64
	}
	if size > maximumBackupRuntimeTransactionBytes {
		return errs.New(
			errs.KindInternal,
			"backup runtime transaction exceeds the 768 KiB preflight limit",
		)
	}
	return nil
}

type backupRunTransitionMode uint8

const (
	backupRunTransitionOrdinary backupRunTransitionMode = iota + 1
	backupRunTransitionOrphanCreate
	backupRunTransitionOrphanDelete
	backupRunTransitionOrphanTerminal
	backupRunTransitionPointCommit
	backupRunTransitionRetentionComplete
)

func validateBackupRunTransition(
	current BackupRunRecord,
	next BackupRunRecord,
	mode backupRunTransitionMode,
) error {
	if validateBackupRunRecord(current) != nil || validateBackupRunRecord(next) != nil ||
		!next.UpdatedAt.After(current.UpdatedAt) || !backupRunImmutableEqual(current, next) ||
		!validBackupRunStateTransition(current.State, next.State) {
		return errs.New(errs.KindValidationFailed, "backup run transition is invalid")
	}
	changed, hasChanged := changedBackupSourceOrdinal(current, next)
	if !hasChanged {
		for index := range current.Sources {
			if !backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
				return errs.New(
					errs.KindValidationFailed,
					"backup run transition changes multiple sources",
				)
			}
		}
		if current.State == next.State {
			return errs.New(errs.KindValidationFailed, "backup run transition is a no-op")
		}
		return nil
	}
	from := current.Sources[changed]
	to := next.Sources[changed]
	if !validBackupSourceTransition(from, to, mode) {
		return errs.New(errs.KindValidationFailed, "backup source transition is invalid")
	}
	return nil
}

func validBackupRunStateTransition(current BackupRunState, next BackupRunState) bool {
	if current == next {
		return current == BackupRunQueued || current == BackupRunRunning
	}
	switch current {
	case BackupRunQueued:
		return next == BackupRunRunning || next == BackupRunFailed || next == BackupRunAborted ||
			next == BackupRunTimedOut
	case BackupRunRunning:
		return next == BackupRunFailed || next == BackupRunCompleted || next == BackupRunAborted ||
			next == BackupRunTimedOut
	default:
		return false
	}
}

func validBackupSourceTransition(
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
	mode backupRunTransitionMode,
) bool {
	if sourceAttemptRequiresArtifact(current.State, current.Phase) &&
		(current.SizeBytes != next.SizeBytes || current.SHA256 != next.SHA256) {
		return false
	}
	if mode == backupRunTransitionOrphanCreate {
		return current.State == BackupSourceAttemptStaged &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanDelete {
		return current.State == BackupSourceAttemptOrphaned &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptFailed && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanTerminal {
		return current.State == BackupSourceAttemptOrphaned &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase &&
			current.FailureCode == "" && next.FailureCode != ""
	}
	if mode == backupRunTransitionPointCommit {
		return ((current.State == BackupSourceAttemptStaged && current.Phase == BackupSourcePhasePointCommit) ||
			(current.State == BackupSourceAttemptOrphaned && current.Phase == BackupSourcePhasePointCommit)) &&
			next.State == BackupSourceAttemptPointCommitted &&
			next.Phase == BackupSourcePhaseRetention
	}
	if mode == backupRunTransitionRetentionComplete {
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention &&
			next.State == BackupSourceAttemptCleanupPending &&
			next.Phase == BackupSourcePhaseCleanup
	}
	if next.State == BackupSourceAttemptFailed {
		return activeBackupSourceAttemptState(current.State) &&
			current.State != BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	switch current.State {
	case BackupSourceAttemptPending:
		return current.Phase == BackupSourcePhaseCapture &&
			next.State == BackupSourceAttemptCapturing && next.Phase == current.Phase
	case BackupSourceAttemptCapturing:
		return current.Phase == BackupSourcePhaseCapture &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging
	case BackupSourceAttemptReady:
		return current.Phase == BackupSourcePhaseStaging &&
			next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload
	case BackupSourceAttemptStaged:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload && next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification && next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptOrphaned:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload &&
				next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification &&
					next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptPointCommitted:
		return next.State == current.State && next.Phase == current.Phase &&
			next.FailureCode == BackupFailureRetention
	case BackupSourceAttemptCleanupPending:
		return next.Phase == current.Phase && (next.State == BackupSourceAttemptSucceeded ||
			(next.State == current.State && next.FailureCode == BackupFailureCleanup))
	default:
		return false
	}
}

func backupRunCheckpointBinding(run BackupRunRecord, ordinal uint32) backupCheckpointBinding {
	if int(ordinal) >= len(run.Sources) {
		return backupCheckpointBinding{}
	}
	return backupCheckpointBinding{
		taskType: TaskBackup, ordinal: ordinal, pointID: run.Sources[ordinal].RecoveryPointID,
	}
}

func backupRunImmutableEqual(current BackupRunRecord, next BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	normalized := next
	normalized.State = current.State
	normalized.UpdatedAt = current.UpdatedAt
	normalized.Sources = append([]BackupRunSourceAttemptRecord(nil), next.Sources...)
	for index := range normalized.Sources {
		normalized.Sources[index].State = current.Sources[index].State
		normalized.Sources[index].Phase = current.Sources[index].Phase
		normalized.Sources[index].SizeBytes = current.Sources[index].SizeBytes
		normalized.Sources[index].SHA256 = current.Sources[index].SHA256
		normalized.Sources[index].FailureCode = current.Sources[index].FailureCode
	}
	return backupRunRecordsEqual(current, normalized)
}

func backupRunSourceMutableEqual(
	left BackupRunSourceAttemptRecord,
	right BackupRunSourceAttemptRecord,
) bool {
	return left.State == right.State && left.Phase == right.Phase &&
		left.SizeBytes == right.SizeBytes &&
		left.SHA256 == right.SHA256 &&
		left.FailureCode == right.FailureCode
}

func backupRunRecordsEqual(left BackupRunRecord, right BackupRunRecord) bool {
	leftValue, leftErr := encodeBackupRunRecord(left)
	rightValue, rightErr := encodeBackupRunRecord(right)
	defer clear(leftValue)
	defer clear(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func backupRunExclusionRecords(
	run BackupRunRecord,
	at time.Time,
) ([]BackupSourceTargetExclusionRecord, error) {
	if validateBackupRunRecord(run) != nil || !validBackupRuntimeInstant(at) {
		return nil, errs.New(errs.KindValidationFailed, "backup run exclusion input is invalid")
	}
	byKey := make(map[string]BackupSourceTargetExclusionRecord)
	add := func(kind BackupSourceTargetKind, targetID string) error {
		key, err := backupSourceTargetExclusionKey(kind, targetID)
		if err != nil {
			return err
		}
		byKey[key] = BackupSourceTargetExclusionRecord{
			EnvironmentID: run.EnvironmentID,
			OperationID:   run.OperationID,
			TaskID:        run.TaskID,
			OperationKind: BackupOperationBackup,
			TargetKind:    kind,
			TargetID:      targetID,
			CreatedAt:     at,
			UpdatedAt:     at,
		}
		return nil
	}
	for _, source := range run.Sources {
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			if err := add(BackupSourceTargetAttach, source.TargetID); err != nil {
				return nil, err
			}
		case BackupRuntimeSourceVolume:
			if err := add(BackupSourceTargetVolume, source.TargetID); err != nil {
				return nil, err
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]BackupSourceTargetExclusionRecord, len(keys))
	for index, key := range keys {
		result[index] = byKey[key]
	}
	return result, nil
}

func exactBackupExclusions(
	values []*KeyValue,
	records []BackupSourceTargetExclusionRecord,
	resultRevision int64,
) bool {
	if len(values) != len(records) {
		return false
	}
	for index, value := range values {
		if value == nil || value.ModRevision != resultRevision {
			return false
		}
		stored, err := decodeBackupSourceTargetExclusionRecord(value.Value)
		if err != nil || !sameBackupExclusionOwner(stored, records[index]) {
			return false
		}
	}
	return true
}

func sameBackupExclusionOwner(
	left BackupSourceTargetExclusionRecord,
	right BackupSourceTargetExclusionRecord,
) bool {
	return left.EnvironmentID == right.EnvironmentID && left.OperationID == right.OperationID &&
		left.TaskID == right.TaskID && left.OperationKind == right.OperationKind &&
		left.TargetKind == right.TargetKind && left.TargetID == right.TargetID
}

func terminalBackupRunState(state BackupRunState) bool {
	return state == BackupRunFailed || state == BackupRunCompleted || state == BackupRunAborted ||
		state == BackupRunTimedOut
}

func clearBackupRuntimeMutations(mutations []Mutation) {
	for index := range mutations {
		clear(mutations[index].Value)
		mutations[index].Value = nil
	}
}

func (repository *BackupRuntimeRepository) loadManualBackupPolicyFence(
	ctx context.Context,
	record BackupRunRecord,
	fixedRevision int64,
) ([]Condition, error) {
	if record.Initiator != BackupRunInitiatorOperator {
		return nil, nil
	}
	key := environmentCoordinationKey(record.EnvironmentID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{key}, Revision: fixedRevision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != fixedRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].ModRevision <= 0 {
		return nil, errs.New(errs.KindStateConflict, "backup policy schedule coordination changed")
	}
	defer clearKeyValues(read.Values)
	coordination, err := decodeEnvironmentCoordinationRecord(read.Values[0].Value)
	if err != nil || coordination.EnvironmentID != record.EnvironmentID ||
		coordination.CurrentBackupScheduleState == nil {
		return nil, errs.New(errs.KindStateConflict, "backup policy schedule coordination is invalid")
	}
	return []Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}
