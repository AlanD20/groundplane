package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorDeletionRoute          = "/connectors/{id}"
	connectorDeletionTimeoutSeconds = 30
)

// BeginConnectorDeletionWithTask atomically fences a visible Connector and
// publishes the Controller Task and immutable intent that own finalization.
func (repository *ConnectorRepository) BeginConnectorDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent connectorrecord.RemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateConnectorHierarchy(ctx, environment, project, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateConnectorVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	connector := current.Record.Connector
	if err := validateConnectorDeletionEnvelope(
		current,
		tombstone,
		intent,
		task,
		marker,
	); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(
		ctx,
		repository.store,
		marker,
	); err != nil ||
		found {
		return existing, err
	}
	domainKeys := []string{
		taskjournal.TaskStorageKey(task.ID),
		taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskQueueKey(task.Executor, task.ID),
		connectorrecord.RecordKey(connector.ID),
		connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
		connectorNameKey(connector.EnvironmentID, connector.Name),
		connectorrecord.CredentialValueKey(connector.ID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID),
		connectorrecord.RemovalIntentKey(task.ID),
		backuppolicy.BackupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID),
	}
	fence, fixed, err := repository.loadConnectorMutationFence(
		ctx,
		environment,
		project,
		domainKeys,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearValues(fixed.Values)
	dependencies := fixed.Values[5:8]
	if dependencies[0] == nil || string(dependencies[0].Value) != connector.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"connector environment index is missing or corrupt",
		)
	}
	if dependencies[1] == nil || string(dependencies[1].Value) != connector.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"connector name index is missing or corrupt",
		)
	}
	if dependencies[2] == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"connector encrypted credentials are missing",
		)
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(dependencies[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return IdempotencyTransactionResult{}, connectorrecord.CorruptRecord()
	}
	clear(credentials.Ciphertext)
	referenceConditions, err := requireConnectorReferencePrefixesEmpty(
		ctx, repository.store, connector.ID, connector.EnvironmentID, fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}

	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	intentValue, err := connectorrecord.EncodeRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	evidence := newConnectorDeletionEvidence(current, fixed, task, fence, referenceConditions)
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: connectorrecord.RemovalIntentKey(task.ID), Value: intentValue},
		epochMutation,
	}
	taskTenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		project,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		taskTenant,
		project,
		environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		evidence.conditions,
		mutations,
		evidence.classifier(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateConnectorDeletionEnvelope(
	current etcdstore.Versioned[connectorrecord.Record],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent connectorrecord.RemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return err
	}
	if err := connectorrecord.ValidateRemovalIntent(intent); err != nil {
		return err
	}
	connector := current.Record.Connector
	if tombstone.TargetKind != deletionrecord.DeletionTargetConnector || tombstone.TargetID != connector.ID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != connector.EnvironmentID || intent.ConnectorID != connector.ID ||
		intent.ConnectorRevision != current.Revision || !intent.CreatedAt.Equal(task.CreatedAt) ||
		task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove || task.Target != connector.ID ||
		task.Status != taskjournal.TaskStatusPending || task.TimeoutSeconds != connectorDeletionTimeoutSeconds ||
		task.IdempotencyKey == "" || task.IdempotencyKey != marker.Locator.Key || len(task.Params) != 3 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceConnector ||
		task.Params[TaskConnectorEnvironmentParam] != connector.EnvironmentID ||
		task.Params[TaskConnectorNameParam] != connector.Name {
		return errs.New(
			errs.KindValidationFailed,
			"connector deletion task, tombstone, and intent do not match",
		)
	}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetConnector,
		ID:   connector.ID,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != connector.EnvironmentID || marker.Locator.Method != http.MethodDelete ||
		marker.Locator.Route != connectorDeletionRoute || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || marker.Response.Status != http.StatusAccepted ||
		!idempotencyrecord.ValidTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() {
		return errs.New(
			errs.KindValidationFailed,
			"connector deletion marker does not match its task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return err
	}
	return nil
}

func requireConnectorReferencePrefixesEmpty(
	ctx context.Context,
	store hierarchyStore,
	connectorID string,
	environmentID string,
	revision int64,
) ([]etcdstore.Condition, error) {
	prefixes := []string{
		backuppolicy.BackupPolicyConnectorReferencePrefix(connectorID),
		backupruntime.BackupRecoveryPointConnectorPrefix + connectorID + "/",
		backupruntime.BackupOrphanConnectorPrefix + connectorID + "/",
	}
	conditions := make([]etcdstore.Condition, 0, len(prefixes))
	for index, prefix := range prefixes {
		result, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, Limit: 1, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) > 1 {
			return nil, errs.New(errs.KindInternal, "connector reference prefix read is incomplete")
		}
		if len(result.Values) != 0 {
			return nil, classifyConnectorReference(index, result.Values[0], connectorID, environmentID)
		}
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return conditions, nil
}

func classifyConnectorReference(
	index int,
	value etcdstore.KeyValue,
	connectorID string,
	environmentID string,
) error {
	switch index {
	case 0:
		expectedKey := backuppolicy.BackupPolicyConnectorReferenceKey(connectorID, environmentID)
		if value.Key != expectedKey || string(value.Value) != environmentID {
			return errs.New(errs.KindInternal, "connector reference prefix is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by an enabled backup policy")
	case 1:
		recoveryPointID := string(value.Value)
		expectedKey, err := backupruntime.BackupRecoveryPointConnectorIndexKey(connectorID, recoveryPointID)
		if err != nil || value.Key != expectedKey {
			return errs.New(errs.KindInternal, "connector recovery point reference is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by a recovery point")
	case 2:
		recoveryPointID := string(value.Value)
		expectedKey, err := backupruntime.BackupOrphanConnectorIndexKey(connectorID, recoveryPointID)
		if err != nil || value.Key != expectedKey {
			return errs.New(errs.KindInternal, "connector orphan reference is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by a backup orphan")
	default:
		return errs.New(errs.KindInternal, "connector reference kind is invalid")
	}
}

type connectorDeletionEvidence struct {
	conditions       []etcdstore.Condition
	task             int
	operation        int
	active           int
	queue            int
	primary          int
	environmentIndex int
	nameIndex        int
	credentials      int
	tombstone        int
	intent           int
	reference        int
	referenceStart   int
	fenceStart       int
	current          etcdstore.Versioned[connectorrecord.Record]
	operationID      string
	fence            environmentfence.Evidence
}

func newConnectorDeletionEvidence(
	current etcdstore.Versioned[connectorrecord.Record],
	dependencies *etcdstore.GetManyResult,
	task TaskRecord,
	fence environmentfence.Evidence,
	referenceConditions []etcdstore.Condition,
) connectorDeletionEvidence {
	connector := current.Record.Connector
	evidence := connectorDeletionEvidence{
		task: 0, operation: 1, active: 2, queue: 3, primary: 4,
		environmentIndex: 5, nameIndex: 6, credentials: 7, tombstone: 8, intent: 9,
		reference: 10, referenceStart: 11, current: current, operationID: task.OperationID, fence: fence,
		conditions: []etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID)},
			{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
			{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
			{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
			{Key: connectorrecord.RecordKey(connector.ID), ModRevision: current.Revision},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[5].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[6].ModRevision,
			},
			{
				Key:         connectorrecord.CredentialValueKey(connector.ID),
				ModRevision: dependencies.Values[7].ModRevision,
			},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)},
			{Key: connectorrecord.RemovalIntentKey(task.ID)},
			{Key: backuppolicy.BackupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID)},
		},
	}
	evidence.conditions = append(evidence.conditions, referenceConditions...)
	evidence.fenceStart = len(evidence.conditions)
	evidence.conditions = append(evidence.conditions, fence.TransactionConditions()...)
	return evidence
}

func (evidence connectorDeletionEvidence) classifier() idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(evidence.conditions) {
			return errs.New(errs.KindInternal, "connector deletion compare evidence is incomplete")
		}
		if values[evidence.active] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[evidence.active].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				evidence.operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{evidence.task, evidence.operation, evidence.queue} {
			if values[index] != nil {
				return errs.New(
					errs.KindInternal,
					"connector deletion collided with durable task state",
				)
			}
		}
		connector := evidence.current.Record.Connector
		if values[evidence.primary] == nil {
			return errs.New(errs.KindConnectorNotFound, "connector was not found")
		}
		if values[evidence.primary].ModRevision != evidence.current.Revision {
			return recordcodec.StateConflict("connector", connector.ID)
		}
		if values[evidence.environmentIndex] == nil ||
			string(values[evidence.environmentIndex].Value) != connector.ID ||
			values[evidence.environmentIndex].ModRevision != evidence.conditions[evidence.environmentIndex].ModRevision {
			return errs.New(errs.KindInternal, "connector environment index changed or is corrupt")
		}
		if values[evidence.nameIndex] == nil ||
			string(values[evidence.nameIndex].Value) != connector.ID ||
			values[evidence.nameIndex].ModRevision != evidence.conditions[evidence.nameIndex].ModRevision {
			return errs.New(errs.KindInternal, "connector name index changed or is corrupt")
		}
		if values[evidence.credentials] == nil {
			return errs.New(
				errs.KindInternal,
				"connector encrypted credentials disappeared during deletion",
			)
		}
		credentials, err := connectorrecord.DecodeEncryptedCredentials(values[evidence.credentials].Value)
		if err != nil || credentials.ConnectorID != connector.ID ||
			values[evidence.credentials].ModRevision != evidence.conditions[evidence.credentials].ModRevision {
			clear(credentials.Ciphertext)
			return errs.New(
				errs.KindInternal,
				"connector encrypted credentials changed or are corrupt",
			)
		}
		clear(credentials.Ciphertext)
		if values[evidence.tombstone] != nil || values[evidence.intent] != nil {
			return errs.New(errs.KindResourceInUse, "connector deletion is already in progress")
		}
		if values[evidence.reference] != nil {
			if values[evidence.reference].Key != evidence.conditions[evidence.reference].Key ||
				string(values[evidence.reference].Value) != connector.EnvironmentID {
				return errs.New(errs.KindInternal, "connector reference index is corrupt")
			}
			return errs.New(
				errs.KindResourceInUse,
				"connector is referenced by an enabled backup policy",
			)
		}
		for index, value := range values[evidence.referenceStart:evidence.fenceStart] {
			if value != nil {
				return classifyConnectorReference(index, *value, connector.ID, connector.EnvironmentID)
			}
		}
		if conflict := evidence.fence.ClassifyConflict(values[evidence.fenceStart:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "connector deletion state changed")
	}
}

func loadConnectorTaskInitiationTenantAtRevision(
	ctx context.Context,
	store hierarchyStore,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	readRevision int64,
) (*etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		return nil, nil
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.TenantKey(project.Record.TenantID)}, Revision: readRevision,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 ||
		result.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	defer etcdstore.ClearValues(result.Values)
	tenant, err := hierarchyrecord.DecodeTenant(result.Values[0].Value)
	if err != nil || tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
