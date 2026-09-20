package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	tombstone DeletionTombstoneRecord,
	intent ConnectorRemovalIntent,
	task TaskRecord,
	marker IdempotencyMarker,
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
		taskKey(task.ID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		connectorrecord.RecordKey(connector.ID),
		connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
		connectorNameKey(connector.EnvironmentID, connector.Name),
		connectorrecord.CredentialValueKey(connector.ID),
		deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
		connectorRemovalIntentKey(task.ID),
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
	defer clearKeyValues(fixed.Values)
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
		ctx, repository.store, connector.ID, connector.EnvironmentID, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}

	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	intentValue, err := encodeConnectorRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	evidence := newConnectorDeletionEvidence(current, fixed, task, fence, referenceConditions)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: connectorRemovalIntentKey(task.ID), Value: intentValue},
		epochMutation,
	}
	taskTenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		project,
		fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		taskTenant,
		project,
		environment,
		TaskActorOperator,
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
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateConnectorDeletionEnvelope(
	current etcdstore.Versioned[connectorrecord.Record],
	tombstone DeletionTombstoneRecord,
	intent ConnectorRemovalIntent,
	task TaskRecord,
	marker IdempotencyMarker,
) error {
	if err := validateDeletionTombstone(tombstone); err != nil {
		return err
	}
	if err := validateConnectorRemovalIntent(intent); err != nil {
		return err
	}
	connector := current.Record.Connector
	if tombstone.TargetKind != DeletionTargetConnector || tombstone.TargetID != connector.ID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != connector.EnvironmentID || intent.ConnectorID != connector.ID ||
		intent.ConnectorRevision != current.Revision || !intent.CreatedAt.Equal(task.CreatedAt) ||
		task.Executor != TaskExecutorController || task.Type != TaskRemove || task.Target != connector.ID ||
		task.Status != TaskStatusPending || task.TimeoutSeconds != connectorDeletionTimeoutSeconds ||
		task.IdempotencyKey == "" || task.IdempotencyKey != marker.Locator.Key || len(task.Params) != 3 ||
		task.Params[TaskResourceKindParam] != TaskResourceConnector ||
		task.Params[TaskConnectorEnvironmentParam] != connector.EnvironmentID ||
		task.Params[TaskConnectorNameParam] != connector.Name {
		return errs.New(
			errs.KindValidationFailed,
			"connector deletion task, tombstone, and intent do not match",
		)
	}
	wantReplayTarget := IdempotencyReplayTarget{
		Kind: IdempotencyReplayTargetConnector,
		ID:   connector.ID,
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != connector.EnvironmentID || marker.Locator.Method != http.MethodDelete ||
		marker.Locator.Route != connectorDeletionRoute || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || marker.Response.Status != http.StatusAccepted ||
		!validTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() {
		return errs.New(
			errs.KindValidationFailed,
			"connector deletion marker does not match its task",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
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
		backupRecoveryPointConnectorPrefix + connectorID + "/",
		backupOrphanConnectorPrefix + connectorID + "/",
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
		expectedKey, err := backupRecoveryPointConnectorIndexKey(connectorID, recoveryPointID)
		if err != nil || value.Key != expectedKey {
			return errs.New(errs.KindInternal, "connector recovery point reference is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by a recovery point")
	case 2:
		recoveryPointID := string(value.Value)
		expectedKey, err := backupOrphanConnectorIndexKey(connectorID, recoveryPointID)
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
	fence            environmentMutationFenceEvidence
}

func newConnectorDeletionEvidence(
	current etcdstore.Versioned[connectorrecord.Record],
	dependencies *etcdstore.GetManyResult,
	task TaskRecord,
	fence environmentMutationFenceEvidence,
	referenceConditions []etcdstore.Condition,
) connectorDeletionEvidence {
	connector := current.Record.Connector
	evidence := connectorDeletionEvidence{
		task: 0, operation: 1, active: 2, queue: 3, primary: 4,
		environmentIndex: 5, nameIndex: 6, credentials: 7, tombstone: 8, intent: 9,
		reference: 10, referenceStart: 11, current: current, operationID: task.OperationID, fence: fence,
		conditions: []etcdstore.Condition{
			{Key: taskKey(task.ID)},
			{Key: taskOperationIndexKey(task.OperationID, task.ID)},
			{Key: taskActiveOperationKey(task.OperationID)},
			{Key: taskQueueKey(task.Executor, task.ID)},
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
			{Key: deletionTombstoneKey(string(DeletionTargetConnector), connector.ID)},
			{Key: connectorRemovalIntentKey(task.ID)},
			{Key: backuppolicy.BackupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID)},
		},
	}
	evidence.conditions = append(evidence.conditions, referenceConditions...)
	evidence.fenceStart = len(evidence.conditions)
	evidence.conditions = append(evidence.conditions, fence.transactionConditions()...)
	return evidence
}

func (evidence connectorDeletionEvidence) classifier() idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(evidence.conditions) {
			return errs.New(errs.KindInternal, "connector deletion compare evidence is incomplete")
		}
		if values[evidence.active] != nil {
			activeTaskID, err := decodeTaskReference(values[evidence.active].Value)
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
			return stateConflict("connector", connector.ID)
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
		if conflict := evidence.fence.classifyCAS(values[evidence.fenceStart:]); conflict != nil {
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
	defer clearKeyValues(result.Values)
	tenant, err := hierarchyrecord.DecodeTenant(result.Values[0].Value)
	if err != nil || tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
