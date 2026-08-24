package etcd

import (
	"context"
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
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ConnectorRecord],
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
		connectorRecordKey(connector.ID),
		connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
		connectorNameKey(connector.EnvironmentID, connector.Name),
		connectorCredentialValueKey(connector.ID),
		deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
		connectorRemovalIntentKey(task.ID),
		backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID),
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
	credentials, err := decodeConnectorEncryptedCredentials(dependencies[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return IdempotencyTransactionResult{}, corruptConnectorRecord()
	}
	clear(credentials.Ciphertext)
	if err := requireConnectorReferencePrefixEmpty(
		ctx, repository.store, connector.ID, connector.EnvironmentID, fence.readAtRevision(),
	); err != nil {
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

	evidence := newConnectorDeletionEvidence(current, fixed, task, fence)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{
			Type:  MutationPut,
			Key:   taskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
			Value: tombstoneValue,
		},
		{Type: MutationPut, Key: connectorRemovalIntentKey(task.ID), Value: intentValue},
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
	current Versioned[ConnectorRecord],
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
		*marker.ReplayTarget != wantReplayTarget || !validTaskResponse(marker.Response, task.ID) ||
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

func requireConnectorReferencePrefixEmpty(
	ctx context.Context,
	store hierarchyStore,
	connectorID string,
	environmentID string,
	revision int64,
) error {
	result, err := store.Range(ctx, RangeRequest{
		Prefix: backupPolicyConnectorReferencePrefix(connectorID), Limit: 2, Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil || result.ReadRevision != revision {
		return errs.New(errs.KindInternal, "connector reference prefix read is incomplete")
	}
	if result.More || len(result.Values) > 1 {
		return errs.New(
			errs.KindInternal,
			"connector reference prefix contains conflicting records",
		)
	}
	if len(result.Values) == 0 {
		return nil
	}
	expectedKey := backupPolicyConnectorReferenceKey(connectorID, environmentID)
	if result.Values[0].Key != expectedKey || string(result.Values[0].Value) != environmentID {
		return errs.New(errs.KindInternal, "connector reference prefix is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "connector is referenced by an enabled backup policy")
}

type connectorDeletionEvidence struct {
	conditions       []Condition
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
	current          Versioned[ConnectorRecord]
	operationID      string
	fence            environmentMutationFenceEvidence
}

func newConnectorDeletionEvidence(
	current Versioned[ConnectorRecord],
	dependencies *GetManyResult,
	task TaskRecord,
	fence environmentMutationFenceEvidence,
) connectorDeletionEvidence {
	connector := current.Record.Connector
	evidence := connectorDeletionEvidence{
		task: 0, operation: 1, active: 2, queue: 3, primary: 4,
		environmentIndex: 5, nameIndex: 6, credentials: 7, tombstone: 8, intent: 9,
		reference: 10, current: current, operationID: task.OperationID, fence: fence,
		conditions: []Condition{
			{Key: taskKey(task.ID)},
			{Key: taskOperationIndexKey(task.OperationID, task.ID)},
			{Key: taskActiveOperationKey(task.OperationID)},
			{Key: taskQueueKey(task.Executor, task.ID)},
			{Key: connectorRecordKey(connector.ID), ModRevision: current.Revision},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[5].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[6].ModRevision,
			},
			{
				Key:         connectorCredentialValueKey(connector.ID),
				ModRevision: dependencies.Values[7].ModRevision,
			},
			{Key: deletionTombstoneKey(string(DeletionTargetConnector), connector.ID)},
			{Key: connectorRemovalIntentKey(task.ID)},
			{Key: backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID)},
		},
	}
	evidence.conditions = append(evidence.conditions, fence.transactionConditions()...)
	return evidence
}

func (evidence connectorDeletionEvidence) classifier() idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
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
		credentials, err := decodeConnectorEncryptedCredentials(values[evidence.credentials].Value)
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
		if conflict := evidence.fence.classifyCAS(values[11:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "connector deletion state changed")
	}
}

func loadConnectorTaskInitiationTenantAtRevision(
	ctx context.Context,
	store hierarchyStore,
	project Versioned[ProjectRecord],
	readRevision int64,
) (*Versioned[TenantRecord], error) {
	if project.Record.Kind == ProjectKindBacking {
		return nil, nil
	}
	result, err := store.GetMany(ctx, GetManyRequest{
		Keys: []string{tenantKey(project.Record.TenantID)}, Revision: readRevision,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 ||
		result.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	defer clearKeyValues(result.Values)
	tenant, err := decodeTenant(result.Values[0].Value)
	if err != nil || tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &Versioned[TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
