package etcd

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	TaskConnectorEnvironmentParam = "connector_environment_id"
	TaskConnectorNameParam        = "connector_name"
)

type connectorTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareConnectorTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (connectorTaskChange, error) {
	applies, err := taskOwnsConnectorRemoval(source)
	if err != nil || !applies {
		return connectorTaskChange{}, err
	}
	retryApplies, err := taskOwnsConnectorRemoval(retry)
	if err != nil {
		return connectorTaskChange{}, err
	}
	if !retryApplies || retry.Type != source.Type || retry.Target != source.Target ||
		retry.TimeoutSeconds != source.TimeoutSeconds ||
		retry.Params[TaskConnectorEnvironmentParam] != source.Params[TaskConnectorEnvironmentParam] ||
		retry.Params[TaskConnectorNameParam] != source.Params[TaskConnectorNameParam] {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			connectorRecordKey(source.Target),
			deletionTombstoneKey(string(DeletionTargetConnector), source.Target),
			connectorRemovalIntentKey(retry.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 3 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector retry evidence is incomplete")
	}
	if stored.Values[0] == nil || stored.Values[1] != nil || stored.Values[2] != nil {
		return connectorTaskChange{}, errs.New(errs.KindStateConflict, "connector is not available for deletion retry")
	}
	record, err := decodeConnectorRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != source.Target {
		return connectorTaskChange{}, corruptConnectorRecord()
	}
	connector := record.Connector
	if connector.EnvironmentID != source.Params[TaskConnectorEnvironmentParam] ||
		connector.Name != source.Params[TaskConnectorNameParam] {
		return connectorTaskChange{}, errs.New(
			errs.KindInternal,
			"connector deletion task target metadata is corrupt",
		)
	}
	dependencies, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorCredentialValueKey(connector.ID),
			backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID),
			environmentKey(connector.EnvironmentID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), connector.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies == nil || len(dependencies.Values) != 6 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector retry evidence is incomplete")
	}
	if dependencies.Values[0] == nil || string(dependencies.Values[0].Value) != connector.ID {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector environment index is missing or corrupt")
	}
	if dependencies.Values[1] == nil || string(dependencies.Values[1].Value) != connector.ID {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector name index is missing or corrupt")
	}
	if dependencies.Values[2] == nil {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are missing")
	}
	credentials, err := decodeConnectorEncryptedCredentials(dependencies.Values[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are corrupt")
	}
	clear(credentials.Ciphertext)
	if err := requireConnectorReferencePrefixEmpty(
		ctx, repository.store, connector.ID, connector.EnvironmentID, revision,
	); err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies.Values[3] != nil {
		return connectorTaskChange{}, errs.New(
			errs.KindResourceInUse,
			"connector is referenced by an enabled backup policy",
		)
	}
	if dependencies.Values[4] == nil {
		return connectorTaskChange{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	if dependencies.Values[5] != nil {
		return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "environment is being deleted")
	}
	environment, err := decodeEnvironment(dependencies.Values[4].Value)
	if err != nil || environment.ID != connector.EnvironmentID {
		return connectorTaskChange{}, corruptRecord()
	}
	parents, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			projectKey(environment.ProjectID),
			deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if parents == nil || len(parents.Values) != 2 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector owner evidence is incomplete")
	}
	if parents.Values[0] == nil {
		return connectorTaskChange{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if parents.Values[1] != nil {
		return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "project is being deleted")
	}
	project, err := decodeProject(parents.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return connectorTaskChange{}, corruptRecord()
	}
	change := connectorTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: connectorRecordKey(connector.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetConnector), connector.ID)},
			{Key: connectorRemovalIntentKey(retry.ID)},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: connectorCredentialValueKey(connector.ID), ModRevision: dependencies.Values[2].ModRevision},
			{Key: backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID)},
			{Key: environmentKey(environment.ID), ModRevision: dependencies.Values[4].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID)},
			{Key: projectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetProject), project.ID)},
		},
	}
	if project.TenantID != "" {
		tenantFence, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys:     []string{deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)},
			Revision: revision,
		})
		if err != nil {
			return connectorTaskChange{}, err
		}
		if tenantFence == nil || len(tenantFence.Values) != 1 {
			return connectorTaskChange{}, errs.New(errs.KindInternal, "connector tenant fence evidence is incomplete")
		}
		if tenantFence.Values[0] != nil {
			return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "tenant is being deleted")
		}
		change.conditions = append(change.conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID),
		})
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetConnector, TargetID: connector.ID,
		TargetRevision: stored.Values[0].ModRevision, TaskID: retry.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	intent, err := NewConnectorRemovalIntent(
		retry.ID, connector.EnvironmentID, connector.ID, stored.Values[0].ModRevision, retry.CreatedAt,
	)
	if err != nil {
		return connectorTaskChange{}, err
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return connectorTaskChange{}, err
	}
	intentValue, err := encodeConnectorRemovalIntent(intent)
	if err != nil {
		clear(tombstoneValue)
		return connectorTaskChange{}, err
	}
	change.values = append(change.values, tombstoneValue, intentValue)
	change.mutations = append(change.mutations,
		Mutation{
			Type:  MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
			Value: tombstoneValue,
		},
		Mutation{Type: MutationPut, Key: connectorRemovalIntentKey(retry.ID), Value: intentValue},
	)
	return change, nil
}

func (repository *TaskRepository) prepareConnectorTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (connectorTaskChange, error) {
	applies, err := taskOwnsConnectorRemoval(task)
	if err != nil || !applies {
		return connectorTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			connectorRecordKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetConnector), task.Target),
			connectorRemovalIntentKey(task.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 3 || stored.Values[0] == nil ||
		stored.Values[1] == nil || stored.Values[2] == nil {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector deletion state is inconsistent")
	}
	record, err := decodeConnectorRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != task.Target {
		return connectorTaskChange{}, corruptConnectorRecord()
	}
	connector := record.Connector
	if connector.EnvironmentID != task.Params[TaskConnectorEnvironmentParam] ||
		connector.Name != task.Params[TaskConnectorNameParam] {
		return connectorTaskChange{}, errs.New(
			errs.KindInternal,
			"connector deletion task target metadata is corrupt",
		)
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetConnector || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing {
		return connectorTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector deletion tombstone does not match its task",
		)
	}
	intent, err := decodeConnectorRemovalIntent(stored.Values[2].Value)
	if err != nil || intent.TaskID != task.ID || intent.EnvironmentID != connector.EnvironmentID ||
		intent.ConnectorID != task.Target || intent.ConnectorRevision != stored.Values[0].ModRevision ||
		!intent.CreatedAt.Equal(task.CreatedAt) {
		return connectorTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector deletion intent does not match its task",
		)
	}
	dependencies, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorCredentialValueKey(connector.ID),
			backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies == nil || len(dependencies.Values) != 4 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector deletion evidence is incomplete")
	}
	if dependencies.Values[0] == nil || string(dependencies.Values[0].Value) != connector.ID {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector environment index is missing or corrupt")
	}
	if dependencies.Values[1] == nil || string(dependencies.Values[1].Value) != connector.ID {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector name index is missing or corrupt")
	}
	if dependencies.Values[2] == nil {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are missing")
	}
	credentials, err := decodeConnectorEncryptedCredentials(dependencies.Values[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are corrupt")
	}
	clear(credentials.Ciphertext)
	if err := requireConnectorReferencePrefixEmpty(
		ctx, repository.store, connector.ID, connector.EnvironmentID, revision,
	); err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies.Values[3] != nil {
		return connectorTaskChange{}, errs.New(
			errs.KindResourceInUse,
			"connector is referenced by an enabled backup policy",
		)
	}
	change := connectorTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: connectorRecordKey(task.Target), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetConnector), task.Target),
				ModRevision: stored.Values[1].ModRevision,
			},
			{Key: connectorRemovalIntentKey(task.ID), ModRevision: stored.Values[2].ModRevision},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: connectorCredentialValueKey(connector.ID), ModRevision: dependencies.Values[2].ModRevision},
			{Key: backupPolicyConnectorReferenceKey(connector.ID, connector.EnvironmentID)},
		},
		mutations: []Mutation{
			{Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetConnector), task.Target)},
			{Type: MutationDelete, Key: connectorRemovalIntentKey(task.ID)},
		},
	}
	if terminalStatus == TaskStatusCompleted {
		change.mutations = append(change.mutations,
			Mutation{Type: MutationDelete, Key: connectorEnvironmentKey(connector.EnvironmentID, connector.ID)},
			Mutation{Type: MutationDelete, Key: connectorNameKey(connector.EnvironmentID, connector.Name)},
			Mutation{Type: MutationDelete, Key: connectorCredentialValueKey(connector.ID)},
			Mutation{Type: MutationDelete, Key: connectorRecordKey(connector.ID)},
		)
	}
	return change, nil
}

func (repository *TaskRepository) validateConnectorTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsConnectorRemoval(task)
	if err != nil || !applies {
		return err
	}
	environmentID := task.Params[TaskConnectorEnvironmentParam]
	name := task.Params[TaskConnectorNameParam]
	keys := []string{
		connectorRecordKey(task.Target),
		connectorEnvironmentKey(environmentID, task.Target),
		connectorNameKey(environmentID, name),
		connectorCredentialValueKey(task.Target),
		deletionTombstoneKey(string(DeletionTargetConnector), task.Target),
		connectorRemovalIntentKey(task.ID),
	}
	markerKey := ""
	if task.RetryOf == "" {
		markerKey, err = idempotencyMarkerKey(IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment,
			ScopeID:   environmentID,
			Method:    http.MethodDelete,
			Route:     connectorDeletionRoute,
			Key:       task.IdempotencyKey,
		})
		if err != nil {
			return err
		}
		replayTargetKey, keyErr := idempotencyReplayTargetKey(
			IdempotencyReplayTarget{Kind: IdempotencyReplayTargetConnector, ID: task.Target},
			http.MethodDelete,
			connectorDeletionRoute,
			task.IdempotencyKey,
		)
		if keyErr != nil {
			return keyErr
		}
		keys = append(keys, replayTargetKey)
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     keys,
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != len(keys) {
		return errs.New(errs.KindInternal, "connector deletion replay evidence is incomplete")
	}
	if stored.Values[4] != nil || stored.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "connector deletion replay found a live cleanup fence")
	}
	if task.RetryOf == "" {
		if stored.Values[6] == nil {
			return errs.New(errs.KindStateConflict, "connector deletion replay target is missing")
		}
		if err := decodeReplayTargetReference(stored.Values[6].Value, markerKey); err != nil {
			return errs.New(errs.KindStateConflict, "connector deletion replay target is corrupt")
		}
	}
	if err := requireConnectorReferencePrefixEmpty(
		ctx, repository.store, task.Target, environmentID, revision,
	); err != nil {
		return err
	}
	if terminalStatus == TaskStatusCompleted {
		for _, value := range stored.Values[:4] {
			if value != nil {
				return errs.New(errs.KindStateConflict, "completed connector deletion retained target state")
			}
		}
		return nil
	}
	for _, value := range stored.Values[:4] {
		if value == nil {
			return errs.New(errs.KindStateConflict, "failed connector deletion lost target state")
		}
	}
	record, err := decodeConnectorRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != task.Target || record.Connector.EnvironmentID != environmentID ||
		record.Connector.Name != name || string(stored.Values[1].Value) != task.Target ||
		string(stored.Values[2].Value) != task.Target {
		return errs.New(errs.KindStateConflict, "failed connector deletion retained corrupt target state")
	}
	credentials, err := decodeConnectorEncryptedCredentials(stored.Values[3].Value)
	if err != nil || credentials.ConnectorID != task.Target {
		clear(credentials.Ciphertext)
		return errs.New(errs.KindStateConflict, "failed connector deletion retained corrupt credentials")
	}
	clear(credentials.Ciphertext)
	return nil
}

func taskOwnsConnectorRemoval(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceConnector {
		return false, nil
	}
	if task.Type != TaskRemove || task.TimeoutSeconds != connectorDeletionTimeoutSeconds ||
		len(task.Params) != 3 || ids.Validate(ids.KindConnector, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, task.Params[TaskConnectorEnvironmentParam]) != nil ||
		validateLabel("connector name", task.Params[TaskConnectorNameParam]) != nil {
		return false, errs.New(errs.KindInternal, "connector deletion task has invalid durable input")
	}
	return true, nil
}

func clearConnectorTaskChange(change connectorTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
