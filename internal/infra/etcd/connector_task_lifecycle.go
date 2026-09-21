package etcd

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
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
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			connectorrecord.RecordKey(source.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), source.Target),
			connectorrecord.RemovalIntentKey(retry.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 3 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector retry evidence is incomplete")
	}
	if stored.Values[0] == nil || stored.Values[1] != nil || stored.Values[2] != nil {
		return connectorTaskChange{}, errs.New(errs.KindStateConflict, "connector is not available for deletion retry")
	}
	record, err := connectorrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != source.Target {
		return connectorTaskChange{}, connectorrecord.CorruptRecord()
	}
	connector := record.Connector
	if connector.EnvironmentID != source.Params[TaskConnectorEnvironmentParam] ||
		connector.Name != source.Params[TaskConnectorNameParam] {
		return connectorTaskChange{}, errs.New(
			errs.KindInternal,
			"connector deletion task target metadata is corrupt",
		)
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorrecord.CredentialValueKey(connector.ID),
			hierarchyrecord.EnvironmentKey(connector.EnvironmentID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), connector.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies == nil || dependencies.ReadRevision != revision || len(dependencies.Values) != 5 {
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
	credentials, err := connectorrecord.DecodeEncryptedCredentials(dependencies.Values[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are corrupt")
	}
	clear(credentials.Ciphertext)
	referenceConditions, err := requireConnectorReferencePrefixesEmpty(
		ctx, repository.store, connector.ID, connector.EnvironmentID, revision,
	)
	if err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies.Values[3] == nil {
		return connectorTaskChange{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	if dependencies.Values[4] != nil {
		return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "environment is being deleted")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(dependencies.Values[3].Value)
	if err != nil || environment.ID != connector.EnvironmentID {
		return connectorTaskChange{}, recordcodec.CorruptRecord()
	}
	parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.ProjectKey(environment.ProjectID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), environment.ProjectID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if parents == nil || parents.ReadRevision != revision || len(parents.Values) != 2 {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector owner evidence is incomplete")
	}
	if parents.Values[0] == nil {
		return connectorTaskChange{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if parents.Values[1] != nil {
		return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "project is being deleted")
	}
	project, err := hierarchyrecord.DecodeProject(parents.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return connectorTaskChange{}, recordcodec.CorruptRecord()
	}
	change := connectorTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: connectorrecord.RecordKey(connector.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)},
			{Key: connectorrecord.RemovalIntentKey(retry.ID)},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: connectorrecord.CredentialValueKey(connector.ID), ModRevision: dependencies.Values[2].ModRevision},
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: dependencies.Values[3].ModRevision},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.ID)},
			{Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), project.ID)},
		},
	}
	change.conditions = append(change.conditions, referenceConditions...)
	if project.TenantID != "" {
		tenantFence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     []string{deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID)},
			Revision: revision,
		})
		if err != nil {
			return connectorTaskChange{}, err
		}
		if tenantFence == nil || tenantFence.ReadRevision != revision || len(tenantFence.Values) != 1 {
			return connectorTaskChange{}, errs.New(errs.KindInternal, "connector tenant fence evidence is incomplete")
		}
		if tenantFence.Values[0] != nil {
			return connectorTaskChange{}, errs.New(errs.KindResourceInUse, "tenant is being deleted")
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
		})
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetConnector, TargetID: connector.ID,
		TargetRevision: stored.Values[0].ModRevision, TaskID: retry.ID,
		Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	intent, err := connectorrecord.NewRemovalIntent(
		retry.ID, connector.EnvironmentID, connector.ID, stored.Values[0].ModRevision, retry.CreatedAt,
	)
	if err != nil {
		return connectorTaskChange{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return connectorTaskChange{}, err
	}
	intentValue, err := connectorrecord.EncodeRemovalIntent(intent)
	if err != nil {
		clear(tombstoneValue)
		return connectorTaskChange{}, err
	}
	change.values = append(change.values, tombstoneValue, intentValue)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID),
			Value: tombstoneValue,
		},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: connectorrecord.RemovalIntentKey(retry.ID), Value: intentValue},
	)
	return change, nil
}

func (repository *TaskRepository) prepareConnectorTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (connectorTaskChange, error) {
	applies, err := taskOwnsConnectorRemoval(task)
	if err != nil || !applies {
		return connectorTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			connectorrecord.RecordKey(task.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), task.Target),
			connectorrecord.RemovalIntentKey(task.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 3 || stored.Values[0] == nil ||
		stored.Values[1] == nil || stored.Values[2] == nil {
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector deletion state is inconsistent")
	}
	record, err := connectorrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != task.Target {
		return connectorTaskChange{}, connectorrecord.CorruptRecord()
	}
	connector := record.Connector
	if connector.EnvironmentID != task.Params[TaskConnectorEnvironmentParam] ||
		connector.Name != task.Params[TaskConnectorNameParam] {
		return connectorTaskChange{}, errs.New(
			errs.KindInternal,
			"connector deletion task target metadata is corrupt",
		)
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetConnector || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return connectorTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector deletion tombstone does not match its task",
		)
	}
	intent, err := connectorrecord.DecodeRemovalIntent(stored.Values[2].Value)
	if err != nil || intent.TaskID != task.ID || intent.EnvironmentID != connector.EnvironmentID ||
		intent.ConnectorID != task.Target || intent.ConnectorRevision != stored.Values[0].ModRevision ||
		!intent.CreatedAt.Equal(task.CreatedAt) {
		return connectorTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector deletion intent does not match its task",
		)
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorrecord.CredentialValueKey(connector.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return connectorTaskChange{}, err
	}
	if dependencies == nil || dependencies.ReadRevision != revision || len(dependencies.Values) != 3 {
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
	credentials, err := connectorrecord.DecodeEncryptedCredentials(dependencies.Values[2].Value)
	if err != nil || credentials.ConnectorID != connector.ID {
		clear(credentials.Ciphertext)
		return connectorTaskChange{}, errs.New(errs.KindInternal, "connector credentials are corrupt")
	}
	clear(credentials.Ciphertext)
	referenceConditions := []etcdstore.Condition(nil)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		referenceConditions, err = requireConnectorReferencePrefixesEmpty(
			ctx, repository.store, connector.ID, connector.EnvironmentID, revision,
		)
		if err != nil {
			return connectorTaskChange{}, err
		}
	}
	change := connectorTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: connectorrecord.RecordKey(task.Target), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), task.Target),
				ModRevision: stored.Values[1].ModRevision,
			},
			{Key: connectorrecord.RemovalIntentKey(task.ID), ModRevision: stored.Values[2].ModRevision},
			{
				Key:         connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         connectorNameKey(connector.EnvironmentID, connector.Name),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: connectorrecord.CredentialValueKey(connector.ID), ModRevision: dependencies.Values[2].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), task.Target)},
			{Type: etcdstore.MutationDelete, Key: connectorrecord.RemovalIntentKey(task.ID)},
		},
	}
	change.conditions = append(change.conditions, referenceConditions...)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		change.mutations = append(change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: connectorEnvironmentKey(connector.EnvironmentID, connector.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: connectorNameKey(connector.EnvironmentID, connector.Name)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: connectorrecord.CredentialValueKey(connector.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: connectorrecord.RecordKey(connector.ID)},
		)
	}
	return change, nil
}

func (repository *TaskRepository) validateConnectorTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsConnectorRemoval(task)
	if err != nil || !applies {
		return err
	}
	environmentID := task.Params[TaskConnectorEnvironmentParam]
	name := task.Params[TaskConnectorNameParam]
	keys := []string{
		connectorrecord.RecordKey(task.Target),
		connectorEnvironmentKey(environmentID, task.Target),
		connectorNameKey(environmentID, name),
		connectorrecord.CredentialValueKey(task.Target),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), task.Target),
		connectorrecord.RemovalIntentKey(task.ID),
	}
	markerKey := ""
	if task.RetryOf == "" {
		markerKey, err = idempotencyrecord.IdempotencyMarkerKey(idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
			ScopeID:   environmentID,
			Method:    http.MethodDelete,
			Route:     connectorDeletionRoute,
			Key:       task.IdempotencyKey,
		})
		if err != nil {
			return err
		}
		replayTargetKey, keyErr := idempotencyrecord.IdempotencyReplayTargetKey(
			idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetConnector, ID: task.Target},
			http.MethodDelete,
			connectorDeletionRoute,
			task.IdempotencyKey,
		)
		if keyErr != nil {
			return keyErr
		}
		keys = append(keys, replayTargetKey)
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     keys,
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != len(keys) {
		return errs.New(errs.KindInternal, "connector deletion replay evidence is incomplete")
	}
	if stored.Values[4] != nil || stored.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "connector deletion replay found a live cleanup fence")
	}
	if task.RetryOf == "" {
		if stored.Values[6] == nil {
			return errs.New(errs.KindStateConflict, "connector deletion replay target is missing")
		}
		if err := idempotencyrecord.DecodeReplayTargetReference(stored.Values[6].Value, markerKey); err != nil {
			return errs.New(errs.KindStateConflict, "connector deletion replay target is corrupt")
		}
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if _, err := requireConnectorReferencePrefixesEmpty(
			ctx, repository.store, task.Target, environmentID, revision,
		); err != nil {
			return err
		}
		for index, value := range stored.Values[:4] {
			if value == nil {
				continue
			}
			if index == 2 {
				ownerID := string(value.Value)
				if recordcodec.ValidateID(ids.KindConnector, ownerID) == nil && ownerID != task.Target {
					continue
				}
			}
			return errs.New(errs.KindStateConflict, "completed connector deletion retained target state")
		}
		return nil
	}
	for _, value := range stored.Values[:4] {
		if value == nil {
			return errs.New(errs.KindStateConflict, "failed connector deletion lost target state")
		}
	}
	record, err := connectorrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Connector.ID != task.Target || record.Connector.EnvironmentID != environmentID ||
		record.Connector.Name != name || string(stored.Values[1].Value) != task.Target ||
		string(stored.Values[2].Value) != task.Target {
		return errs.New(errs.KindStateConflict, "failed connector deletion retained corrupt target state")
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(stored.Values[3].Value)
	if err != nil || credentials.ConnectorID != task.Target {
		clear(credentials.Ciphertext)
		return errs.New(errs.KindStateConflict, "failed connector deletion retained corrupt credentials")
	}
	clear(credentials.Ciphertext)
	return nil
}

func taskOwnsConnectorRemoval(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceConnector {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || task.TimeoutSeconds != connectorDeletionTimeoutSeconds ||
		len(task.Params) != 3 || ids.Validate(ids.KindConnector, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, task.Params[TaskConnectorEnvironmentParam]) != nil ||
		recordcodec.ValidateLabel("connector name", task.Params[TaskConnectorNameParam]) != nil {
		return false, errs.New(errs.KindInternal, "connector deletion task has invalid durable input")
	}
	return true, nil
}

func clearConnectorTaskChange(change connectorTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
