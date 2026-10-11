package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateConnectorCheck atomically publishes native work, captured private input,
// cleanup ownership and exact Secret-pin activation. A draft remains unsaved.
func (repository *ConnectorRepository) CreateConnectorCheck(ctx context.Context,
	source ConnectorCheckSource, input connectorchecks.Record, task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateConnectorCheckPublication(source, input, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		source.project,
		source.fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		tenant,
		source.project,
		source.environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	bySecret := make(map[string]tasksecretpinrecord.Record, len(input.SecretSources))
	for _, secret := range input.SecretSources {
		if err := addRecoverySecretPin(task.OperationID, secret.Pin, bySecret); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	if len(bySecret) > 0 {
		task.Configuration = &taskconfiguration.TaskConfiguration{}
	}
	task, prepared, err := prepareRecoverySecretPinSet(ctx, repository.store, task, bySecret)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	result, err := repository.publishConnectorCheck(ctx, source, input, task, marker, prepared, initiation)
	if err != nil {
		if !prepared.IsZero() {
			err = finishRecoverySecretPreparation(ctx, repository.store, task, err)
		}
		return IdempotencyTransactionResult{}, err
	}
	// A competing idempotency request may have won with a different Task ID.
	// Only this unpublished preparation is abandoned; the winner is preserved.
	if result.kind != idempotencyTransactionApplied && !prepared.IsZero() {
		if err := finishRecoverySecretPreparation(ctx, repository.store, task, nil); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	return result, nil
}

func (repository *ConnectorRepository) publishConnectorCheck(ctx context.Context,
	source ConnectorCheckSource, input connectorchecks.Record, task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker, prepared tasksecretpins.Prepared, initiation TaskInitiation,
) (IdempotencyTransactionResult, error) {
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []keyvalue.Condition{{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{
			Key: taskjournal.TaskActiveOperationKey(task.OperationID),
		}, {Key: taskjournal.TaskQueueKey(task.Executor, task.ID)}}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
	}
	inputConditions, inputMutations, err := connectorchecks.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(inputMutations)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions = append(conditions, source.conditions...)
	conditions = append(conditions, inputConditions...)
	mutations = append(mutations, inputMutations...)
	if !input.Draft {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: connectorchecks.LatestKey(task.Target), Value: []byte(task.ID)})
	}
	mutations = append(mutations, epoch)
	ancestry, err := hierarchydeletion.BindMutationEpochs(
		ctx,
		repository.store,
		source.fence.ReadRevision(),
		hierarchydeletion.MutationScope{
			TenantID:  source.project.Record.TenantID,
			ProjectID: source.project.Record.ID,
		},
		nil,
		nil,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	conditions = append(conditions, ancestry.Conditions()...)
	mutations = append(mutations, ancestry.Mutations()...)
	activation, err := recoverySecretPinActivation(prepared)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(activation.mutations)
	// Capture the base condition list before binding activation: classifiers
	// receive only their own evidence, not a slice closed over its later append.
	baseConditions := append([]keyvalue.Condition(nil), conditions...)
	classify := connectorCheckPublicationConflict(baseConditions)
	conditions, mutations, classify = bindRecoverySecretPinPublication(activation, conditions, mutations, classify)
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func connectorCheckPublicationConflict(conditions []keyvalue.Condition) idempotencyPlanClassifier {
	return func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "connector check publication evidence is incomplete")
		}
		for index, condition := range conditions {
			if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
				return errs.New(
					errs.KindStateConflict,
					"connector check source, credential selection or operation changed; submit a new check",
				)
			}
		}
		return nil
	}
}

func validateConnectorCheckPublication(source ConnectorCheckSource, input connectorchecks.Record,
	task TaskRecord, marker idempotencyrecord.IdempotencyMarker,
) error {
	if source.fence.ReadRevision() <= 0 || source.fence.EnvironmentID() != source.environment.Record.ID ||
		input.Validate() != nil || input.Phase != connectorchecks.Ready || input.Credentials == nil ||
		input.ConnectorRevision != source.revision || input.Draft != source.draft || task.Owner.EnvironmentID != source.environment.Record.ID ||
		task.Owner.ProjectID != source.project.Record.ID || task.Owner.TenantID != source.project.Record.TenantID ||
		task.ID != input.Owner.TaskID || task.PlanID != input.Owner.PlanID || task.OperationID != input.Owner.OperationID ||
		task.Target != source.connector.Connector.ID || task.Params[taskjournal.TaskConnectorCheckInputParam] != input.ConfigurationSHA256 ||
		!connectorCheckTask(
			task,
		) || task.Executor != taskjournal.TaskExecutorController || task.Actor != taskjournal.TaskActorOperator ||
		task.Status != taskjournal.TaskStatusPending || task.RetryOf != "" || task.Configuration != nil ||
		len(
			task.Steps,
		) != 0 || len(task.Materializations) != 0 || len(task.Params) != 2 || task.RenderGeneration != 1 ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Owner.EnvironmentID || marker.Locator.Key != task.IdempotencyKey ||
		!marker.CreatedAt.Equal(
			task.CreatedAt,
		) || !marker.UpdatedAt.Equal(marker.CreatedAt) || !input.CreatedAt.Equal(task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "connector check publication differs from captured authority")
	}
	sources, values := source.SecretValues(task.OperationID)
	for index := range values {
		clear(values[index].Ciphertext)
	}
	expected, err := connectorchecks.NewRecord(
		input.Owner,
		source.connector,
		source.revision,
		source.draft,
		*input.Credentials,
		sources,
		input.CreatedAt,
	)
	if err != nil {
		return err
	}
	defer expected.Clear()
	planHash, err := connectorchecks.PlanHash(expected)
	if err != nil || expected.ConfigurationSHA256 != input.ConfigurationSHA256 || planHash != task.PlanHash {
		return errs.New(errs.KindValidationFailed, "connector check captured sources do not match its Task")
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}
