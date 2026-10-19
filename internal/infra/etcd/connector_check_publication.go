package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateConnectorCheck publishes the visible Task, one work item, captured input
// and at most two exact Secret pins atomically. There is no old Task publication.
func (repository *ConnectorRepository) CreateConnectorCheck(ctx context.Context,
	source ConnectorCheckSource, input connectorchecks.Record, task workexecution.Task,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateConnectorCheckPublication(source, input, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	item := workexecution.Item{
		ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.ConnectorCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
	}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	inputConditions, inputMutations, err := connectorchecks.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(inputMutations)
	conditions := append(work.Conditions, source.conditions...)
	conditions = append(conditions, inputConditions...)
	mutations := append(work.Mutations, inputMutations...)
	if !input.Draft {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: connectorchecks.LatestKey(task.TargetID), Value: []byte(task.ID)})
	}
	// The two credentials may reference the same Secret. All source metadata,
	// ciphertext revisions and deletion fences are already in source.conditions.
	pins := make(map[string]tasksecretpinrecord.Record, len(input.SecretSources))
	for _, source := range input.SecretSources {
		pin := source.Pin
		if previous, present := pins[pin.SecretID]; present {
			if previous != pin {
				return IdempotencyTransactionResult{}, errs.New(
					errs.KindStateConflict,
					"connector check requires conflicting Secret versions",
				)
			}
			continue
		}
		pins[pin.SecretID] = pin
		value, err := tasksecretpinrecord.Encode(pin)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		key := tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
		conditions = append(conditions, keyvalue.Condition{Key: key})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	}
	defer keyvalue.ClearMutationValues(mutations)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
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
	conditions, classify, err := prepareTaskInitiationFences(
		initiation,
		conditions,
		connectorCheckPublicationConflict(conditions),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotencyrecord.IdempotencyMarkerTask,
		conditions,
		mutations,
		classify,
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

func connectorCheckPublicationConflict(conditions []keyvalue.Condition) idempotencyPlanClassifier {
	return func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "connector check publication evidence is incomplete")
		}
		for index, condition := range conditions {
			if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
				return errs.New(
					errs.KindStateConflict,
					"connector check source, credentials or operation changed; submit a new check",
				)
			}
		}
		return nil
	}
}

func validateConnectorCheckPublication(source ConnectorCheckSource, input connectorchecks.Record,
	task workexecution.Task, marker idempotencyrecord.IdempotencyMarker,
) error {
	if task.Validate() != nil || source.fence.ReadRevision() <= 0 ||
		source.fence.EnvironmentID() != source.environment.Record.ID ||
		input.Validate() != nil ||
		input.Phase != connectorchecks.Ready ||
		input.Credentials == nil ||
		input.ConnectorRevision != source.revision ||
		input.Draft != source.draft ||
		task.Scope.EnvironmentID != source.environment.Record.ID ||
		task.Scope.ProjectID != source.project.Record.ID ||
		task.Scope.TenantID != source.project.Record.TenantID ||
		task.ID != input.Owner.TaskID ||
		task.Intent.ID != input.Owner.PlanID ||
		task.TargetID != source.connector.Connector.ID ||
		task.Actor != workexecution.OperatorActor ||
		task.Action != workexecution.ActionCheck ||
		task.ExpectedItems != 1 ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Scope.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!input.CreatedAt.Equal(task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "connector check publication differs from captured authority")
	}
	sources, values := source.SecretValues(input.Owner.OperationID)
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
	digest, err := connectorchecks.PlanHash(expected)
	if err != nil || expected.ConfigurationSHA256 != input.ConfigurationSHA256 || digest != task.Intent.SHA256 {
		return errs.New(errs.KindValidationFailed, "connector check captured sources do not match its work")
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}
