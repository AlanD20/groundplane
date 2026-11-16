package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareConnectorCheck returns captured input, exact Secret pins and resource
// authority for promotion of the existing accepted Task.
func (repository *ConnectorRepository) PrepareConnectorCheck(ctx context.Context,
	source ConnectorCheckSource, input connectorchecks.Record, task workexecution.Task,
) (workexecution.Item, workqueue.Change, error) {
	if err := validateCapturedConnectorCheck(source, input, task); err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	item := workexecution.Item{
		ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.ConnectorCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: task.Deadline,
	}
	inputConditions, inputMutations, err := connectorchecks.PreparePublication(input)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	conditions := append([]keyvalue.Condition(nil), source.conditions...)
	conditions = append(conditions, inputConditions...)
	mutations := inputMutations
	keep := false
	defer func() {
		if !keep {
			keyvalue.ClearMutationValues(mutations)
		}
	}()
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
				return workexecution.Item{}, workqueue.Change{}, errs.New(
					errs.KindStateConflict,
					"connector check requires conflicting Secret versions",
				)
			}
			continue
		}
		pins[pin.SecretID] = pin
		value, err := tasksecretpinrecord.Encode(pin)
		if err != nil {
			return workexecution.Item{}, workqueue.Change{}, err
		}
		key := tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
		conditions = append(conditions, keyvalue.Condition{Key: key})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	}
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
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
		return workexecution.Item{}, workqueue.Change{}, err
	}
	conditions = append(conditions, ancestry.Conditions()...)
	mutations = append(mutations, ancestry.Mutations()...)
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		source.project,
		source.fence.ReadRevision(),
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		tenant,
		source.project,
		source.environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	joined, err := workqueue.JoinChanges(
		workqueue.Change{Conditions: conditions, Mutations: mutations},
		workqueue.Change{Conditions: initiation.fences},
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	keep = true
	return item, joined, nil
}

func validateCapturedConnectorCheck(source ConnectorCheckSource, input connectorchecks.Record,
	task workexecution.Task,
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
		input.CreatedAt.Before(task.CreatedAt) || input.CreatedAt.After(task.UpdatedAt) {
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
	return nil
}
