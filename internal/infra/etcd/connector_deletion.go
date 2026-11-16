package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	connectormutations "github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectorDeletionRoute = "/connectors/{id}"

func (repository *ConnectorRepository) PrepareConnectorRemovalAuthority(ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	input connectorremoval.Input, task workexecution.Task,
) (workqueue.Change, workexecution.Item, error) {
	if err := connectormutations.ValidateConnectorHierarchy(ctx, environment, project, current.Record); err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if err := connectorrecord.ValidateConnectorVersion(current); err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	connector := current.Record.Connector
	ref, err := input.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if task.Validate() != nil || input.TaskID != task.ID || input.ConnectorID != connector.ID ||
		input.ConnectorRevision != current.Revision || input.EnvironmentID != connector.EnvironmentID ||
		input.ProjectID != project.Record.ID || input.TenantID != project.Record.TenantID ||
		task.Scope != (workexecution.Scope{TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID}) ||
		task.Action != workexecution.ActionRemove || task.Actor != workexecution.OperatorActor ||
		task.TargetID != connector.ID || task.Intent != ref || task.ExpectedItems != 1 ||
		task.CreatedAt.After(input.CreatedAt) || !task.UpdatedAt.Equal(input.CreatedAt) {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindValidationFailed,
			"Connector removal Task differs from captured input",
		)
	}
	tombstoneKey := deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)
	keys := []string{connectorrecord.RecordKey(connector.ID), connectorrecord.CredentialValueKey(connector.ID),
		connectorrecord.ConnectorEnvironmentKey(connector.EnvironmentID, connector.ID),
		connectorrecord.ConnectorNameKey(connector.EnvironmentID, connector.Name), tombstoneKey}
	fence, fixed, err := repository.LoadConnectorMutationFence(ctx, environment, project, keys)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if fixed == nil {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindInternal,
			"Connector publication read is missing",
		)
	}
	defer etcdstore.ClearValues(fixed.Values)
	if len(fixed.Values) != len(keys)+2 || fixed.Values[0] == nil || fixed.Values[1] == nil ||
		fixed.Values[0].ModRevision != input.ConnectorRevision || fixed.Values[1].ModRevision != input.CredentialRevision ||
		fixed.Values[2] == nil || string(fixed.Values[2].Value) != connector.ID ||
		fixed.Values[3] == nil || string(fixed.Values[3].Value) != connector.ID || fixed.Values[4] != nil {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindStateConflict,
			"Connector or removal state changed; refresh before removing",
		)
	}
	refs, err := connectormutations.RequireConnectorReferencesUnused(
		ctx,
		repository.store,
		connector.ID,
		connector.EnvironmentID,
		fence.ReadRevision(),
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.ConnectorRemoval,
		TargetID: connector.ID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: connector.ID}}, Input: &ref, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: input.CreatedAt.Add(30 * time.Second)}
	if item.Deadline.After(task.Deadline) {
		item.Deadline = task.Deadline
	}
	captureConditions, captureWrites, err := connectorremoval.PreparePublication(input)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer etcdstore.ClearMutationValues(captureWrites)
	tombstone, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetConnector, TargetID: connector.ID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt})
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer clear(tombstone)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer clear(epoch.Value)
	conditions := append([]etcdstore.Condition(nil), captureConditions...)
	conditions = append(conditions, refs...)
	conditions = append(conditions, fence.TransactionConditions()...)
	for index, key := range keys {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(fixed.Values[index])},
		)
	}
	mutations := append([]etcdstore.Mutation(nil), captureWrites...)
	mutations = append(
		mutations,
		epoch,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstone},
	)
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(ctx, repository.store, project, fence.ReadRevision())
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*etcdstore.KeyValue) error {
		return errs.New(
			errs.KindStateConflict,
			"Connector removal input, references or Environment changed; refresh before removing",
		)
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	_ = classify

	return workqueue.Change{Conditions: conditions, Mutations: etcdstore.CloneMutations(mutations)}, item, nil
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
	if result == nil ||
		result.ReadRevision != readRevision ||
		len(result.Values) != 1 ||
		result.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	defer etcdstore.ClearValues(result.Values)
	tenant, err := hierarchyrecord.DecodeTenant(result.Values[0].Value)
	if err != nil ||
		tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
