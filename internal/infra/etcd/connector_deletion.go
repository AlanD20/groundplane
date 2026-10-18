package etcd

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	connectormutations "github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectorDeletionRoute = "/connectors/{id}"

func (repository *ConnectorRepository) PublishConnectorRemoval(ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	input connectorremoval.Input, task workexecution.Task, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := connectormutations.ValidateConnectorHierarchy(ctx, environment, project, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := connectorrecord.ValidateConnectorVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	connector := current.Record.Connector
	ref, err := input.Reference()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if task.Validate() != nil || input.TaskID != task.ID || input.ConnectorID != connector.ID ||
		input.ConnectorRevision != current.Revision || input.EnvironmentID != connector.EnvironmentID ||
		input.ProjectID != project.Record.ID || input.TenantID != project.Record.TenantID ||
		task.Scope != (workexecution.Scope{TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID}) ||
		task.Action != workexecution.ActionRemove || task.Actor != workexecution.OperatorActor ||
		task.TargetID != connector.ID || task.TargetName != connector.Name || task.Intent != ref || task.ExpectedItems != 1 ||
		!task.CreatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Connector removal Task differs from captured input")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetConnector, ID: connector.ID}
	if idempotencyrecord.ValidateIdempotencyMarker(marker) != nil ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != connector.EnvironmentID || marker.Locator.Method != http.MethodDelete ||
		marker.Locator.Route != connectorDeletionRoute || marker.ReplayTarget == nil || *marker.ReplayTarget != target ||
		!idempotencyrecord.ValidTaskResponse(marker.Response, task.ID) || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(task.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Connector removal replay marker is invalid")
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	tombstoneKey := deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)
	keys := []string{connectorrecord.RecordKey(connector.ID), connectorrecord.CredentialValueKey(connector.ID),
		connectorrecord.ConnectorEnvironmentKey(connector.EnvironmentID, connector.ID),
		connectorrecord.ConnectorNameKey(connector.EnvironmentID, connector.Name), tombstoneKey}
	fence, fixed, err := repository.LoadConnectorMutationFence(ctx, environment, project, keys)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if fixed == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Connector publication read is missing")
	}
	defer etcdstore.ClearValues(fixed.Values)
	if len(fixed.Values) != len(keys)+2 || fixed.Values[0] == nil || fixed.Values[1] == nil ||
		fixed.Values[0].ModRevision != input.ConnectorRevision || fixed.Values[1].ModRevision != input.CredentialRevision ||
		fixed.Values[2] == nil || string(fixed.Values[2].Value) != connector.ID ||
		fixed.Values[3] == nil || string(fixed.Values[3].Value) != connector.ID || fixed.Values[4] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Connector or removal state changed; refresh before removing")
	}
	refs, err := connectormutations.RequireConnectorReferencesUnused(ctx, repository.store, connector.ID, connector.EnvironmentID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.ConnectorRemoval,
		TargetID: connector.ID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: connector.ID}}, Input: &ref, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	captureConditions, captureWrites, err := connectorremoval.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(captureWrites)
	tombstone, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetConnector, TargetID: connector.ID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstone)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions := append(work.Conditions, captureConditions...)
	conditions = append(conditions, refs...)
	conditions = append(conditions, fence.TransactionConditions()...)
	for index, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(fixed.Values[index])})
	}
	mutations := append(work.Mutations, captureWrites...)
	mutations = append(mutations, epoch, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstone})
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(ctx, repository.store, project, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*etcdstore.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Connector removal input, references or Environment changed; refresh before removing")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotencyrecord.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, plan)
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
