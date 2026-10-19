package etcd

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptauthoring"
	scriptmutations "github.com/AlanD20/groundplane/internal/infra/etcd/scriptmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishScriptRemoval atomically fences one Script and publishes the
// Controller Task that owns finalization. The Script remains visible until
// successful acknowledgement.
func (repository *ScriptRepository) PublishScriptRemoval(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	input scriptremoval.Input,
	task workexecution.Task,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := scriptmutations.ValidateScriptHierarchy(ctx, environment, project, target, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := scriptmutations.ValidateScriptVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.ActiveReferences != 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse, "active Script executions fence deletion",
		)
	}
	scriptID := current.Record.Desired.ID
	active, err := scriptrecord.ReadActiveScriptSet(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		current.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if active.Record.GenerationID != current.Record.ScriptSetGeneration {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
	ref, err := input.Reference()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if task.Validate() != nil || task.Action != workexecution.ActionRemove ||
		task.Actor != workexecution.OperatorActor ||
		task.TargetID != scriptID ||
		task.TargetName != current.Record.Desired.Slug ||
		task.ExpectedItems != 1 ||
		task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) ||
		input.TaskID != task.ID ||
		input.ScriptID != scriptID ||
		input.ScriptRevision != current.Revision ||
		input.EnvironmentID != environment.Record.ID ||
		input.ProjectID != project.Record.ID ||
		input.TenantID != project.Record.TenantID ||
		input.ServiceID != target.Record.Desired.ID ||
		input.ScriptSetGeneration != current.Record.ScriptSetGeneration ||
		task.Intent != ref ||
		!task.CreatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Script removal differs from its captured input",
		)
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetScript, TargetID: scriptID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetScript,
		ID:   scriptID,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != http.MethodDelete || marker.Locator.Route != "/scripts/{id}" ||
		!idempotencyrecord.ValidTaskResponse(marker.Response, task.ID) ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID ||
		marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Script deletion marker does not match its Task",
		)
	}

	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetOwnerKey(current.Record.EnvironmentID, active.Record.GenerationID, scriptID),
			scriptrecord.ScriptSetSlugKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				current.Record.Desired.Slug,
			),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes != nil {
		defer etcdstore.ClearValues(indexes.Values)
	}
	if indexes == nil || indexes.ReadRevision != current.ReadRevision || len(indexes.Values) != 2 ||
		indexes.Values[0] == nil ||
		indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != scriptID ||
		string(indexes.Values[1].Value) != scriptID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Script deletion indexes are corrupt")
	}

	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.ScriptRemoval,
		TargetID: scriptID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: scriptID}}, Input: &ref, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	captureConditions, captureWrites, err := scriptremoval.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(captureWrites)
	publication, err := prepareDirectDesiredProjectionPublication(
		ctx, repository.store, current.Record.EnvironmentID, marker,
		scriptauthoring.Remove(current.Record),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearDesiredMutationFragment(publication)
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(activeValue)

	conditions := []etcdstore.Condition{
		{
			Key: scriptrecord.ScriptSetScriptKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				scriptID,
			),
			ModRevision: current.Revision,
		},
		{
			Key: scriptrecord.ScriptSetOwnerKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				scriptID,
			),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key: scriptrecord.ScriptSetSlugKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				current.Record.Desired.Slug,
			),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), scriptID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		servicerecord.ServiceDesiredCondition(target),
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: deletionrecord.TombstoneKey("service", target.Record.Desired.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID), ModRevision: active.Revision},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID),
		})
	}
	mutations := []etcdstore.Mutation{
		{
			Type: etcdstore.MutationPut, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), scriptID),
			Value: tombstoneValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID),
			Value: activeValue,
		},
	}
	conditions = append(conditions, work.Conditions...)
	conditions = append(conditions, captureConditions...)
	mutations = append(mutations, work.Mutations...)
	mutations = append(mutations, captureWrites...)
	classify := idempotencyPlanClassifier(func(_ int64, _ []*etcdstore.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Script removal input or Environment changed; refresh before removing")
	})
	conditions, mutations, classify, err = publication.bindDirectDesired(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
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
