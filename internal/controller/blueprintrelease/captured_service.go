package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareCapturedService finalizes a direct Deploy/Rollback using only its
// accepted rollout, configuration and hook sources. Blueprint authoring has its
// separate preparation boundary; both publish the same typed Release input.
func (service *Service) PrepareCapturedService(ctx context.Context, work workqueue.View,
	request etcd.DirectReleasePreparation, rollout keyvalue.Versioned[releaserender.ServiceRolloutCapture],
) (prepared Prepared, resultErr error) {
	if ctx == nil || service == nil || service.ledger == nil || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.Task.Task.Action != workexecution.ActionDeploy && work.Task.Task.Action != workexecution.ActionRollback ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil || work.Task.Task.CollectionStartedAt != nil ||
		work.Item.Item.Phase != workexecution.Waiting || work.Item.Item.Wait == nil ||
		work.Item.Item.Wait.Reason != workexecution.WaitInput || work.Item.Item.Input != nil ||
		work.Item.Item.AttemptNumber != 0 || work.Item.Item.ForwardRevoked ||
		!time.Now().Before(work.Item.Item.Deadline) || !time.Now().Before(work.Task.Task.Deadline) {
		return Prepared{}, errs.New(errs.KindStateConflict, "Direct Release is not waiting for its accepted executable input")
	}
	if err := request.Matches(work.Task.Task, work.Item.Item, rollout.Record); err != nil {
		return Prepared{}, err
	}
	if rollout.Revision <= 0 || rollout.Revision > work.ReadRevision {
		return Prepared{}, errs.New(errs.KindStateConflict, "Direct Release rollout is not available at its work snapshot")
	}
	capture := rollout.Record
	hooks := preparedHooks{members: []releaserender.ReleaseTaskRenderMember{{Intent: request.Intent, Render: capture.Render}},
		sources: make(map[string]scriptsourcequeries.ScriptExecutionSources, len(request.Hooks)), executions: len(request.Hooks)}
	for _, hook := range request.Hooks {
		hooks.sources[hook.ExecutionID] = hook.Sources
	}
	conditions, err := service.ledger.DirectReleasePreparationConditions(ctx, work, request, rollout)
	if err != nil {
		return Prepared{}, err
	}
	artifact, err := releaserender.OpenReleaseCandidateArtifact(capture)
	if err != nil {
		return Prepared{}, err
	}
	storage, err := service.ledger.PostgresPatchConditions(ctx, artifact, capture.Render.ServiceID, request.Source.ReadRevision)
	if err != nil {
		return Prepared{}, err
	}
	defer func() {
		if resultErr == nil {
			prepared.sourceConditions = append(prepared.sourceConditions, conditions...)
		}
	}()
	return service.finishSelectedRuntime(ctx, prepareInput{
		Work: work, OperationID: capture.OperationID, PlanID: capture.Render.PlanID,
		Projection: capture.Render.Projection, CreatedAt: work.Item.Item.CreatedAt,
		Environment:   keyvalue.Versioned[hierarchy.EnvironmentRecord]{Record: hierarchy.EnvironmentRecord{ID: capture.Scope.EnvironmentID}},
		Configuration: &request.Configuration,
		AllocateNamed: func(kind ids.Kind, name string) string {
			return ids.DeriveAt(kind, work.Item.Item.CreatedAt, work.Item.Item.ID, name)
		},
	}, preparedSelectedRuntime{capture: capture, manifest: request.Manifest, hooks: hooks,
		predecessors: []etcd.BlueprintNativePredecessorCapture{request.Predecessor}, members: hooks.members,
		storageConditions: storage}, networkreservations.ProxyAddresses{})
}
