package network

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *zoneDeletionService) prepareZoneRemovalWork(
	ctx context.Context,
	request etcd.NetworkWorkPublication,
	projectionEvidence controllerrevision.ProjectionEvidence,
	affected []string,
	accepted workqueue.TaskSnapshot,
	at time.Time,
	impactToken string,
	detachIDs *[]string,
) (taskdependencies.Prepared, error) {
	placement, err := service.repository.ResolveResourceHost(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	scope := workexecution.Scope{TenantID: request.Project.Record.TenantID,
		ProjectID: request.Project.Record.ID, EnvironmentID: request.Environment.Record.ID}
	request.Task, request.Item, request.Input, err = buildZoneRemovalWork(scope, placement, request.Claim,
		request.Zone.Record.Desired.ID, request.Zone.Record.Desired.Name, request.Zone.Record.Desired.Subnet, affected)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	request.Task.TargetName = accepted.Task.TargetName
	request.Task.AcceptedIntent = accepted.Task.AcceptedIntent
	request.Task.After = append([]string(nil), accepted.Task.After...)
	request.Task.CreatedAt, request.Task.Deadline, request.Task.UpdatedAt = accepted.Task.CreatedAt, accepted.Task.Deadline, at
	budget, err := networkexecution.ExecutionBudget(request.Input.Action)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	request.Item.CreatedAt, request.Item.UpdatedAt = at, at
	request.Item.Deadline = acceptedaction.ExecutionDeadline(request.Task, at, budget)
	var cascade capturedBackingZoneWork
	defer cascade.Clear()
	if request.Project.Record.Kind == hierarchy.ProjectKindBacking {
		snapshots, err := service.repository.CaptureBackingZoneDetaches(ctx, request.Project.Record.ID,
			request.Zone.Record.Desired.ID, request.Zone.ReadRevision)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		cascade, err = captureBackingZoneWork(ctx, service.repository, request.Task, request.Item,
			request.Zone.ReadRevision, impactToken, snapshots)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		for _, item := range cascade.Graph.Items[:len(cascade.Graph.Items)-1] {
			*detachIDs = append(*detachIDs, item.ID)
		}
		request.Task = cascade.Graph.Task
		request.Item = cascade.Graph.Items[len(cascade.Graph.Items)-1]
		request.Cascade = &etcd.NetworkCascadePublication{SnapshotRevision: cascade.Graph.SnapshotRevision,
			Items: cascade.Graph.Items, Inputs: cascade.Inputs, Captures: cascade.Hooks}
	}
	desired, err := controllerrevision.DeriveCurrentMutationDesiredInput(ctx, service.repository,
		request.Environment.Record.ID, request.Authorities.Desired.Record.RevisionID, request.Projection,
		request.Environment.Record.NetworkPool, func(*core.BlueprintDesiredInput) error { return nil })
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: request.Claim, Mutation: &blueprints.EnvironmentDesiredMutationAudit{Zone: &blueprints.EnvironmentZoneMutationAudit{
			Action: blueprints.EnvironmentZoneMutationRemove, BaseRevisionID: request.Authorities.Desired.Record.RevisionID,
			ZoneID: request.Zone.Record.Desired.ID,
		}}, DesiredInput: desired, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return taskdependencies.Prepared{}, err
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: request.Task.ID})
	if err != nil {
		return taskdependencies.Prepared{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	request.Marker = idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: request.Claim.Locator, Intent: request.Claim.Intent, TaskID: request.Task.ID,
		ReplayTarget: &idempotency.IdempotencyReplayTarget{
			Kind: idempotency.IdempotencyReplayTargetZone,
			ID:   request.Task.TargetID,
		},
		Response: response, CreatedAt: request.Task.CreatedAt, UpdatedAt: request.Task.CreatedAt,
	}
	authority, err := service.repository.PrepareNetworkWork(ctx, request)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	graph := []workexecution.Item{request.Item}
	staged := request.Cascade != nil
	if staged {
		graph = request.Cascade.Items
	}
	return taskdependencies.Prepared{Task: request.Task, Graph: graph, Authority: authority, Staged: staged}, nil
}
