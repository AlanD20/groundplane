package releaseoperation

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/rollingpredecessor"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// captureServiceWork selects no mutable inputs after acceptance. Staged Release
// records and configuration are inert; the caller must atomically publish the
// returned native work, its complete capture and replay identity.
func (service *Service) captureServiceWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	projection environmentprojection.EnvironmentComposeProjection, candidate releaseCandidateInput,
	addresses networkreservations.ProxyAddresses, requestSHA string,
) (workexecution.Task, workexecution.Item, etcd.DirectReleasePreparation, releaserender.ServiceRolloutCapture, error) {
	var task workexecution.Task
	var item workexecution.Item
	var request etcd.DirectReleasePreparation
	var capture releaserender.ServiceRolloutCapture
	placement, err := service.ledger.ResolveReleaseHost(ctx, scope.ReadRevision)
	if err != nil {
		return task, item, request, capture, err
	}
	at := service.now().UTC()
	taskID, workID, planID := ids.New(ids.KindTask), ids.New(ids.KindWorkItem), ids.New(ids.KindPlan)
	operationID, publicationID, releaseID := ids.New(ids.KindOperation), ids.NewULID(), ids.New(ids.KindDeployment)
	action, operation := workexecution.ActionDeploy, domain.OperationDeploy
	if candidate.rollbackSource != "" {
		action, operation = workexecution.ActionRollback, domain.OperationRollback
	}
	scopeID := workexecution.Scope{TenantID: scope.Project.Record.TenantID, ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID}
	task = workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Scope: scopeID, Action: action,
		TargetID: candidate.planning.Service.Record.Desired.ID, TargetName: candidate.planning.Service.Record.Desired.Name,
		Intent: workexecution.Input{ID: planID, SHA256: requestSHA}, CreatedAt: at, UpdatedAt: at,
		Deadline: at.Add(service.timeout), ExpectedItems: 1}
	item = workexecution.Item{ID: workID, TaskID: taskID, Operation: workexecution.Release,
		TargetID: task.TargetID, Placement: placement, Phase: workexecution.Waiting,
		Wait: &workexecution.Wait{Reason: workexecution.WaitInput}, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline,
		Claims: []workexecution.Claim{{ScopeID: scopeID.EnvironmentID}, {HostID: placement.HostID, ScopeID: scopeID.EnvironmentID},
			{ScopeID: task.TargetID}, {HostID: placement.HostID, ScopeID: task.TargetID}}}
	if task.Validate() != nil || item.Validate() != nil {
		return task, item, request, capture, errs.New(errs.KindValidationFailed, "Direct Release native identity is invalid")
	}
	render, predecessor, err := service.captureServiceWorkRender(ctx, scope, projection, candidate, addresses, releaseID, planID)
	if err != nil {
		return task, item, request, capture, err
	}
	raw, err := releaserender.EncodeReleaseRenderInput(render)
	if err != nil {
		return task, item, request, capture, err
	}
	renderDigest, err := domain.Digest(json.RawMessage(raw))
	if err != nil {
		return task, item, request, capture, err
	}
	intent := domain.Intent{ID: releaseID, EnvironmentID: scopeID.EnvironmentID, ServiceID: task.TargetID,
		OperationID: operationID, OperationKind: operation, CandidateWorkload: candidate.workload,
		Tag: candidate.tag, Strategy: candidate.strategy, Slot: candidate.slot, OnFailure: candidate.onFailure,
		RollbackSourceReleaseID: candidate.rollbackSource, PriorServingReleaseID: candidate.planning.Projection.ServingReleaseID,
		PriorSuccessfulReleaseID: candidate.planning.Projection.CurrentSuccessfulReleaseID,
		RenderInputID:            render.ArtifactID, RenderInputDigest: renderDigest, CreatedAt: at, Actor: "operator",
		OriginatingTaskID: taskID, Workspace: domain.Workspace{Kind: domain.WorkspaceTenant,
			TenantID: scopeID.TenantID, ProjectID: scopeID.ProjectID, EnvironmentID: scopeID.EnvironmentID}}
	if scopeID.TenantID == "" {
		intent.Workspace.Kind = domain.WorkspacePlatform
	}
	manifest, err := service.ledger.Stage(ctx, releases.ReleaseStage{PublicationID: publicationID, OperationID: operationID,
		CreatedAt: at, Members: []releases.ReleaseStageMember{{Intent: intent, RenderInput: raw,
			Checkpoint: domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: at}}}})
	if err != nil {
		return task, item, request, capture, err
	}
	work := workqueue.View{Task: workqueue.TaskSnapshot{Task: task}, Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: scope.ReadRevision}
	hooks, err := service.prepareReleaseHooks(ctx, scope, manifest.ReadRevision, operation, work, operationID, planID,
		[]releaserender.ReleaseTaskRenderMember{{Intent: intent, Render: render}})
	if err != nil {
		return task, item, request, capture, err
	}
	if err := service.validateServiceWorkBudget(candidate, hooks, render); err != nil {
		return task, item, request, capture, err
	}
	render = hooks.members[0].Render
	artifact, err := blueprintrelease.RenderServiceReleaseArtifact(ctx, hooks.members[0])
	if err != nil {
		return task, item, request, capture, err
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return task, item, request, capture, errs.Wrap(errs.KindInternal, err)
	}
	capture = releaserender.ServiceRolloutCapture{Action: action, TaskID: taskID, WorkItemID: workID,
		OperationID: operationID, PublicationID: publicationID, SourceReadRevision: scope.ReadRevision,
		Scope: scopeID, Placement: placement, CreatedAt: at, Deadline: item.Deadline,
		Render: render, CandidateArtifact: artifactBytes}
	rollout, err := releaserender.ServiceRolloutInput(capture)
	if err != nil {
		return task, item, request, capture, err
	}
	configuration, err := service.ledger.PrepareReleaseWorkConfiguration(ctx, work, ids.New(ids.KindConfig),
		projection.RenderGeneration, nil)
	if err != nil {
		return task, item, request, capture, err
	}
	request = etcd.DirectReleasePreparation{Rollout: rollout, Intent: intent, Manifest: manifest, Predecessor: predecessor,
		Configuration: configuration, Source: etcd.DirectReleaseSource{ReadRevision: scope.ReadRevision,
			EnvironmentRevision: scope.Environment.Revision, ProjectRevision: scope.Project.Revision,
			TenantRevision: scope.Tenant.Revision, DesiredRevision: candidate.planning.Service.Revision,
			EpochRevision: scope.EnvironmentEpochRevision}}
	for _, hook := range render.Hooks {
		sources, found := hooks.sources[hook.ScriptExecutionID]
		if !found {
			return task, item, request, capture, errs.New(errs.KindInternal, "Direct Release hook has no captured source")
		}
		request.Hooks = append(request.Hooks, etcd.DirectReleaseHookSources{ExecutionID: hook.ScriptExecutionID, Sources: sources})
	}
	value, reference, err := etcd.EncodeDirectReleasePreparation(request)
	clear(value)
	if err != nil {
		return task, item, request, capture, err
	}
	task.Intent = reference
	return task, item, request, capture, request.Matches(task, item, capture)
}

func (service *Service) validateServiceWorkBudget(candidate releaseCandidateInput, hooks preparedReleaseHooks, render releaserender.ReleaseRenderInput) error {
	budget := 30 * time.Minute
	if candidate.onFailure == domain.OnFailureSwitchBack {
		budget += 10 * time.Minute
	}
	if render.Rolling != nil {
		changes := max(len(render.Rolling.Prior), len(render.Rolling.Candidate))
		budget += time.Duration(changes+1)*time.Duration(render.Rolling.ReplicaTimeoutSeconds)*time.Second - 10*time.Minute
	}
	budget += time.Duration(hooks.executions) * time.Duration(executionplan.ScriptExecutionTimeoutSeconds) * time.Second
	if service.timeout < budget {
		return errs.New(errs.KindReleaseDeadlineTooShort, "configured Release deadline is below its captured rollout and hook budget")
	}
	return nil
}

func (service *Service) captureServiceWorkRender(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	projection environmentprojection.EnvironmentComposeProjection, candidate releaseCandidateInput,
	addresses networkreservations.ProxyAddresses, releaseID, planID string,
) (releaserender.ReleaseRenderInput, etcd.BlueprintNativePredecessorCapture, error) {
	priorSlot := candidate.planning.Projection.ServingSlot
	if candidate.priorStrategy != domain.StrategyBlueGreen {
		priorSlot = ""
	}
	render := releaserender.ReleaseRenderInput{ProxyAddresses: addresses.ForService(candidate.planning.Service.Record.Desired.ID),
		ReleaseID: releaseID, PlanID: planID, ArtifactID: ids.New(ids.KindConfig),
		ServiceID: candidate.planning.Service.Record.Desired.ID, ServiceName: candidate.planning.Service.Record.Desired.Name,
		CandidateWorkload: candidate.workload, Strategy: candidate.strategy, PriorStrategy: candidate.priorStrategy,
		Slot: candidate.slot, PriorSlot: priorSlot, ServiceDependencyPlans: scope.Compose.Record.ServiceDependencyPlans.Clone(),
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: scope.Environment.Record.ID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir: scope.Environment.Record.VolumeDir, Projection: projection}
	predecessor := etcd.BlueprintNativePredecessorCapture{ServiceID: render.ServiceID, FixedReadRevision: scope.ReadRevision,
		ProjectionRevision: candidate.planning.ProjectionRevision}
	var err error
	render.CandidateTarget, err = domain.TargetFor(render.Strategy, render.Slot)
	if err != nil {
		return render, predecessor, err
	}
	render.PriorTarget, err = domain.TargetFor(render.PriorStrategy, render.PriorSlot)
	if err != nil {
		return render, predecessor, err
	}
	var priorRender *releaserender.ReleaseRenderInput
	if candidate.priorReleaseID != "" {
		render.PriorArtifactID, render.PriorWorkload = ids.New(ids.KindConfig), candidate.priorWorkload
		prior, err := service.ledger.GetReleaseRenderInputAt(ctx, candidate.priorReleaseID, scope.ReadRevision)
		if err != nil {
			return render, predecessor, err
		}
		priorRender = &prior.Record
		captured, err := servicelifecycle.CaptureAcknowledgedRuntime(ctx, service.ledger,
			keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision},
			render.EnvironmentID, render.ServiceID, render.PriorArtifactID)
		if err != nil {
			return render, predecessor, err
		}
		if err := bindServingRuntime(&render, candidate.priorReleaseID, captured); err != nil {
			return render, predecessor, err
		}
		predecessor.RuntimeRevision, predecessor.Serving = captured.RuntimeRevision, &captured.Release
		predecessor.RetainedPriorReleaseID = captured.RetainedPriorReleaseID
		predecessor.CurrentArtifact, predecessor.RetainedPriorArtifact = captured.CurrentArtifact, captured.RetainedPriorArtifact
	}
	if err := rollingpredecessor.Capture(ctx, service.agents, service.observer, &render, candidate.planning.Service.Record.Desired.RollingReplicaTimeoutSeconds); err != nil {
		return render, predecessor, err
	}
	if err := configureReleaseProxy(&render, candidate.planning.Service.Record.Desired.ProxyExposures(), priorRender, candidate.planning.Projection.Revision); err != nil {
		return render, predecessor, err
	}
	if err := service.plans.PrepareReleaseProxyImage(&render, priorRender); err != nil {
		return render, predecessor, err
	}
	return render, predecessor, nil
}
