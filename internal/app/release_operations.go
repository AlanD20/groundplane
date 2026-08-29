package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdrg "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

const (
	serviceDeployRoute             = "/services/{id}/deploy"
	serviceRollbackRoute           = "/services/{id}/rollback"
	releaseGroupDeployRoute        = "/release-groups/{id}/deploy"
	releaseGroupRollbackRoute      = "/release-groups/{id}/rollback"
	defaultReleaseExecutionTimeout = 15 * time.Hour
)

type releaseOperationService struct {
	ledger      *etcd.ReleaseLedger
	services    *etcd.ServiceRepository
	groups      *etcdrg.Store
	idempotency *etcd.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
	plans       *controllerpkg.TaskPlanResolver
	timeout     time.Duration
	now         func() time.Time
}

type releaseCandidateInput struct {
	planning       etcd.ReleasePlanningService
	image          string
	tag            string
	digest         string
	priorImage     string
	priorReleaseID string
	strategy       domain.Strategy
	priorStrategy  domain.Strategy
	slot           domain.Slot
	onFailure      domain.OnFailure
	rollbackSource string
}

func newReleaseOperationService(
	ledger *etcd.ReleaseLedger,
	services *etcd.ServiceRepository,
	groups *etcdrg.Store,
	idempotency *etcd.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
	plans *controllerpkg.TaskPlanResolver,
	timeout time.Duration,
) (*releaseOperationService, error) {
	if ledger == nil || services == nil || groups == nil || idempotency == nil || coordinator == nil || plans == nil {
		return nil, errs.New(errs.KindInternal, "release operation dependencies are not configured")
	}
	if timeout == 0 {
		timeout = defaultReleaseExecutionTimeout
	}
	if timeout < 40*time.Minute || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return nil, errs.New(errs.KindValidationFailed, "release execution timeout must be an integral duration from 40m through 24h")
	}
	return &releaseOperationService{
		ledger: ledger, services: services, groups: groups, idempotency: idempotency,
		coordinator: coordinator, plans: plans, timeout: timeout, now: time.Now,
	}, nil
}

func (service *releaseOperationService) DeployService(
	ctx context.Context,
	serviceID string,
	request apiTypes.DeployRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.services.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := idempotentintent.JSONBody(idempotentintent.Object(
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
		idempotentintent.Field{Name: "strategy", Value: idempotentintent.String(request.Strategy)},
		idempotentintent.Field{Name: "on_failure", Value: idempotentintent.String(string(request.OnFailure))},
	))
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Record.EnvironmentID, http.MethodPost, serviceDeployRoute, serviceID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	selected, err := service.deployCandidate(ctx, scope, planning[0], request.Tag, request.Strategy, request.OnFailure)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredService, serviceID, planning[0].Service.Revision,
		"", []releaseCandidateInput{selected}, locator, durable, protected)
}

func (service *releaseOperationService) RollbackService(
	ctx context.Context,
	serviceID string,
	request apiTypes.RollbackRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.services.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := idempotentintent.JSONBody(idempotentintent.Object(
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
	))
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Record.EnvironmentID, http.MethodPost, serviceRollbackRoute, serviceID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	selected, err := service.rollbackCandidate(ctx, scope, planning[0], request.Tag, "")
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredService, serviceID, planning[0].Service.Revision,
		"", []releaseCandidateInput{selected}, locator, durable, protected)
}

func (service *releaseOperationService) DeployReleaseGroup(
	ctx context.Context,
	groupID string,
	request apiTypes.ReleaseGroupDeployRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := idempotentintent.JSONBody(idempotentintent.Object(
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
	))
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Group.EnvironmentID, http.MethodPost, releaseGroupDeployRoute, groupID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Group.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	group, err := service.groups.GetAtRevision(ctx, groupID, scope.ReadRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tag := request.Tag
	if tag == "" {
		tag = group.Group.DefaultTag
	}
	if tag == "" {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindReleaseGroupTagRequired, "release group deploy requires a tag or group default")
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, group.Group.Order)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidates := make([]releaseCandidateInput, len(planning))
	for index := range planning {
		candidates[index], err = service.deployCandidate(ctx, scope, planning[index], tag, "", apiTypes.OnFailure(group.Group.OnFailure))
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredGroup, groupID, group.Revision,
		groupID, candidates, locator, durable, protected)
}

func (service *releaseOperationService) RollbackReleaseGroup(
	ctx context.Context,
	groupID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Group.EnvironmentID, http.MethodPost, releaseGroupRollbackRoute, groupID,
		idempotencyKey, idempotentintent.JSONBody(idempotentintent.Object()),
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Group.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	group, err := service.groups.GetAtRevision(ctx, groupID, scope.ReadRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, group.Group.Order)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidates := make([]releaseCandidateInput, len(planning))
	for index := range planning {
		candidates[index], err = service.rollbackCandidate(ctx, scope, planning[index], "", domain.OnFailure(group.Group.OnFailure))
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredGroup, groupID, group.Revision,
		groupID, candidates, locator, durable, protected)
}

func (service *releaseOperationService) begin(
	ctx context.Context,
	environmentID string,
	method string,
	route string,
	targetID string,
	key string,
	body idempotentintent.Body,
) (etcd.IdempotencyLocator, idempotentintent.ProtectedEvidence, etcd.ProtectedIntentRecord, *etcd.IdempotencyResponse, error) {
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: method, Route: route, Key: key,
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: targetID}},
		Query: idempotentintent.Object(), Body: body,
	})
	if err != nil {
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	defer digest.Destroy()
	protected, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		protected.Destroy()
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	resolution, exists, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, protected)
	if err != nil {
		clear(durable.Ciphertext)
		protected.Destroy()
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	if exists {
		clear(durable.Ciphertext)
		protected.Destroy()
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil,
				errs.New(errs.KindInternal, "release idempotency resolution is invalid")
		}
		response := cloneIdempotencyResponse(resolution.Response)
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, &response, nil
	}
	return locator, protected, durable, nil, nil
}

func (service *releaseOperationService) deployCandidate(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	planning etcd.ReleasePlanningService,
	requestedTag string,
	requestedStrategy string,
	requestedFailure apiTypes.OnFailure,
) (releaseCandidateInput, error) {
	serving, hasServing, err := service.ledger.GetPlanningServingIntent(ctx, scope, planning)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	tag := strings.TrimSpace(requestedTag)
	if tag != requestedTag {
		return releaseCandidateInput{}, errs.New(errs.KindValidationFailed, "release tag must not contain surrounding whitespace")
	}
	if tag == "" && hasServing {
		tag = serving.Tag
	}
	image, authoredTag, digest, err := releaseImageWithTag(planning.Service.Record.Desired.Image, tag)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	if tag == "" {
		tag = authoredTag
	}
	strategy, err := releaseStrategy(requestedStrategy, planning.Service.Record.Desired.Strategy)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	onFailure, err := releaseFailurePolicy(requestedFailure, planning.Service.Record.Desired.OnFailure)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	return releaseCandidateInput{
		planning: planning, image: image, tag: tag, digest: digest,
		priorImage: releasePriorImage(planning, serving, hasServing), priorReleaseID: releasePriorID(serving, hasServing),
		strategy: strategy, priorStrategy: releasePriorStrategy(serving, hasServing),
		slot: inactiveReleaseSlot(strategy, planning.Projection.ServingSlot), onFailure: onFailure,
	}, nil
}

func (service *releaseOperationService) rollbackCandidate(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	planning etcd.ReleasePlanningService,
	explicitTag string,
	overrideFailure domain.OnFailure,
) (releaseCandidateInput, error) {
	selection, err := service.ledger.SelectRollback(
		ctx, scope.Environment.Record.ID, planning.Service.Record.Desired.ID, explicitTag, scope.ReadRevision,
	)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	serving, hasServing, err := service.ledger.GetPlanningServingIntent(ctx, scope, planning)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	onFailure := selection.Source.OnFailure
	if overrideFailure != "" {
		onFailure = overrideFailure
	}
	return releaseCandidateInput{
		planning: planning, image: selection.Source.Image, tag: selection.Source.Tag,
		digest: selection.Source.Digest, strategy: selection.Source.Strategy,
		priorImage: releasePriorImage(planning, serving, hasServing), priorReleaseID: releasePriorID(serving, hasServing),
		priorStrategy: releasePriorStrategy(serving, hasServing),
		slot:          inactiveReleaseSlot(selection.Source.Strategy, planning.Projection.ServingSlot),
		onFailure:     onFailure, rollbackSource: selection.Source.ID,
	}, nil
}

func (service *releaseOperationService) publish(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	desiredKind etcd.ReleaseDesiredKind,
	desiredID string,
	desiredRevision int64,
	groupID string,
	candidates []releaseCandidateInput,
	locator etcd.IdempotencyLocator,
	durable etcd.ProtectedIntentRecord,
	protected idempotentintent.ProtectedEvidence,
) (etcd.IdempotencyResponse, error) {
	now := service.now().UTC()
	operationKind := domain.OperationDeploy
	taskType := etcd.TaskDeploy
	for _, candidate := range candidates {
		if candidate.rollbackSource != "" {
			operationKind = domain.OperationRollback
			taskType = etcd.TaskRollback
			break
		}
	}
	configured := int64(service.timeout / time.Second)
	policy := candidates[0].onFailure
	dependencyPlan := scope.Compose.Record.DeployDependencyPlan
	if operationKind == domain.OperationRollback {
		dependencyPlan = scope.Compose.Record.RollbackDependencyPlan
	}
	if err := validateReleaseDependencyOrder(candidates, dependencyPlan); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	budget := int64((20*time.Minute + time.Duration(len(candidates))*10*time.Minute) / time.Second)
	if policy == domain.OnFailureSwitchBack {
		budget += int64(time.Duration(len(candidates)) * 10 * time.Minute / time.Second)
	}
	if configured < budget {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindReleaseDeadlineTooShort, "configured release deadline is below the computed attempt budget")
	}
	publicationID := ids.NewULID()
	operationID := ids.New(ids.KindOperation)
	taskID := ids.New(ids.KindTask)
	planID := ids.New(ids.KindPlan)
	artifactID := ids.New(ids.KindConfig)
	owner, err := etcd.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, IdempotencyKey: locator.Key,
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorAgent,
		PlanID: planID, Type: taskType, Target: desiredID,
		Params: map[string]string{etcd.TaskReleasePublicationParam: publicationID},
		Steps:  make([]etcd.TaskStepRecord, len(candidates)*5), TimeoutSeconds: configured,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	for index := range task.Steps {
		task.Steps[index] = etcd.TaskStepRecord{ID: ids.New(ids.KindStep)}
	}
	stage := etcd.ReleaseStage{
		PublicationID: publicationID, OperationID: operationID,
		Members: make([]etcd.ReleaseStageMember, len(candidates)), CreatedAt: now,
	}
	groupMembers := make([]domain.GroupMember, len(candidates))
	renderMembers := make([]etcd.ReleaseTaskRenderMember, len(candidates))
	fenceMembers := make([]etcd.ReleaseFenceMember, len(candidates))
	priorTopologyArtifactID := ""
	for _, candidate := range candidates {
		if releaseNeedsPriorArtifact(candidate) {
			priorTopologyArtifactID = ids.New(ids.KindConfig)
			break
		}
	}
	for index, candidate := range candidates {
		releaseID := ids.New(ids.KindDeployment)
		var proxyPorts []uint16
		var candidateProxyDigest, priorProxyDigest string
		priorArtifactID := ""
		priorSlot := candidate.planning.Projection.ServingSlot
		if candidate.priorStrategy != domain.StrategyBlueGreen {
			priorSlot = ""
		}
		candidateTarget, err := domain.TargetFor(candidate.strategy, candidate.slot)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		priorTarget, err := domain.TargetFor(candidate.priorStrategy, priorSlot)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if releaseNeedsPriorArtifact(candidate) {
			priorArtifactID = priorTopologyArtifactID
		}
		var proxyGeneration, priorGeneration uint64
		proxyPorts, proxyErr := domain.ProxyPorts(candidate.planning.Service.Record.Desired.Expose)
		if proxyErr == nil {
			proxyGeneration = candidate.planning.Projection.Revision + 1
			if proxyGeneration == 1 {
				proxyGeneration = 2
			}
			priorGeneration = proxyGeneration - 1
			candidateProxy, err := domain.RenderProxyConfig(candidate.planning.Service.Record.Desired.Name, releaseID, candidateTarget, proxyGeneration, proxyPorts)
			if err != nil {
				return etcd.IdempotencyResponse{}, err
			}
			priorProxy, err := domain.RenderProxyConfig(candidate.planning.Service.Record.Desired.Name, candidate.priorReleaseID, priorTarget, priorGeneration, proxyPorts)
			if err != nil {
				return etcd.IdempotencyResponse{}, err
			}
			candidateProxyDigest = hex.EncodeToString(candidateProxy.SHA256[:])
			priorProxyDigest = hex.EncodeToString(priorProxy.SHA256[:])
		} else if candidate.strategy == domain.StrategyBlueGreen {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "blue-green release requires an addressable TCP service")
		} else {
			priorSlot = ""
		}
		render := etcd.ReleaseRenderInput{
			ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID, PriorArtifactID: priorArtifactID,
			ServiceID:   candidate.planning.Service.Record.Desired.ID,
			ServiceName: candidate.planning.Service.Record.Desired.Name,
			Image:       candidate.image, PriorImage: conditionalPriorImage(priorArtifactID != "", candidate.priorImage),
			Strategy: candidate.strategy, PriorStrategy: candidate.priorStrategy, Slot: candidate.slot,
			PriorSlot: priorSlot, CandidateTarget: candidateTarget, PriorTarget: priorTarget,
			ProxyGeneration: proxyGeneration, PriorProxyGeneration: priorGeneration,
			ProxyPorts: proxyPorts, ProxyConfigDigest: candidateProxyDigest,
			PriorProxyDigest:       priorProxyDigest,
			ServiceDependencyPlans: scope.Compose.Record.ServiceDependencyPlans.Clone(),
			TenantID:               scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
			ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
			EnvironmentID: scope.Environment.Record.ID, EnvironmentName: scope.Environment.Record.Name,
			AuthorizedVolumeDir: scope.Environment.Record.VolumeDir, Projection: scope.Compose.Record,
		}
		raw, err := etcd.EncodeReleaseRenderInput(render)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		digest, _ := domain.Digest(json.RawMessage(raw))
		intent := domain.Intent{
			ID: releaseID, EnvironmentID: scope.Environment.Record.ID, ServiceID: render.ServiceID,
			OperationID: operationID, OperationKind: operationKind,
			Image: candidate.image, Tag: candidate.tag, Digest: candidate.digest,
			Strategy: candidate.strategy, Slot: candidate.slot, OnFailure: candidate.onFailure,
			RollbackSourceReleaseID:  candidate.rollbackSource,
			PriorServingReleaseID:    candidate.planning.Projection.ServingReleaseID,
			PriorSuccessfulReleaseID: candidate.planning.Projection.CurrentSuccessfulReleaseID,
			RenderInputID:            artifactID, RenderInputDigest: digest, CreatedAt: now, Actor: "operator",
			Workspace: domain.Workspace{
				Kind: domain.WorkspaceTenant, TenantID: scope.Tenant.Record.ID,
				ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID,
			}, OriginatingTaskID: taskID,
		}
		if groupID != "" {
			intent.GroupOperationID = operationID
			intent.GroupMemberOrdinal = uint32(index + 1)
		}
		checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: now}
		stage.Members[index] = etcd.ReleaseStageMember{Intent: intent, RenderInput: raw, Checkpoint: checkpoint}
		groupMembers[index] = domain.GroupMember{Ordinal: uint32(index + 1), ServiceID: render.ServiceID, ReleaseID: releaseID}
		renderMembers[index] = etcd.ReleaseTaskRenderMember{Intent: intent, Render: render}
		fenceMembers[index] = etcd.ReleaseFenceMember{
			ServiceID: render.ServiceID, CandidateReleaseID: releaseID, RenderInputDigest: digest,
		}
	}
	head := etcd.ReleaseOperationHead{
		OperationID: operationID, PublicationID: publicationID, EnvironmentID: scope.Environment.Record.ID,
		ReleaseGroupID: groupID, FailurePolicy: policy, State: domain.StatePending,
		Attempts: []domain.Attempt{{ID: taskID, TaskID: taskID, StartedAt: now}},
		Members:  groupMembers, LatestTaskID: taskID,
		ConfiguredTimeoutSeconds: configured, ComputedBudgetSeconds: budget, CreatedAt: now, UpdatedAt: now,
	}
	if groupID != "" {
		executor, err := domain.NewGroupExecutor(domain.GroupManifest{
			OperationID: operationID, ReleaseGroupID: groupID, EnvironmentID: scope.Environment.Record.ID,
			FailurePolicy: policy, Members: groupMembers,
			ConfiguredTimeoutSeconds: configured, ComputedBudgetSeconds: budget,
		})
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		progress, err := executor.Begin(taskID, now)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		head.Progress = &progress
	}
	preparedTask, err := service.plans.PrepareReleaseTask(ctx, task, etcd.ReleaseTaskRenderInput{
		PublicationID: publicationID, Operation: head, Members: renderMembers,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task = preparedTask
	manifest, err := service.ledger.Stage(ctx, stage)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, responseBody, err := releaseAcceptedResponse(groupID, taskID, operationID, groupMembers)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: durable, Response: response, TaskID: taskID,
		CreatedAt: now, UpdatedAt: now,
	}
	result, err := service.ledger.Publish(ctx, etcd.ReleasePublicationEvidence{
		Manifest: manifest, EnvironmentID: scope.Environment.Record.ID,
		ProjectID: scope.Project.Record.ID, TenantID: scope.Tenant.Record.ID,
		DesiredKind: desiredKind, DesiredID: desiredID, DesiredRevision: desiredRevision,
		EnvironmentEpochRevision: scope.EnvironmentEpochRevision,
		EnvironmentEpochValue:    slices.Clone(scope.EnvironmentEpochValue), FenceRevision: 0,
		Task: task, Marker: marker,
		Fence: etcd.ReleaseFenceSet{
			EnvironmentID: scope.Environment.Record.ID, Generation: 1, OperationID: operationID,
			AttemptTaskID: taskID, Group: groupID != "", Members: fenceMembers,
		}, Operation: head, PublishedAt: now,
	})
	if err != nil {
		if !releaseGroupUnknownOutcome(err) {
			return etcd.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, protected, err)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		return releaseOperationResolution(resolution, response)
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, protected, result.Idempotency)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	_ = responseBody
	return releaseOperationResolution(resolution, response)
}

func validateReleaseDependencyOrder(candidates []releaseCandidateInput, plan core.ServiceDependencyPhasePlan) error {
	positions := make(map[string]int, len(candidates))
	for index, candidate := range candidates {
		positions[candidate.planning.Service.Record.Desired.Name] = index
	}
	for _, edge := range plan.Edges {
		consumer, selectedConsumer := positions[edge.Service]
		dependency, selectedDependency := positions[edge.Dependency]
		if selectedConsumer && selectedDependency && dependency >= consumer {
			return errs.Newf(errs.KindValidationFailed, "release group order places Service %s before prerequisite %s", edge.Service, edge.Dependency)
		}
	}
	return nil
}

func releasePriorImage(planning etcd.ReleasePlanningService, serving domain.Intent, hasServing bool) string {
	if hasServing {
		return serving.Image
	}
	return planning.Service.Record.Desired.Image
}

func releasePriorID(serving domain.Intent, hasServing bool) string {
	if hasServing {
		return serving.ID
	}
	return "baseline"
}

func conditionalPriorImage(singleton bool, image string) string {
	if singleton {
		return image
	}
	return ""
}

func releasePriorStrategy(serving domain.Intent, hasServing bool) domain.Strategy {
	if hasServing {
		return serving.Strategy
	}
	return domain.StrategyRecreate
}

func releaseNeedsPriorArtifact(candidate releaseCandidateInput) bool {
	priorSlot := candidate.planning.Projection.ServingSlot
	if candidate.priorStrategy != domain.StrategyBlueGreen {
		priorSlot = ""
	}
	transition, err := domain.NewTopologyTransition(candidate.strategy, candidate.slot, candidate.priorStrategy, priorSlot)
	return err == nil && transition.RequiresPriorArtifact()
}

func releaseAcceptedResponse(
	groupID string,
	taskID string,
	operationID string,
	members []domain.GroupMember,
) (etcd.IdempotencyResponse, []byte, error) {
	var value any
	if groupID == "" {
		value = apiTypes.ReleaseTaskAccepted{TaskID: taskID, OperationID: operationID, ReleaseID: members[0].ReleaseID}
	} else {
		releases := make([]apiTypes.ReleaseGroupMemberRelease, len(members))
		for index, member := range members {
			releases[index] = apiTypes.ReleaseGroupMemberRelease{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID}
		}
		value = apiTypes.ReleaseGroupTaskAccepted{
			TaskID: taskID, ReleaseGroupOperationID: operationID, Releases: releases,
		}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return etcd.IdempotencyResponse{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, body, nil
}

func releaseOperationResolution(
	resolution idempotentintent.Resolution,
	applied etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(applied), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release idempotency resolution is invalid")
	}
}

func releaseImageWithTag(value string, requested string) (string, string, string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "service image reference is invalid")
	}
	digest := ""
	if digested, ok := named.(reference.Digested); ok {
		digest = digested.Digest().Encoded()
	}
	tag := requested
	if tag == "" {
		tagged, ok := named.(reference.NamedTagged)
		if !ok {
			return "", "", "", errs.New(
				errs.KindValidationFailed,
				"release tag is required when the Service image has no tag",
			)
		}
		tag = tagged.Tag()
	}
	if strings.ContainsAny(tag, "@/\\") || strings.TrimSpace(tag) != tag || tag == "" {
		return "", "", "", errs.New(errs.KindValidationFailed, "release image tag is invalid")
	}
	tagged, err := reference.WithTag(reference.TrimNamed(named), tag)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "release image tag is invalid")
	}
	return reference.FamiliarString(tagged), tag, digest, nil
}

func releaseStrategy(requested string, declared core.Strategy) (domain.Strategy, error) {
	selected := requested
	if selected == "" {
		selected = string(declared)
	}
	switch selected {
	case string(core.StrategyBlueGreen):
		return domain.StrategyBlueGreen, nil
	case string(core.StrategyRecreate):
		return domain.StrategyRecreate, nil
	case string(core.StrategyRolling):
		return "", errs.New(errs.KindStrategyNotImplemented, "rolling release strategy is not implemented in the MVP")
	default:
		return "", errs.New(errs.KindValidationFailed, "release strategy must be selected or declared by the Service")
	}
}

func releaseFailurePolicy(requested apiTypes.OnFailure, declared core.OnFailure) (domain.OnFailure, error) {
	selected := string(requested)
	if selected == "" {
		selected = string(declared.WithDefault())
	}
	switch selected {
	case string(core.OnFailureSwitchBack):
		return domain.OnFailureSwitchBack, nil
	case string(core.OnFailureLeaveActive):
		return domain.OnFailureLeaveActive, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "release failure policy is invalid")
	}
}

func inactiveReleaseSlot(strategy domain.Strategy, serving domain.Slot) domain.Slot {
	if strategy != domain.StrategyBlueGreen {
		return ""
	}
	if serving == domain.SlotBlue {
		return domain.SlotGreen
	}
	return domain.SlotBlue
}
