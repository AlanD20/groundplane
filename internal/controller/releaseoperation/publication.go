package releaseoperation

import (
	"context"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

func (service *Service) publish(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	desiredKind etcd.ReleaseDesiredKind,
	desiredID string,
	desiredRevision int64,
	groupID string,
	candidates []releaseCandidateInput,
	locator idempotencyrecord.IdempotencyLocator,
	durable idempotencyrecord.ProtectedIntentRecord,
	protected requestidempotency.ProtectedEvidence,
) (idempotencyrecord.IdempotencyResponse, error) {
	if err := service.sealCandidates(ctx, candidates); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projection, err := service.captureDesiredProjection(ctx, scope)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	operationKind := domain.OperationDeploy
	taskType := taskjournal.TaskDeploy
	for _, candidate := range candidates {
		if candidate.rollbackSource != "" {
			operationKind = domain.OperationRollback
			taskType = taskjournal.TaskRollback
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	budget := int64((20*time.Minute + time.Duration(len(candidates))*10*time.Minute) / time.Second)
	if policy == domain.OnFailureSwitchBack {
		budget += int64(time.Duration(len(candidates)) * 10 * time.Minute / time.Second)
	}
	publicationID := ids.NewULID()
	operationID := ids.New(ids.KindOperation)
	taskID := ids.New(ids.KindTask)
	planID := ids.New(ids.KindPlan)
	artifactID := ids.New(ids.KindConfig)
	owner, err := etcd.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, IdempotencyKey: locator.Key,
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: taskjournal.TaskExecutorAgent,
		PlanID: planID, Type: taskType, Target: desiredID,
		Params: map[string]string{
			etcd.TaskReleasePublicationParam: publicationID,
			etcd.TaskComposeArtifactParam:    artifactID,
		},
		Steps: make([]taskjournal.TaskStepRecord, len(candidates)*5), TimeoutSeconds: configured,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	for index := range task.Steps {
		task.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	stage := etcd.ReleaseStage{
		PublicationID: publicationID, OperationID: operationID,
		Members: make([]etcd.ReleaseStageMember, len(candidates)), CreatedAt: now,
	}
	groupMembers := make([]domain.GroupMember, len(candidates))
	renderMembers := make([]etcd.ReleaseTaskRenderMember, len(candidates))
	fenceMembers := make([]etcd.ReleaseFenceMember, len(candidates))
	for index, candidate := range candidates {
		releaseID := ids.New(ids.KindDeployment)
		priorArtifactID := ""
		priorSlot := candidate.planning.Projection.ServingSlot
		if candidate.priorStrategy != domain.StrategyBlueGreen {
			priorSlot = ""
		}
		candidateTarget, err := domain.TargetFor(candidate.strategy, candidate.slot)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		priorTarget, err := domain.TargetFor(candidate.priorStrategy, priorSlot)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if releaseNeedsPriorArtifact(candidate) {
			priorArtifactID = ids.New(ids.KindConfig)
		}
		render := etcd.ReleaseRenderInput{
			ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID, PriorArtifactID: priorArtifactID,
			ServiceID:         candidate.planning.Service.Record.Desired.ID,
			ServiceName:       candidate.planning.Service.Record.Desired.Name,
			CandidateWorkload: candidate.workload, PriorWorkload: conditionalPriorWorkload(priorArtifactID != "", candidate.priorWorkload),
			Strategy: candidate.strategy, PriorStrategy: candidate.priorStrategy, Slot: candidate.slot,
			PriorSlot: priorSlot, CandidateTarget: candidateTarget, PriorTarget: priorTarget,
			ServiceDependencyPlans: scope.Compose.Record.ServiceDependencyPlans.Clone(),
			TenantID:               scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
			ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
			EnvironmentID: scope.Environment.Record.ID, EnvironmentName: scope.Environment.Record.Name,
			AuthorizedVolumeDir: scope.Environment.Record.VolumeDir, Projection: projection,
		}
		var priorRender *etcd.ReleaseRenderInput
		if candidate.priorReleaseID != "" {
			prior, err := service.ledger.GetReleaseRenderInputAt(ctx, candidate.priorReleaseID, scope.ReadRevision)
			if err != nil {
				return idempotencyrecord.IdempotencyResponse{}, err
			}
			if prior.Record.ServiceID != render.ServiceID || prior.Record.EnvironmentID != render.EnvironmentID {
				return idempotencyrecord.IdempotencyResponse{}, errs.New(
					errs.KindStateConflict,
					"historical Service proxy source identity changed",
				)
			}
			priorRender = &prior.Record
		}
		if err := configureReleaseProxy(&render, candidate.planning.Service.Record.Desired.Expose,
			priorRender, candidate.planning.Projection.Revision); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if err := service.plans.PrepareReleaseProxyImage(&render, priorRender); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if err := service.captureServingRuntime(ctx, scope, &render, candidate.priorReleaseID); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		raw, err := etcd.EncodeReleaseRenderInput(render)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		digest, _ := domain.Digest(json.RawMessage(raw))
		intent := domain.Intent{
			ID: releaseID, EnvironmentID: scope.Environment.Record.ID, ServiceID: render.ServiceID,
			OperationID: operationID, OperationKind: operationKind,
			CandidateWorkload: candidate.workload, Tag: candidate.tag,
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
		groupMembers[index] = domain.GroupMember{
			Ordinal:   uint32(index + 1),
			ServiceID: render.ServiceID,
			ReleaseID: releaseID,
		}
		renderMembers[index] = etcd.ReleaseTaskRenderMember{Intent: intent, Render: render}
		fenceMembers[index] = etcd.ReleaseFenceMember{
			ServiceID: render.ServiceID, CandidateReleaseID: releaseID, RenderInputDigest: digest,
		}
	}
	manifest, err := service.ledger.Stage(ctx, stage)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	hooks, err := service.prepareReleaseHooks(ctx, scope, manifest.ReadRevision, operationKind, task, renderMembers)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task, renderMembers = hooks.task, hooks.members
	budget += int64(hooks.executions) * int64(executionplan.ScriptExecutionTimeoutSeconds)
	if configured < budget {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindReleaseDeadlineTooShort,
			"configured release deadline is below the computed attempt budget",
		)
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
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		progress, err := executor.Begin(taskID, now)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		head.Progress = &progress
	}
	preparedTask, plan, err := service.plans.PrepareReleaseTask(ctx, task, etcd.ReleaseTaskRenderInput{
		PublicationID: publicationID, Operation: head, Members: renderMembers,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task, err = service.ledger.PrepareTaskConfigurationAtRevision(ctx, preparedTask, scope.ReadRevision)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateDescriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	var executions []etcd.ScriptExecutionRecord
	if len(plan.GetScriptRunnerSnapshots()) != 0 {
		executions, err = etcd.NewScriptExecutionRecords(task, plan, now)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	if len(executions) != hooks.executions {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "release hook execution authority is incomplete")
	}
	hookPublications := make([]etcd.ReleaseHookExecutionPublication, len(executions))
	for index, execution := range executions {
		sources, exists := hooks.sources[execution.ID]
		if !exists {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "release hook execution source is missing")
		}
		hookPublications[index] = etcd.ReleaseHookExecutionPublication{Sources: sources, Execution: execution}
	}
	response, responseBody, err := releaseAcceptedResponse(groupID, taskID, operationID, groupMembers)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
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
		}, Operation: head, CandidateReleaseDescriptor: candidateDescriptor, Plan: plan,
		Hooks: hookPublications, PublishedAt: now,
	})
	if err != nil {
		if !releaseGroupUnknownOutcome(err) {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, protected, err)
		if resolveErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, resolveErr
		}
		return releaseOperationResolution(resolution, response)
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, protected, result.Idempotency)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
			return errs.Newf(
				errs.KindValidationFailed,
				"release group order places Service %s before prerequisite %s",
				edge.Service,
				edge.Dependency,
			)
		}
	}
	return nil
}

func releasePriorWorkload(serving domain.Intent, hasServing bool) *domain.WorkloadSeal {
	if !hasServing {
		return nil
	}
	seal := serving.CandidateWorkload
	return &seal
}

func releasePriorID(serving domain.Intent, hasServing bool) string {
	if hasServing {
		return serving.ID
	}
	return ""
}

func conditionalPriorWorkload(required bool, seal *domain.WorkloadSeal) *domain.WorkloadSeal {
	if required {
		return seal
	}
	return nil
}

func releasePriorStrategy(serving domain.Intent, hasServing bool) domain.Strategy {
	if hasServing {
		return serving.Strategy
	}
	return domain.StrategyRecreate
}

func releaseNeedsPriorArtifact(candidate releaseCandidateInput) bool {
	return candidate.priorWorkload != nil
}

func releaseAcceptedResponse(
	groupID string,
	taskID string,
	operationID string,
	members []domain.GroupMember,
) (idempotencyrecord.IdempotencyResponse, []byte, error) {
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
		return idempotencyrecord.IdempotencyResponse{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, body, nil
}

func releaseOperationResolution(
	resolution requestidempotency.Resolution,
	applied idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneIdempotencyResponse(applied), nil
	case requestidempotency.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "release idempotency resolution is invalid")
	}
}

func releaseImageWithTag(value string, requested string, current string) (string, string, string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "service image reference is invalid")
	}
	digest := ""
	if digested, ok := named.(reference.Digested); ok {
		digest = digested.Digest().Encoded()
	}
	tag := requested
	preserveDigest := false
	if tag == "" && current != "" {
		tag = current
		_, tagged := named.(reference.NamedTagged)
		preserveDigest = digest != "" && !tagged
	}
	if tag == "" {
		tagged, ok := named.(reference.NamedTagged)
		if ok {
			tag = tagged.Tag()
		} else if digest != "" {
			tag = "sha-" + digest
			preserveDigest = true
		} else {
			return "", "", "", errs.New(
				errs.KindValidationFailed,
				"release tag is required when the Service image has no tag",
			)
		}
	}
	if strings.ContainsAny(tag, "@/\\") || strings.TrimSpace(tag) != tag || tag == "" {
		return "", "", "", errs.New(errs.KindValidationFailed, "release image tag is invalid")
	}
	if preserveDigest {
		return reference.FamiliarString(named), tag, digest, nil
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

func releaseFailurePolicy(requested domain.OnFailure, declared core.OnFailure) (domain.OnFailure, error) {
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

func cloneIdempotencyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func releaseGroupUnknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
