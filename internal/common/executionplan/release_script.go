package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumReleaseScriptExecutions = 16

func validateReleaseScriptPlan(
	plan *agentpb.ExecutionPlan,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	releaseOperation := plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY ||
		plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	blueprintApply := plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
		ids.Validate(ids.KindEnvironment, plan.TargetId) == nil
	if !releaseOperation && !blueprintApply {
		return errs.New(errs.KindValidationFailed, "release Script plan operation is invalid")
	}
	runs := make([]*agentpb.ExecutionStep, 0, len(plan.ScriptBodyArtifacts))
	for _, step := range plan.Steps {
		if step.GetRunScript() != nil {
			runs = append(runs, step)
		}
	}
	if len(runs) == 0 {
		if len(plan.ScriptRunnerSnapshots) != 0 || len(plan.ScriptRunnerProjections) != 0 ||
			len(plan.ScriptBodyArtifacts) != 0 {
			return errs.New(errs.KindValidationFailed, "hook-free release plan carries Script material")
		}
		return nil
	}
	if len(runs) > maximumReleaseScriptExecutions || len(plan.ScriptRunnerSnapshots) != len(runs) ||
		len(plan.ScriptRunnerProjections) != len(runs) || len(plan.ScriptBodyArtifacts) != len(runs) {
		return errs.New(errs.KindValidationFailed, "release Script execution set is incomplete")
	}
	snapshots := make(map[string]*agentpb.ResolvedRunnerSnapshot, len(runs))
	projections := make(map[string]*agentpb.ScriptRunnerProjection, len(runs))
	bodies := make(map[string]*agentpb.ScriptBodyArtifactMetadata, len(runs))
	for _, value := range plan.ScriptRunnerSnapshots {
		if value == nil || snapshots[value.ScriptExecutionId] != nil {
			return errs.New(errs.KindValidationFailed, "release Script snapshot identity is duplicated")
		}
		snapshots[value.ScriptExecutionId] = value
	}
	for _, value := range plan.ScriptRunnerProjections {
		if value == nil || projections[value.SnapshotId] != nil {
			return errs.New(errs.KindValidationFailed, "release Script projection identity is duplicated")
		}
		projections[value.SnapshotId] = value
	}
	bodyBytes := uint64(0)
	for _, value := range plan.ScriptBodyArtifacts {
		if value == nil || bodies[value.ScriptExecutionId] != nil {
			return errs.New(errs.KindValidationFailed, "release Script body identity is duplicated")
		}
		bodies[value.ScriptExecutionId] = value
		bodyBytes += uint64(value.Size)
	}
	if bodyBytes > 1<<20 {
		return errs.New(errs.KindValidationFailed, "release Script bodies exceed the operation limit")
	}
	seen := make(map[string]struct{}, len(runs))
	for _, step := range runs {
		run := step.GetRunScript()
		if _, duplicate := seen[run.ScriptExecutionId]; duplicate {
			return errs.New(errs.KindValidationFailed, "release Script execution identity is duplicated")
		}
		seen[run.ScriptExecutionId] = struct{}{}
		snapshot := snapshots[run.ScriptExecutionId]
		body := bodies[run.ScriptExecutionId]
		if snapshot == nil || body == nil || projections[snapshot.SnapshotId] == nil {
			return errs.New(errs.KindValidationFailed, "release Script references are incomplete")
		}
		if blueprintApply {
			applyStep, artifact, err := blueprintScriptCandidateArtifact(plan, step, artifacts)
			if err != nil {
				return err
			}
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK ||
				run.EnvironmentId != plan.TargetId ||
				!blueprintCandidateReleaseBound(run, snapshot, applyStep, artifact) {
				return errs.New(errs.KindValidationFailed, "Blueprint Script candidate authority is invalid")
			}
		} else {
			switch step.Policy {
			case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK,
				agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK,
				agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK:
			default:
				return errs.New(errs.KindValidationFailed, "release Script phase policy is invalid")
			}
		}
		if err := validateSingleScriptPlan(&agentpb.ExecutionPlan{
			Schema: plan.Schema, PlanId: plan.PlanId, RenderGeneration: plan.RenderGeneration,
			Operation: agentpb.PlanOperation_PLAN_OPERATION_SCRIPT, TargetId: run.ScriptId,
			ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{snapshot},
			ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{projections[snapshot.SnapshotId]},
			ScriptBodyArtifacts:     []*agentpb.ScriptBodyArtifactMetadata{body},
			Steps:                   []*agentpb.ExecutionStep{step},
		}, !blueprintApply); err != nil {
			return err
		}
	}
	return nil
}

func blueprintScriptCandidateArtifact(
	plan *agentpb.ExecutionPlan,
	run *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) (*agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	byID := make(map[string]*agentpb.ExecutionStep, len(plan.Steps))
	for _, step := range plan.Steps {
		byID[step.StepId] = step
	}
	predecessor := byID[run.PrerequisiteStepId]
	for predecessor != nil && predecessor.GetRunScript() != nil {
		predecessor = byID[predecessor.PrerequisiteStepId]
	}
	if predecessor == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Script procedure has no candidate prerequisite")
	}
	apply := predecessor.GetComposeApply()
	artifact := artifacts[apply.GetArtifactId()]
	if apply == nil || artifact == nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != plan.GetTargetId() {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Script predecessor is not its candidate apply")
	}
	return predecessor, artifact, nil
}

func blueprintCandidateReleaseBound(
	run *agentpb.RunScript,
	snapshot *agentpb.ResolvedRunnerSnapshot,
	applyStep *agentpb.ExecutionStep,
	artifact *agentpb.ComposeArtifact,
) bool {
	authority := snapshot.GetProcedureServiceImage()
	apply := applyStep.GetComposeApply()
	if authority == nil || apply == nil || snapshot.GetImageReference() != "" ||
		len(snapshot.GetImageDigest()) != 0 ||
		authority.GetComposeApplyStepId() != applyStep.GetStepId() ||
		authority.GetArtifactId() != apply.GetArtifactId() ||
		authority.GetServiceId() != run.GetServiceId() ||
		authority.GetReleaseId() != run.GetReleaseId() ||
		!composeApplySelectsService(apply, run.GetServiceId()) {
		return false
	}
	matches := 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == run.GetServiceId() &&
			expectedReleaseLabel(service) == run.GetReleaseId() &&
			service.GetImageReference() == authority.GetRequestedReference() {
			matches++
		}
	}
	return matches == 1
}
