package executionplan

import (
	"bytes"
	"crypto/sha256"

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
	var candidateApply *agentpb.ComposeApply
	var candidateArtifact *agentpb.ComposeArtifact
	if blueprintApply {
		var err error
		candidateApply, candidateArtifact, err = blueprintScriptCandidateArtifact(plan, runs, artifacts)
		if err != nil {
			return err
		}
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
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK ||
				run.EnvironmentId != plan.TargetId ||
				!blueprintCandidateReleaseBound(run, snapshot, candidateApply, candidateArtifact) {
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
		if err := validateManualScriptPlan(&agentpb.ExecutionPlan{
			Schema: plan.Schema, PlanId: plan.PlanId, RenderGeneration: plan.RenderGeneration,
			Operation: agentpb.PlanOperation_PLAN_OPERATION_SCRIPT, TargetId: run.ScriptId,
			ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{snapshot},
			ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{projections[snapshot.SnapshotId]},
			ScriptBodyArtifacts:     []*agentpb.ScriptBodyArtifactMetadata{body},
			Steps:                   []*agentpb.ExecutionStep{step},
		}); err != nil {
			return err
		}
	}
	return nil
}

func blueprintScriptCandidateArtifact(
	plan *agentpb.ExecutionPlan,
	runs []*agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) (*agentpb.ComposeApply, *agentpb.ComposeArtifact, error) {
	firstRun := len(plan.Steps) - len(runs)
	if firstRun == 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Script procedure has no candidate prerequisite")
	}
	for index := firstRun; index < len(plan.Steps); index++ {
		if plan.Steps[index].GetRunScript() == nil ||
			plan.Steps[index].PrerequisiteStepId != plan.Steps[index-1].StepId {
			return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Script procedure is not a chained suffix")
		}
	}
	apply := plan.Steps[firstRun-1].GetComposeApply()
	artifact := artifacts[apply.GetArtifactId()]
	if apply == nil || artifact == nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != plan.GetTargetId() {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Script predecessor is not its candidate apply")
	}
	return apply, artifact, nil
}

func blueprintCandidateReleaseBound(
	run *agentpb.RunScript,
	snapshot *agentpb.ResolvedRunnerSnapshot,
	apply *agentpb.ComposeApply,
	artifact *agentpb.ComposeArtifact,
) bool {
	selected := apply.GetFullReconcile()
	if !selected {
		for _, serviceID := range apply.GetServiceIds() {
			if serviceID == run.GetServiceId() {
				selected = true
				break
			}
		}
	}
	if !selected {
		return false
	}
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == run.GetServiceId() &&
			expectedReleaseLabel(service) == run.GetReleaseId() &&
			service.GetImageReference() == snapshot.GetImageReference() &&
			bytes.Equal(service.GetImageIndexDigest(), snapshot.GetImageDigest()) &&
			len(service.GetImageChildDigest()) == sha256.Size {
			return true
		}
	}
	return false
}
