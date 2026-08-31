package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumReleaseScriptExecutions = 16

func validateReleaseScriptPlan(plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_DEPLOY &&
		plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK {
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
		switch step.Policy {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK:
		default:
			return errs.New(errs.KindValidationFailed, "release Script phase policy is invalid")
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
