package agent

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func clearExecutionPlanSecrets(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.Steps {
		if procedure := step.GetBackingHookProcedure(); procedure != nil {
			clearBackingHookProcedureValues(procedure)
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}

func clearBackingHookProcedureValues(procedure *agentpb.BackingHookProcedure) {
	for _, value := range append(procedure.GetInputs(), procedure.GetFacts()...) {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}

func validateAndCopyAssignment(assignment Assignment, volumeRoot string) (Assignment, error) {
	if err := ids.Validate(ids.KindAssignment, assignment.AssignmentID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid assignment id")
	}
	if err := ids.Validate(ids.KindTask, assignment.TaskID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid task id")
	}
	if assignment.ExecutionEpoch == 0 || assignment.ForwardDeadline.IsZero() || assignment.RecoveryDeadline.IsZero() ||
		assignment.RecoveryDeadline.Before(assignment.ForwardDeadline) {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent invalid execution authority")
	}
	deadline := assignment.ForwardDeadline
	switch assignment.ExecutionMode {
	case agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD:
		if assignment.ReleaseRecoveryDirective != nil || len(assignment.ReleaseRecoveryRecordSHA256) != 0 ||
			assignment.RecoveryProofRequired {
			return Assignment{}, errs.New(errs.KindInternal, "agent: forward assignment carries recovery authority")
		}
	case agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY:
		deadline = assignment.RecoveryDeadline
		if assignment.RecoveryProofRequired {
			deadline = assignment.Deadline
		}
		if assignment.ReleaseRecoveryDirective == nil || len(assignment.ReleaseRecoveryRecordSHA256) != 32 ||
			!bytes.Equal(
				assignment.ReleaseRecoveryDirective.GetReleaseRecoveryRecordSha256(),
				assignment.ReleaseRecoveryRecordSHA256,
			) ||
			assignment.RecoveryProofRequired && !assignment.Deadline.After(assignment.RecoveryDeadline) {
			return Assignment{}, errs.New(errs.KindInternal, "agent: recovery assignment authority is incomplete")
		}
	default:
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid execution mode")
	}
	if err := ids.Validate(ids.KindOperation, assignment.OperationID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid operation id")
	}
	if assignment.RetryOf != "" {
		if err := ids.Validate(
			ids.KindTask,
			assignment.RetryOf,
		); err != nil ||
			assignment.RetryOf == assignment.TaskID {
			return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid retry identity")
		}
	}
	plan, err := executionplan.Validate(assignment.Plan)
	if err != nil {
		return Assignment{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, volumeRoot); err != nil {
		return Assignment{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := validateCandidateReleaseAssignmentAuthority(assignment, plan); err != nil {
		return Assignment{}, err
	}
	if assignment.AutomaticReconcile && plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		return Assignment{}, errs.New(
			errs.KindInternal,
			"agent: automatic reconciliation assignment has an invalid plan",
		)
	}
	scriptArtifacts, err := validateAndCopyScriptArtifacts(plan, assignment.ScriptArtifacts)
	if err != nil {
		return Assignment{}, err
	}
	scriptCheckpoints := make([]*agentpb.ScriptExecutionCheckpoint, len(assignment.ScriptCheckpoints))
	if len(assignment.ScriptCheckpoints) != len(plan.ScriptBodyArtifacts) {
		clearScriptArtifacts(scriptArtifacts)
		return Assignment{}, errs.New(errs.KindInternal, "agent: Script checkpoint set is incomplete")
	}
	expectedCheckpoints := make(map[string]struct{}, len(plan.ScriptBodyArtifacts))
	for _, metadata := range plan.ScriptBodyArtifacts {
		if metadata == nil || metadata.ScriptExecutionId == "" {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(errs.KindInternal, "agent: Script execution metadata is invalid")
		}
		expectedCheckpoints[metadata.ScriptExecutionId] = struct{}{}
	}
	seenCheckpoints := make(map[string]struct{}, len(assignment.ScriptCheckpoints))
	for index, checkpoint := range assignment.ScriptCheckpoints {
		scriptCheckpoints[index], err = executionplan.ValidateScriptExecutionCheckpoint(checkpoint)
		if err != nil {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.Wrap(errs.KindInternal, err)
		}
		if _, expected := expectedCheckpoints[scriptCheckpoints[index].ScriptExecutionId]; !expected {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(
				errs.KindInternal,
				"agent: Script checkpoint does not belong to the execution plan",
			)
		}
		if _, duplicate := seenCheckpoints[scriptCheckpoints[index].ScriptExecutionId]; duplicate {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(
				errs.KindInternal,
				"agent: Script checkpoint set contains a duplicate execution",
			)
		}
		seenCheckpoints[scriptCheckpoints[index].ScriptExecutionId] = struct{}{}
	}
	var restorationAuthority *agentpb.ReleaseRestorationAuthority
	if assignment.RestorationAuthority != nil {
		restorationAuthority = proto.Clone(assignment.RestorationAuthority).(*agentpb.ReleaseRestorationAuthority)
	}
	var recoveryDirective *agentpb.ReleaseRecoveryDirective
	if assignment.ReleaseRecoveryDirective != nil {
		recoveryDirective = proto.Clone(assignment.ReleaseRecoveryDirective).(*agentpb.ReleaseRecoveryDirective)
	}
	return Assignment{
		AssignmentID: assignment.AssignmentID,
		TaskID:       assignment.TaskID, OperationID: assignment.OperationID,
		RetryOf: assignment.RetryOf, Plan: plan, ScriptArtifacts: scriptArtifacts,
		ScriptCheckpoints: scriptCheckpoints,
		Deadline:          deadline, ForwardDeadline: assignment.ForwardDeadline, RecoveryDeadline: assignment.RecoveryDeadline,
		RecoveryProofRequired: assignment.RecoveryProofRequired,
		ExecutionMode:         assignment.ExecutionMode, ExecutionEpoch: assignment.ExecutionEpoch,
		RestorationAuthority: restorationAuthority, ReleaseRecoveryDirective: recoveryDirective,
		ReleaseRecoveryRecordSHA256: append([]byte(nil), assignment.ReleaseRecoveryRecordSHA256...),
		AutomaticReconcile:          assignment.AutomaticReconcile,
	}, nil
}

func hashForPlan(plan *agentpb.ExecutionPlan) PlanHash {
	var hash PlanHash
	if plan != nil {
		copy(hash[:], plan.PlanHash)
	}
	return hash
}

func usesEnvironmentDirectory(plan *agentpb.ExecutionPlan) bool {
	return executionplan.UsesEnvironmentDirectoryResult(plan)
}
