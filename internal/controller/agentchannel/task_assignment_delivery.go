package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) sendTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
) error {
	assignment, err := s.taskAssignmentMessage(stream.Context(), claim, false)
	if err != nil {
		return err
	}
	defer clearScriptAssignmentArtifacts(assignment.GetScriptArtifacts())
	defer clearBackingHookPlanSecrets(assignment.GetPlan())
	return s.sendResolvedTaskAssignment(stream, claim, assignment)
}

func (s *Server) dispatchResolvedTaskAssignment(
	session *Session,
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
	recovered bool,
) (bool, error) {
	defer clearScriptAssignmentArtifacts(assignment.GetScriptArtifacts())
	defer clearBackingHookPlanSecrets(assignment.GetPlan())
	expired := false
	sent, err := session.sendAssignment(func() error {
		if recovered && claim.Assignment.Record.ExecutionMode == taskassignments.TaskExecutionModeForward &&
			!s.now().UTC().Before(claim.Assignment.Record.Deadline.UTC()) {
			expired = true
			return nil
		}
		return s.sendResolvedTaskAssignment(stream, claim, assignment)
	})
	if expired {
		return false, nil
	}
	return sent, err
}

func (s *Server) sendResolvedTaskAssignment(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
) error {
	if err := stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: assignment},
	}); err != nil {
		return err
	}
	for _, step := range assignment.GetPlan().GetSteps() {
		if step.GetBackupSourceCapture() != nil || step.GetBackupArtifactPrune() != nil {
			if err := s.sendBackupSecretSlots(
				stream.Context(),
				stream,
				backupsecret.Request{
					TaskID:          claim.Task.Record.ID,
					AssignmentID:    claim.Assignment.Record.AssignmentID,
					AgentID:         claim.Assignment.Record.AgentID,
					AgentGeneration: claim.Assignment.Record.AgentGeneration,
					Deadline:        claim.Assignment.Record.Deadline,
					StepID:          step.GetStepId(),
					Plan:            assignment.GetPlan(),
					Step:            step,
				},
			); err != nil {
				return err
			}
		}
		if action := step.GetComponentApply(); action != nil && action.GetManagedConfigContent() &&
			assignment.GetPlan().GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			if err := s.sendManagedConfig(
				stream, claim.Task.Record, assignment.GetAssignmentId(), assignment.GetPlan(), step,
			); err != nil {
				return err
			}
		}
		if step.GetMaterializeFile() == nil {
			continue
		}
		if err := s.sendMaterialization(
			stream, claim.Task.Record, assignment.GetAssignmentId(), assignment.GetPlan(), step,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) taskAssignmentMessage(
	ctx context.Context,
	claim etcd.TaskAssignment,
	recovered bool,
) (*agentpb.TaskAssignment, error) {
	_ = recovered
	task := claim.Task.Record
	record := claim.Assignment.Record
	if ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || record.TaskID != task.ID ||
		record.Executor != taskjournal.TaskExecutorAgent {
		return nil, errs.New(errs.KindInternal, "durable Agent Task assignment is invalid")
	}
	if s.plans == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || len(planHash) != 32 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid plan hash")
	}
	if task.TimeoutSeconds <= 0 {
		return nil, errs.New(errs.KindInternal, "durable Task has an invalid Agent timeout")
	}
	resolved, err := s.plans.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return nil, err
	}
	plan, err := executionplan.Validate(resolved)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	planIDMatches := plan.PlanId == task.PlanID
	planHashMatches := bytes.Equal(plan.PlanHash, planHash)
	renderGenerationMatches := plan.RenderGeneration == uint64(task.RenderGeneration)
	targetMatches := plan.TargetId == task.Target
	operationMatches := operationMatchesTask(plan.Operation, task)
	stepsMatch := stepSummariesMatch(plan.Steps, task.Steps)
	if !planIDMatches || !planHashMatches || !renderGenerationMatches || !targetMatches || !operationMatches ||
		!stepsMatch {
		return nil, errs.Newf(
			errs.KindInternal,
			"resolved execution plan does not match its durable Task: plan_id=%t plan_hash=%t render_generation=%t target=%t operation=%t steps=%t",
			planIDMatches,
			planHashMatches,
			renderGenerationMatches,
			targetMatches,
			operationMatches,
			stepsMatch,
		)
	}
	if err := validateCandidateReleaseAssignment(ctx, s.tasks, claim, plan); err != nil {
		return nil, err
	}
	executionAuthority, err := candidateReleaseAssignmentAuthority(claim, plan)
	if err != nil {
		return nil, err
	}
	var scriptArtifacts *agentpb.ScriptAssignmentArtifacts
	var scriptCheckpoints []*agentpb.ScriptExecutionCheckpoint
	if len(plan.ScriptBodyArtifacts) != 0 {
		if s.scriptArtifacts == nil {
			return nil, errs.New(errs.KindInternal, "Script artifact resolver is not configured")
		}
		scriptArtifacts, err = s.scriptArtifacts.ResolveScriptAssignmentArtifacts(ctx, task, plan)
		if err != nil {
			return nil, err
		}
		scriptCheckpoints, err = s.scriptArtifacts.ResolveScriptExecutionCheckpoints(ctx, task, plan)
		if err != nil {
			clearScriptAssignmentArtifacts(scriptArtifacts)
			return nil, err
		}
	}
	executionDeadline := record.Deadline
	if record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
		executionDeadline = record.RecoveryDeadline
		if claim.RecoveryProofRequired {
			if record.RecoveryExecutionDeadline.IsZero() {
				return nil, errs.New(errs.KindInternal, "proof-required recovery has no execution budget")
			}
			executionDeadline = record.RecoveryExecutionDeadline
		} else if !record.RecoveryExecutionDeadline.IsZero() {
			return nil, errs.New(errs.KindInternal, "active recovery carries proof-required execution budget")
		}
	}
	return &agentpb.TaskAssignment{
		TaskId: task.ID, AssignmentId: record.AssignmentID,
		OperationId: task.OperationID, RetryOf: task.RetryOf,
		Plan: plan, ScriptArtifacts: scriptArtifacts, ScriptCheckpoints: scriptCheckpoints,
		AutomaticReconcile:          etcd.IsAutomaticReconcileTask(task),
		ForwardDeadline:             timestamppb.New(record.Deadline.UTC()),
		ExecutionEpoch:              record.ExecutionEpoch,
		ExecutionMode:               executionAuthority.mode,
		RecoveryDeadline:            timestamppb.New(record.RecoveryDeadline.UTC()),
		RestorationAuthority:        executionAuthority.restoration,
		ReleaseRecoveryRecordSha256: append([]byte(nil), executionAuthority.recoveryDigest...),
		ReleaseRecoveryDirective:    executionAuthority.recovery,
		ExecutionDeadline:           timestamppb.New(executionDeadline.UTC()),
	}, nil
}

func clearScriptAssignmentArtifacts(artifacts *agentpb.ScriptAssignmentArtifacts) {
	if artifacts == nil {
		return
	}
	for _, body := range artifacts.Bodies {
		if body != nil {
			clear(body.Body)
			body.Body = nil
		}
	}
	for _, secret := range artifacts.Secrets {
		if secret != nil {
			clear(secret.Value)
			secret.Value = nil
		}
	}
	for _, entry := range artifacts.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}

func stepSummariesMatch(steps []*agentpb.ExecutionStep, summaries []taskjournal.TaskStepRecord) bool {
	if len(steps) != len(summaries) {
		return false
	}
	for index, step := range steps {
		if step == nil || step.StepId != summaries[index].ID {
			return false
		}
	}
	return true
}
