package hierarchydeletion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fixedIDs struct{ task string }

func (generator fixedIDs) NewTaskID() string { return generator.task }

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func finalizer(
	kind ActionKind,
	target ActionTargetKind,
	id string,
	revision int64,
	salt string,
) ControllerFinalizerInput {
	return ControllerFinalizerInput{Finalizer: controllerFinalizers[kind], TargetKind: target, TargetID: id,
		FixedInputRevision: revision, FixedInputDigest: digest(salt + "/input"), BatchCount: 1}
}

func planningNode(target ActionTargetKind, id string, kind ActionKind) MembershipNode {
	input := ProcedureInput{Kind: ProcedureControllerFinalizer}
	value := finalizer(kind, target, id, 1, id)
	input.ControllerFinalizer = &value
	if typed, found := agentProcedures[kind]; found {
		input = ProcedureInput{Kind: ProcedureAgentWork, AgentWork: &AgentWorkInput{
			TypedProcedure: typed, InputDigest: digest(id + "/input"), Timeout: time.Minute}}
	}
	return MembershipNode{NodeID: string(kind) + "/" + id, TargetKind: target, ID: id, ActionKind: kind,
		TargetRevision: 1, ProcedureInput: input}
}

// Recording planning ports only. No fake executor or fabricated child Task
// settles host work or root metadata on behalf of production owners.
type planningRepository struct {
	operation                 Operation
	snapshot                  FrozenMembership
	actions                   []Action
	begin                     BeginDeletion
	resolvedTarget            TargetKind
	resolveErr, replayErr     error
	replay                    bool
	resolveCalls, appendCalls int
	changeBoundInput          bool
}

func (repository *planningRepository) ResolveDeletionReplay(context.Context, DeleteRequest) (BeginResult, bool, error) {
	return BeginResult{
		Operation: repository.operation,
		Existing:  repository.replay,
	}, repository.replay, repository.replayErr
}

func (repository *planningRepository) ResolveDeletionTarget(
	_ context.Context,
	requested TargetKind,
	id string,
) (DeletionTargetResolution, error) {
	repository.resolveCalls++
	if repository.resolveErr != nil {
		return DeletionTargetResolution{}, repository.resolveErr
	}
	if repository.resolvedTarget == TargetBackingService && requested == TargetProject {
		return DeletionTargetResolution{}, errs.New(
			errs.KindValidationFailed,
			"project deletion requires an ordinary Project",
		)
	}
	return DeletionTargetResolution{TargetKind: requested, ScopeKind: requested, ScopeID: id}, nil
}

func (repository *planningRepository) BeginDeletion(_ context.Context, begin BeginDeletion) (BeginResult, error) {
	repository.begin = begin
	repository.operation = Operation{ID: begin.OperationID, Kind: begin.OperationKind, TargetKind: begin.TargetKind,
		TargetID: begin.TargetID, TaskID: begin.TaskIDCandidate, Phase: PhasePlanning, CreatedAt: begin.CreatedAt,
		UpdatedAt: begin.CreatedAt, DeadlineAt: begin.DeadlineAt}
	return BeginResult{Operation: repository.operation}, nil
}

func (repository *planningRepository) OperationByTask(_ context.Context, taskID string) (Operation, error) {
	if taskID != repository.operation.TaskID {
		return Operation{}, errors.New("unexpected planning Task")
	}
	return repository.operation, nil
}

func (repository *planningRepository) FreezeMembership(context.Context, Operation) (FrozenMembership, error) {
	return repository.snapshot, nil
}

func (repository *planningRepository) BindPlan(
	_ context.Context,
	_ Operation,
	planned []PlannedAction,
) ([]Action, error) {
	bound := make([]Action, 0, len(planned))
	for _, item := range planned {
		procedure := Procedure{Kind: item.ProcedureInput.Kind}
		if input := item.ProcedureInput.AgentWork; input != nil {
			procedure.AgentWork = &AgentWorkProcedure{
				TypedProcedure: input.TypedProcedure,
				InputDigest:    input.InputDigest,
				Timeout:        input.Timeout,
			}
		} else {
			input := item.ProcedureInput.ControllerFinalizer
			procedure.ControllerFinalizer = &ControllerFinalizerProcedure{Finalizer: input.Finalizer, FixedInputRevision: input.FixedInputRevision,
				CompareTemplateDigest: digest(item.ID + "/compare"), MutationTemplateDigest: digest(item.ID + "/mutation"), PostconditionTemplateDigest: digest(item.ID + "/postcondition")}
		}
		action := Action{ID: item.ID, NodeID: item.NodeID, Ordinal: item.Ordinal, OperationID: item.OperationID,
			Kind: item.Kind, TargetKind: item.TargetKind, TargetID: item.TargetID, TargetRevision: item.TargetRevision,
			PrerequisiteOrdinals: append(
				[]int(nil),
				item.PrerequisiteOrdinals...), State: ActionPending, Procedure: procedure}
		if repository.changeBoundInput && action.Procedure.AgentWork != nil {
			action.Procedure.AgentWork.InputDigest = digest("changed")
		}
		bound = append(bound, action)
	}
	return bound, nil
}

func (repository *planningRepository) AppendPlan(
	_ context.Context,
	_ Operation,
	actions []Action,
	start int,
	sealed bool,
) (Operation, error) {
	if start != len(repository.actions) {
		return Operation{}, errors.New("planning cursor changed")
	}
	repository.appendCalls++
	repository.actions = append(repository.actions, actions...)
	repository.operation.PlanCursor, repository.operation.ActionCount = len(repository.actions), len(repository.actions)
	repository.operation.PlanSealed = sealed
	if sealed {
		repository.operation.Phase = PhaseExecuting
	}
	return repository.operation, nil
}

func (*planningRepository) GetDeletionTaskIDAtRevision(context.Context, TargetKind, string, int64) (*string, error) {
	return nil, errors.New("unexpected deletion-availability read")
}

func planningService(repository *planningRepository) *Service {
	return NewService(repository, fixedIDs{task: "task_01M15540AH211T0MA5QQ4KT50C"},
		fixedClock{now: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)})
}
