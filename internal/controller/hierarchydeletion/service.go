package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Repository is a domain seam because an operation and Task must survive restarts.
// Implementations compare-and-swap every transition and enforce the revision fence.
type Repository interface {
	BeginDeletion(context.Context, BeginDeletion) (BeginResult, error)
	OperationByTask(context.Context, string) (Operation, error)
	FreezeMembership(context.Context, Operation) (FrozenMembership, error)
	BindPlan(context.Context, Operation, []PlannedAction) ([]Action, error)
	AppendPlan(context.Context, Operation, []Action, int, bool) (Operation, error)
	ReadyActions(context.Context, Operation, int) ([]Action, error)
	ConsumeAgentTerminal(context.Context, Operation, Action, AgentTerminalProof) (Operation, error)
	CompleteControllerAction(context.Context, Operation, Action) (Operation, error)
	PrepareRootFinalization(context.Context, Operation, Action) (Operation, error)
	GetDeletionTaskIDAtRevision(context.Context, TargetKind, string, int64) (*string, error)
}
type BeginDeletion struct {
	OperationID              string
	TaskOperationIDCandidate string
	OperationKind            OperationKind
	TargetKind               TargetKind
	TargetID                 string
	TaskIDCandidate          string
	IdempotencyKey           string
	IdempotencyIntent        IdempotencyIntent
	CreatedAt                time.Time
	DeadlineAt               time.Time
	RetainUntil              *time.Time
}

const (
	TenantDeleteRoute      = "/tenants/{id}"
	ProjectDeleteRoute     = "/projects/{id}"
	EnvironmentDeleteRoute = "/environments/{id}"
)

type PathBinding struct {
	Name  string
	Value string
}
type IdempotencyIntent struct {
	Method        string
	RouteTemplate string
	ScopeKind     TargetKind
	ScopeID       string
	PathBindings  []PathBinding
}

func idempotencyIntent(target TargetKind, targetID string) IdempotencyIntent {
	route := TenantDeleteRoute
	if target == TargetProject {
		route = ProjectDeleteRoute
	} else if target == TargetEnvironment {
		route = EnvironmentDeleteRoute
	}
	return IdempotencyIntent{Method: "DELETE", RouteTemplate: route, ScopeKind: target, ScopeID: targetID, PathBindings: []PathBinding{{Name: "id", Value: targetID}}}
}

type BeginResult struct {
	Operation Operation
	Existing  bool
}

// ActionExecutor must be idempotent by Action.ID. A crash after the effect but
// before its receipt deliberately invokes the same action identity again.
type AgentActionExecutor interface {
	Execute(context.Context, Operation, Action) (AgentTerminalProof, error)
}
type IDGenerator interface{ NewTaskID() string }
type Clock interface{ Now() time.Time }

type StableIDGenerator struct{}

func (StableIDGenerator) NewTaskID() string { return ids.New(ids.KindTask) }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type Service struct {
	repository Repository
	executor   AgentActionExecutor
	ids        IDGenerator
	clock      Clock
}

func NewService(repository Repository, executor AgentActionExecutor, ids IDGenerator, clock Clock) *Service {
	return &Service{repository: repository, executor: executor, ids: ids, clock: clock}
}

func (s *Service) Delete(ctx context.Context, request DeleteRequest) (TaskAccepted, error) {
	if err := validateRequest(request); err != nil {
		return TaskAccepted{}, err
	}
	now := s.clock.Now().UTC()
	kind := operationForTarget(request.TargetKind)
	operationID := stableOperationID(kind, request.TargetID, request.IdempotencyKey)
	taskIDCandidate := s.ids.NewTaskID()
	taskOperationID, err := taskOperationID(taskIDCandidate)
	if err != nil {
		return TaskAccepted{}, err
	}
	result, err := s.repository.BeginDeletion(ctx, BeginDeletion{OperationID: operationID, TaskOperationIDCandidate: taskOperationID, OperationKind: kind, TargetKind: request.TargetKind, TargetID: request.TargetID, TaskIDCandidate: taskIDCandidate, IdempotencyKey: request.IdempotencyKey, IdempotencyIntent: idempotencyIntent(request.TargetKind, request.TargetID), CreatedAt: now, DeadlineAt: now.Add(OperationDeadline)})
	if err != nil {
		return TaskAccepted{}, err
	}
	return TaskAccepted{TaskID: result.Operation.TaskID, OperationID: result.Operation.ID, Existing: result.Existing}, nil
}

func (s *Service) Execute(ctx context.Context, taskID string) error {
	for {
		operation, err := s.repository.OperationByTask(ctx, taskID)
		if err != nil {
			return err
		}
		if s.clock.Now().After(operation.DeadlineAt) && operation.Phase != PhaseRetained {
			return errs.Newf(errs.KindStateConflict, "hierarchy deletion %s exceeded its deadline", operation.ID)
		}
		switch operation.Phase {
		case PhasePlanning:
			if err := s.plan(ctx, operation); err != nil {
				return err
			}
		case PhaseExecuting:
			progressed, err := s.executeBatch(ctx, operation)
			if err != nil {
				return err
			}
			if !progressed {
				return errs.Newf(errs.KindStateConflict, "hierarchy deletion %s has no ready action but is incomplete", operation.ID)
			}
		case PhaseFinalizing:
			return nil
		case PhaseSummarizing:
			return errs.Newf(errs.KindInternal, "hierarchy deletion %s stopped in obsolete pre-root summary phase", operation.ID)
		case PhaseRetained:
			return nil
		default:
			return errs.Newf(errs.KindInternal, "hierarchy deletion %s has invalid phase %q", operation.ID, operation.Phase)
		}
	}
}
func (s *Service) plan(ctx context.Context, operation Operation) error {
	snapshot, err := s.repository.FreezeMembership(ctx, operation)
	if err != nil {
		return err
	}
	plan, err := BuildPlan(operation, snapshot)
	if err != nil {
		return err
	}
	if operation.PlanCursor > len(plan.Actions) {
		return errs.Newf(errs.KindInternal, "hierarchy deletion %s plan cursor exceeds deterministic plan", operation.ID)
	}
	actions, err := s.repository.BindPlan(ctx, operation, plan.Actions)
	if err != nil {
		return err
	}
	if len(actions) != len(plan.Actions) {
		return errs.Newf(errs.KindInternal, "hierarchy deletion %s binder changed the plan length", operation.ID)
	}
	for index := range actions {
		if err = validateBoundAction(plan.Actions[index], actions[index]); err != nil {
			return err
		}
	}
	for start := operation.PlanCursor; start < len(plan.Actions); start += PlanBatchSize {
		end := start + PlanBatchSize
		if end > len(plan.Actions) {
			end = len(plan.Actions)
		}
		operation, err = s.repository.AppendPlan(ctx, operation, actions[start:end], start, end == len(actions))
		if err != nil {
			return err
		}
	}
	if len(plan.Actions) == 0 {
		_, err = s.repository.AppendPlan(ctx, operation, nil, 0, true)
	}
	return err
}
func (s *Service) executeBatch(ctx context.Context, operation Operation) (bool, error) {
	actions, err := s.repository.ReadyActions(ctx, operation, ExecutionBatchSize)
	if err != nil {
		return false, err
	}
	for _, action := range actions {
		if isRootFinalizer(operation, action) {
			if _, err = s.repository.PrepareRootFinalization(ctx, operation, action); err != nil {
				return true, err
			}
			return true, nil
		}
		if action.Procedure.Kind == ProcedureControllerFinalizer {
			if _, err = s.repository.CompleteControllerAction(ctx, operation, action); err != nil {
				return true, err
			}
			continue
		}
		proof, executeErr := s.executor.Execute(ctx, operation, action)
		if executeErr != nil {
			return true, executeErr
		}
		if executeErr = proof.Validate(action); executeErr != nil {
			return true, executeErr
		}
		if _, executeErr = s.repository.ConsumeAgentTerminal(ctx, operation, action, proof); executeErr != nil {
			return true, executeErr
		}
		if proof.Terminal != AgentTerminalCompleted {
			return true, errs.Newf(errs.KindStateConflict, "hierarchy deletion action %s ended %s", action.ID, proof.Terminal)
		}
	}
	return len(actions) != 0, nil
}

func isRootFinalizer(operation Operation, action Action) bool {
	if !operation.PlanSealed || operation.ActionCount <= 0 || action.Ordinal != operation.ActionCount-1 || action.TargetID != operation.TargetID || string(action.TargetKind) != string(operation.TargetKind) || action.Procedure.Kind != ProcedureControllerFinalizer {
		return false
	}
	switch operation.TargetKind {
	case TargetTenant:
		return action.Kind == ActionTenantFinalize
	case TargetProject:
		return action.Kind == ActionProjectFinalize
	case TargetEnvironment:
		return action.Kind == ActionEnvironmentFinalize
	case TargetBackingService:
		return action.Kind == ActionBackingServiceFinalize
	default:
		return false
	}
}
