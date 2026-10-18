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
	ResolveDeletionReplay(context.Context, DeleteRequest) (BeginResult, bool, error)
	ResolveDeletionTarget(context.Context, TargetKind, string) (DeletionTargetResolution, error)
	BeginDeletion(context.Context, BeginDeletion) (BeginResult, error)
	OperationByTask(context.Context, string) (Operation, error)
	FreezeMembership(context.Context, Operation) (FrozenMembership, error)
	BindPlan(context.Context, Operation, []PlannedAction) ([]Action, error)
	AppendPlan(context.Context, Operation, []Action, int, bool) (Operation, error)
	GetDeletionTaskIDAtRevision(context.Context, TargetKind, string, int64) (*string, error)
}

type DeletionTargetResolution struct {
	TargetKind TargetKind
	ScopeKind  TargetKind
	ScopeID    string
}
type BeginDeletion struct {
	OperationID       string
	OperationKind     OperationKind
	TargetKind        TargetKind
	TargetID          string
	TaskIDCandidate   string
	IdempotencyKey    string
	IdempotencyIntent IdempotencyIntent
	CreatedAt         time.Time
	DeadlineAt        time.Time
	RetainUntil       *time.Time
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
	return idempotencyIntentWithScope(target, targetID, target, targetID)
}

func idempotencyIntentWithScope(
	target TargetKind,
	targetID string,
	scopeKind TargetKind,
	scopeID string,
) IdempotencyIntent {
	route := TenantDeleteRoute
	if target == TargetProject || target == TargetBackingService {
		route = ProjectDeleteRoute
	} else if target == TargetEnvironment {
		route = EnvironmentDeleteRoute
	}
	return IdempotencyIntent{
		Method:        "DELETE",
		RouteTemplate: route,
		ScopeKind:     scopeKind,
		ScopeID:       scopeID,
		PathBindings:  []PathBinding{{Name: "id", Value: targetID}},
	}
}

type BeginResult struct {
	Operation Operation
	Existing  bool
}

type IDGenerator interface{ NewTaskID() string }
type Clock interface{ Now() time.Time }

type StableIDGenerator struct{}

func (StableIDGenerator) NewTaskID() string { return ids.New(ids.KindTask) }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type Service struct {
	repository Repository
	ids        IDGenerator
	clock      Clock
}

func NewService(repository Repository, ids IDGenerator, clock Clock) *Service {
	return &Service{repository: repository, ids: ids, clock: clock}
}

func (s *Service) Delete(ctx context.Context, request DeleteRequest) (TaskAccepted, error) {
	if err := validateRequest(request); err != nil {
		return TaskAccepted{}, err
	}
	replay, found, err := s.repository.ResolveDeletionReplay(ctx, request)
	if err != nil {
		return TaskAccepted{}, err
	}
	if found {
		return TaskAccepted{TaskID: replay.Operation.TaskID, OperationID: replay.Operation.ID, Existing: true}, nil
	}
	resolved, err := s.repository.ResolveDeletionTarget(ctx, request.TargetKind, request.TargetID)
	if err != nil {
		return TaskAccepted{}, err
	}
	if !resolved.TargetKind.Valid() || !resolved.ScopeKind.Valid() {
		return TaskAccepted{}, errs.New(errs.KindInternal, "hierarchy deletion resolved an invalid target kind")
	}
	now := s.clock.Now().UTC()
	kind := operationForTarget(resolved.TargetKind)
	operationID := stableOperationID(kind, request.TargetID, request.IdempotencyKey)
	taskIDCandidate := s.ids.NewTaskID()
	result, err := s.repository.BeginDeletion(
		ctx,
		BeginDeletion{
			OperationID:     operationID,
			OperationKind:   kind,
			TargetKind:      resolved.TargetKind,
			TargetID:        request.TargetID,
			TaskIDCandidate: taskIDCandidate,
			IdempotencyKey:  request.IdempotencyKey,
			IdempotencyIntent: idempotencyIntentWithScope(
				request.TargetKind,
				request.TargetID,
				resolved.ScopeKind,
				resolved.ScopeID,
			),
			CreatedAt:  now,
			DeadlineAt: now.Add(OperationDeadline),
		},
	)
	if err != nil {
		return TaskAccepted{}, err
	}
	return TaskAccepted{
		TaskID:      result.Operation.TaskID,
		OperationID: result.Operation.ID,
		Existing:    result.Existing,
	}, nil
}

// PreparePlan advances one captured planning batch, without executing deletion.
// Publication sweeps call it again until the immutable action set is sealed.
func (s *Service) PreparePlan(ctx context.Context, taskID string) (bool, error) {
	operation, err := s.repository.OperationByTask(ctx, taskID)
	if err != nil {
		return false, err
	}
	if operation.Phase == PhasePlanning {
		return false, s.plan(ctx, operation)
	}
	if operation.Phase != PhaseExecuting || !operation.PlanSealed {
		return false, errs.New(errs.KindStateConflict, "hierarchy deletion has no sealed publication plan")
	}
	return true, nil
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
		return errs.Newf(
			errs.KindInternal,
			"hierarchy deletion %s plan cursor exceeds deterministic plan",
			operation.ID,
		)
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
	if start := operation.PlanCursor; start < len(plan.Actions) {
		end := start + PlanBatchSize
		if end > len(plan.Actions) {
			end = len(plan.Actions)
		}
		_, err = s.repository.AppendPlan(ctx, operation, actions[start:end], start, end == len(actions))
		return err
	}
	_, err = s.repository.AppendPlan(ctx, operation, nil, operation.PlanCursor, true)
	return err
}
