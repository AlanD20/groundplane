// Package hierarchydeletion owns durable, child-first deletion of hierarchy
// aggregates. Public handlers only start an operation; this package is the
// single orchestration authority for Tenant, Project, and Environment deletes.
package hierarchydeletion

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type TargetKind string

const (
	TargetTenant         TargetKind = "tenant"
	TargetProject        TargetKind = "project"
	TargetEnvironment    TargetKind = "environment"
	TargetBackingService TargetKind = "backing-service"
)

func (k TargetKind) Valid() bool {
	switch k {
	case TargetTenant, TargetProject, TargetEnvironment, TargetBackingService:
		return true
	default:
		return false
	}
}

type OperationKind string

const (
	OperationTenantDelete      OperationKind = "tenant.delete"
	OperationProjectDelete     OperationKind = "project.delete"
	OperationEnvironmentDelete OperationKind = "environment.delete"
	OperationBackingDelete     OperationKind = "backing.delete"
)

func (k OperationKind) Valid() bool {
	switch k {
	case OperationTenantDelete, OperationProjectDelete, OperationEnvironmentDelete, OperationBackingDelete:
		return true
	default:
		return false
	}
}

func operationForTarget(target TargetKind) OperationKind {
	switch target {
	case TargetTenant:
		return OperationTenantDelete
	case TargetProject:
		return OperationProjectDelete
	case TargetEnvironment:
		return OperationEnvironmentDelete
	case TargetBackingService:
		return OperationBackingDelete
	default:
		return ""
	}
}

type Phase string

const (
	PhasePlanning    Phase = "planning"
	PhaseExecuting   Phase = "executing"
	PhaseSummarizing Phase = "summarizing"
	PhaseFinalizing  Phase = "finalizing"
	PhaseRetained    Phase = "retained"
)

type ActionState string

const (
	ActionPending   ActionState = "pending"
	ActionRunning   ActionState = "running"
	ActionSucceeded ActionState = "succeeded"
	ActionFailed    ActionState = "failed"
)

type DeleteRequest struct {
	TargetKind     TargetKind
	TargetID       string
	IdempotencyKey string
}
type TaskAccepted struct {
	TaskID      string
	OperationID string
	Existing    bool
}

type Operation struct {
	ID                string
	TaskOperationID   string
	Kind              OperationKind
	TargetKind        TargetKind
	TargetID          string
	TaskID            string
	Phase             Phase
	SnapshotRevision  int64
	CoordinationEpoch int64
	DeadlineAt        time.Time
	RetainUntil       *time.Time
	PlanSealed        bool
	PlanCursor        int
	ActionCount       int
	SucceededCount    int
	FailedCount       int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type MembershipNode struct {
	NodeID              string
	TargetKind          ActionTargetKind
	ID                  string
	ActionKind          ActionKind
	TargetRevision      int64
	PrerequisiteNodeIDs []string
	ProcedureInput      ProcedureInput
}
type ReverseReference struct {
	SourceKind ActionTargetKind
	SourceID   string
	TargetKind ActionTargetKind
	TargetID   string
}
type FrozenMembership struct {
	Revision                int64
	CoordinationEpoch       int64
	RootRevision            int64
	RootProcedureInput      ControllerFinalizerInput
	RootPrerequisiteNodeIDs []string
	Nodes                   []MembershipNode
	ReverseReferences       []ReverseReference
}
type Action struct {
	ID                   string
	NodeID               string
	Ordinal              int
	OperationID          string
	Kind                 ActionKind
	TargetKind           ActionTargetKind
	TargetID             string
	TargetRevision       int64
	PrerequisiteOrdinals []int
	State                ActionState
	Attempt              int
	Procedure            Procedure
	UpdatedAt            time.Time
}
type PlannedAction struct {
	ID                   string
	NodeID               string
	Ordinal              int
	OperationID          string
	Kind                 ActionKind
	TargetKind           ActionTargetKind
	TargetID             string
	TargetRevision       int64
	PrerequisiteOrdinals []int
	ProcedureInput       ProcedureInput
}
type Summary struct {
	ActionCount             int
	SucceededCount          int
	FailedCount             int
	ReceiptCount            int
	ReceiptSummaryDigest    string
	CompletionSummaryDigest string
	Completed               bool
}
type Plan struct{ Actions []PlannedAction }

const (
	PlanBatchSize      = 47
	ExecutionBatchSize = 1
	OperationDeadline  = 6 * time.Hour
	TaskRetention      = 90 * 24 * time.Hour
)

func stableOperationID(kind OperationKind, targetID, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(string(kind) + "\x00" + targetID + "\x00" + idempotencyKey))
	return "del_" + hex.EncodeToString(sum[:16])
}

func stableActionID(
	operationID string,
	ordinal int,
	kind ActionKind,
	targetKind ActionTargetKind,
	targetID string,
) string {
	value := fmt.Sprintf("%s\x00%020d\x00%s\x00%s\x00%s", operationID, ordinal, kind, targetKind, targetID)
	sum := sha256.Sum256([]byte(value))
	return "act_" + hex.EncodeToString(sum[:16])
}
func taskOperationID(taskID string) (string, error) {
	separator := strings.IndexByte(taskID, '_')
	if separator <= 0 || separator == len(taskID)-1 {
		return "", errs.Newf(errs.KindInternal, "hierarchy deletion task id %q has no canonical suffix", taskID)
	}
	return "op_" + taskID[separator+1:], nil
}

func validateRequest(request DeleteRequest) error {
	if request.TargetKind != TargetTenant && request.TargetKind != TargetProject &&
		request.TargetKind != TargetEnvironment {
		return errs.Newf(errs.KindValidationFailed, "hierarchy deletion target kind %q is invalid", request.TargetKind)
	}
	if strings.TrimSpace(request.TargetID) == "" {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target id is required")
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion idempotency key is required")
	}
	return nil
}

func validateMembership(snapshot FrozenMembership) error {
	if snapshot.Revision <= 0 {
		return errs.New(errs.KindInternal, "hierarchy deletion membership revision must be positive")
	}
	if snapshot.CoordinationEpoch <= 0 {
		return errs.New(errs.KindInternal, "hierarchy deletion coordination epoch must be positive")
	}
	seen := make(map[string]struct{}, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if strings.TrimSpace(node.NodeID) == "" || !node.TargetKind.Valid() || strings.TrimSpace(node.ID) == "" ||
			!node.ActionKind.Valid() ||
			node.TargetRevision <= 0 {
			return errs.New(errs.KindInternal, "hierarchy deletion membership contains an invalid node")
		}
		if err := node.ProcedureInput.Validate(node.ActionKind, node.TargetKind, node.ID, node.TargetRevision); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if _, ok := seen[node.NodeID]; ok {
			return errs.Newf(errs.KindInternal, "hierarchy deletion membership contains duplicate node %s", node.NodeID)
		}
		seen[node.NodeID] = struct{}{}
	}
	return nil
}

func BuildPlan(operation Operation, snapshot FrozenMembership) (Plan, error) {
	if err := validateMembership(snapshot); err != nil {
		return Plan{}, err
	}
	if len(snapshot.ReverseReferences) > 0 {
		ref := snapshot.ReverseReferences[0]
		return Plan{}, errs.Newf(
			errs.KindStateConflict,
			"hierarchy deletion is fenced by %s %s referencing %s %s",
			ref.SourceKind,
			ref.SourceID,
			ref.TargetKind,
			ref.TargetID,
		)
	}
	finalKind := ActionTenantFinalize
	if operation.Kind == OperationBackingDelete || operation.TargetKind == TargetBackingService {
		finalKind = ActionBackingServiceFinalize
	} else if operation.TargetKind == TargetProject {
		finalKind = ActionProjectFinalize
	} else if operation.TargetKind == TargetEnvironment {
		finalKind = ActionEnvironmentFinalize
	}
	rootKind := ActionTargetKind(operation.TargetKind)
	rootRevision := snapshot.RootRevision
	if rootRevision <= 0 {
		rootRevision = snapshot.Revision
	}
	rootInput := ProcedureInput{Kind: ProcedureControllerFinalizer, ControllerFinalizer: &snapshot.RootProcedureInput}
	if err := rootInput.Validate(finalKind, rootKind, operation.TargetID, rootRevision); err != nil {
		return Plan{}, errs.Wrap(errs.KindInternal, err)
	}
	nodes := append([]MembershipNode(nil), snapshot.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	byID := make(map[string]int, len(nodes))
	for index, node := range nodes {
		byID[node.NodeID] = index
	}
	actions := make([]PlannedAction, 0, len(nodes)+1)
	state := make([]uint8, len(nodes))
	ordinals := make(map[int]int, len(nodes))
	var visit func(int) error
	visit = func(index int) error {
		if state[index] == 1 {
			return errs.New(errs.KindInternal, "hierarchy deletion membership contains a cycle")
		}
		if state[index] == 2 {
			return nil
		}
		state[index] = 1
		node := nodes[index]
		prerequisiteIDs := append([]string(nil), node.PrerequisiteNodeIDs...)
		sort.Strings(prerequisiteIDs)
		prerequisites := make([]int, 0, len(prerequisiteIDs))
		for _, prerequisiteID := range prerequisiteIDs {
			prerequisiteIndex, ok := byID[prerequisiteID]
			if !ok {
				return errs.Newf(
					errs.KindInternal,
					"hierarchy deletion node %s requires missing node %s",
					node.NodeID,
					prerequisiteID,
				)
			}
			if err := visit(prerequisiteIndex); err != nil {
				return err
			}
			prerequisites = append(prerequisites, ordinals[prerequisiteIndex])
		}
		ordinal := len(actions)
		ordinals[index] = ordinal
		actions = append(
			actions,
			PlannedAction{
				ID:                   stableActionID(operation.ID, ordinal, node.ActionKind, node.TargetKind, node.ID),
				NodeID:               node.NodeID,
				Ordinal:              ordinal,
				OperationID:          operation.ID,
				Kind:                 node.ActionKind,
				TargetKind:           node.TargetKind,
				TargetID:             node.ID,
				TargetRevision:       node.TargetRevision,
				PrerequisiteOrdinals: prerequisites,
				ProcedureInput:       node.ProcedureInput,
			},
		)
		state[index] = 2
		return nil
	}
	rootIDs := append([]string(nil), snapshot.RootPrerequisiteNodeIDs...)
	sort.Strings(rootIDs)
	rootOrdinals := make([]int, 0, len(rootIDs))
	for _, rootID := range rootIDs {
		rootIndex, ok := byID[rootID]
		if !ok {
			return Plan{}, errs.Newf(errs.KindInternal, "hierarchy deletion root requires missing node %s", rootID)
		}
		if err := visit(rootIndex); err != nil {
			return Plan{}, err
		}
		rootOrdinals = append(rootOrdinals, ordinals[rootIndex])
	}
	if len(actions) != len(nodes) {
		return Plan{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion membership contains an orphan outside the aggregate",
		)
	}
	ordinal := len(actions)
	final := PlannedAction{
		ID:                   stableActionID(operation.ID, ordinal, finalKind, rootKind, operation.TargetID),
		NodeID:               "root.finalize",
		Ordinal:              ordinal,
		OperationID:          operation.ID,
		Kind:                 finalKind,
		TargetKind:           rootKind,
		TargetID:             operation.TargetID,
		TargetRevision:       rootRevision,
		PrerequisiteOrdinals: rootOrdinals,
		ProcedureInput:       rootInput,
	}
	actions = append(actions, final)
	return Plan{Actions: actions}, nil
}
