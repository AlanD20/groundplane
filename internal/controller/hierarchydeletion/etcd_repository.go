package hierarchydeletion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	hierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EtcdRepository struct {
	journal     *etcdinfra.HierarchyDeletionRepository
	idempotency *etcdinfra.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	clock       Clock
}

func NewEtcdRepository(
	journal *etcdinfra.HierarchyDeletionRepository,
	idempotency *etcdinfra.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	clock Clock,
) (*EtcdRepository, error) {
	if journal == nil || idempotency == nil || coordinator == nil || clock == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion etcd adapter dependencies are required")
	}
	return &EtcdRepository{
		journal: journal, idempotency: idempotency, coordinator: coordinator, clock: clock,
	}, nil
}

func (repository *EtcdRepository) ResolveDeletionTarget(
	ctx context.Context,
	requested TargetKind,
	targetID string,
) (DeletionTargetResolution, error) {
	resolved, err := repository.journal.ResolveDeletionTarget(
		ctx, hierarchydeletion.HierarchyDeletionTargetKind(requested), targetID,
	)
	if err != nil {
		return DeletionTargetResolution{}, err
	}
	scopeKind, err := domainScopeKind(resolved.ScopeKind)
	if err != nil {
		return DeletionTargetResolution{}, err
	}
	return DeletionTargetResolution{
		TargetKind: TargetKind(resolved.TargetKind), ScopeKind: scopeKind, ScopeID: resolved.ScopeID,
	}, nil
}

func (repository *EtcdRepository) ResolveTargetKind(
	ctx context.Context,
	requested TargetKind,
	targetID string,
) (TargetKind, error) {
	resolved, err := repository.ResolveDeletionTarget(ctx, requested, targetID)
	if err != nil {
		return "", err
	}
	return resolved.TargetKind, nil
}

func (repository *EtcdRepository) BeginDeletion(
	ctx context.Context,
	begin BeginDeletion,
) (BeginResult, error) {
	protected, marker, err := repository.protectBegin(ctx, begin)
	if err != nil {
		return BeginResult{}, err
	}
	defer protected.Destroy()
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	request, err := replayRequestFromBegin(begin)
	if err != nil {
		return BeginResult{}, err
	}
	if replayed, found, resolveErr := repository.resolveReplay(ctx, request, protected, &marker.Locator, false); resolveErr != nil {
		return BeginResult{}, resolveErr
	} else if found {
		return replayed, nil
	}
	if replayed, found, resolveErr := repository.resolveReplayAfterMiss(
		ctx, request, protected, marker.Locator,
	); resolveErr != nil {
		return BeginResult{}, resolveErr
	} else if found {
		return replayed, nil
	}
	created, err := repository.journal.Begin(ctx, etcdinfra.HierarchyDeletionBegin{
		OperationID: begin.OperationID, TaskOperationID: begin.TaskOperationIDCandidate,
		OperationKind: hierarchydeletion.HierarchyDeletionOperationKind(begin.OperationKind),
		TargetKind:    hierarchydeletion.HierarchyDeletionTargetKind(begin.TargetKind), TargetID: begin.TargetID,
		TaskID: begin.TaskIDCandidate, IdempotencyHash: idempotencyKeyHash(begin.IdempotencyKey),
		Marker: marker, CreatedAt: begin.CreatedAt, DeadlineAt: begin.DeadlineAt,
	})
	if err != nil {
		if !isUnknownBeginError(err) {
			return BeginResult{}, err
		}
		if replayed, found, resolveErr := repository.resolveReplay(ctx, request, protected, &marker.Locator, false); resolveErr != nil {
			return BeginResult{}, resolveErr
		} else if found {
			return replayed, nil
		}
		if replayed, found, resolveErr := repository.resolveReplayAfterMiss(
			ctx, request, protected, marker.Locator,
		); resolveErr != nil {
			return BeginResult{}, resolveErr
		} else if found {
			return replayed, nil
		}
		return BeginResult{}, err
	}
	if !created.Existing {
		return BeginResult{Operation: operationFromEtcd(created.Operation), Existing: false}, nil
	}
	if replayed, found, resolveErr := repository.resolveReplay(ctx, request, protected, &marker.Locator, false); resolveErr != nil {
		return BeginResult{}, resolveErr
	} else if found {
		return replayed, nil
	}
	if replayed, found, resolveErr := repository.resolveReplayAfterMiss(
		ctx, request, protected, marker.Locator,
	); resolveErr != nil {
		return BeginResult{}, resolveErr
	} else if found {
		return replayed, nil
	}
	return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion publication has no replay index")
}

func (repository *EtcdRepository) OperationByTask(
	ctx context.Context,
	taskID string,
) (Operation, error) {
	operation, err := repository.journal.OperationByTask(ctx, taskID)
	return operationFromEtcd(operation), err
}

func (repository *EtcdRepository) FreezeMembership(
	ctx context.Context,
	operation Operation,
) (FrozenMembership, error) {
	frozen, err := repository.journal.FreezeMembership(ctx, operationToEtcd(operation))
	if err != nil {
		return FrozenMembership{}, err
	}
	nodes := make([]MembershipNode, len(frozen.Nodes))
	for index, node := range frozen.Nodes {
		nodes[index] = MembershipNode{
			NodeID: node.NodeID, TargetKind: ActionTargetKind(node.TargetKind), ID: node.TargetID,
			ActionKind: ActionKind(node.ActionKind), TargetRevision: node.TargetRevision,
			PrerequisiteNodeIDs: append([]string(nil), node.PrerequisiteNodeIDs...),
			ProcedureInput:      procedureInputFromEtcd(node.ProcedureInput),
		}
	}
	root := ControllerFinalizerInput{
		Finalizer:          frozen.RootProcedureInput.Finalizer,
		TargetKind:         ActionTargetKind(frozen.RootProcedureInput.TargetKind),
		TargetID:           frozen.RootProcedureInput.TargetID,
		FixedInputRevision: frozen.RootProcedureInput.FixedInputRevision,
		FixedInputDigest:   frozen.RootProcedureInput.FixedInputDigest,
		BatchOrdinal:       int(frozen.RootProcedureInput.BatchOrdinal),
		BatchCount:         int(frozen.RootProcedureInput.BatchCount),
	}
	return FrozenMembership{
		Revision: frozen.Revision, CoordinationEpoch: frozen.CoordinationEpoch,
		RootRevision: frozen.RootRevision, RootProcedureInput: root,
		RootPrerequisiteNodeIDs: append([]string(nil), frozen.RootPrerequisiteNodeIDs...), Nodes: nodes,
	}, nil
}

func (repository *EtcdRepository) BindPlan(
	ctx context.Context,
	operation Operation,
	planned []PlannedAction,
) ([]Action, error) {
	encoded := make([]etcdinfra.HierarchyDeletionPlannedAction, len(planned))
	for index, action := range planned {
		encoded[index] = plannedActionToEtcd(action)
	}
	bound, err := repository.journal.BindActions(ctx, operationToEtcd(operation), encoded)
	if err != nil {
		return nil, err
	}
	result := make([]Action, len(bound))
	for index := range bound {
		result[index] = actionFromEtcd(bound[index], operationToEtcd(operation))
	}
	return result, nil
}

func (repository *EtcdRepository) AppendPlan(
	ctx context.Context,
	operation Operation,
	actions []Action,
	start int,
	sealed bool,
) (Operation, error) {
	encoded := make([]hierarchydeletion.HierarchyDeletionAction, len(actions))
	for index, action := range actions {
		encoded[index] = actionToEtcd(action)
	}
	updated, err := repository.journal.AppendActions(ctx, operationToEtcd(operation), encoded, int64(start), sealed)
	return operationFromEtcd(updated), err
}

func (repository *EtcdRepository) ReadyActions(
	ctx context.Context,
	operation Operation,
	limit int,
) ([]Action, error) {
	if limit <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion action limit is invalid")
	}
	action, current, err := repository.journal.ReadyAction(ctx, operationToEtcd(operation))
	if err != nil || action == nil {
		return nil, err
	}
	return []Action{actionFromEtcd(*action, current)}, nil
}

func (repository *EtcdRepository) ConsumeAgentTerminal(
	ctx context.Context,
	operation Operation,
	action Action,
	proof AgentTerminalProof,
) (Operation, error) {
	updated, err := repository.journal.ConsumeAgentTerminal(
		ctx, operationToEtcd(operation), actionToEtcd(action),
		etcdinfra.HierarchyDeletionAgentTerminalProof{
			ChildOperationID: proof.ChildOperationID, AttemptID: proof.AttemptID,
			TaskID: proof.TaskID, AssignmentID: proof.AssignmentID,
			AttemptGeneration: proof.AttemptGeneration, ReceiptRevision: proof.ReceiptRevision,
			ReceiptDigest: proof.ReceiptDigest, ProgressKey: proof.ProgressKey,
			ProgressDigest: proof.ProgressDigest, TerminalTaskDigest: proof.TerminalTaskDigest,
			Terminal:     hierarchydeletion.HierarchyDeletionAgentTerminal(proof.Terminal),
			ResultDigest: proof.ResultDigest, ErrorDigest: proof.ErrorDigest,
			CheckpointDigest: proof.CheckpointDigest,
		}, repository.clock.Now().UTC(),
	)
	return operationFromEtcd(updated), err
}

func (repository *EtcdRepository) CompleteControllerAction(
	ctx context.Context,
	operation Operation,
	action Action,
) (Operation, error) {
	updated, err := repository.journal.CompleteControllerAction(
		ctx, operationToEtcd(operation), actionToEtcd(action), repository.clock.Now().UTC(),
	)
	return operationFromEtcd(updated), err
}

func (repository *EtcdRepository) PrepareRootFinalization(
	ctx context.Context,
	operation Operation,
	action Action,
) (Operation, error) {
	updated, err := repository.journal.PrepareRootFinalization(
		ctx, operationToEtcd(operation), actionToEtcd(action), repository.clock.Now().UTC(),
	)
	return operationFromEtcd(updated), err
}

func (repository *EtcdRepository) GetDeletionTaskIDAtRevision(
	ctx context.Context,
	target TargetKind,
	id string,
	revision int64,
) (*string, error) {
	return repository.journal.GetDeletionTaskIDAtRevision(
		ctx, hierarchydeletion.HierarchyDeletionTargetKind(target), id, revision,
	)
}

func (repository *EtcdRepository) Execute(
	ctx context.Context,
	operation Operation,
	action Action,
) (AgentTerminalProof, error) {
	if action.Procedure.Kind != ProcedureAgentChild || action.Procedure.AgentChild == nil {
		return AgentTerminalProof{}, errs.New(errs.KindValidationFailed, "hierarchy deletion Agent action is invalid")
	}
	etcdOperation := operationToEtcd(operation)
	etcdAction := actionToEtcd(action)
	if _, err := repository.journal.PublishOrResumeAgentAction(
		ctx, etcdOperation, etcdAction, repository.clock.Now().UTC(),
	); err != nil {
		return AgentTerminalProof{}, err
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		proof, err := repository.journal.AgentTerminalProof(ctx, etcdOperation, etcdAction)
		if err != nil {
			return AgentTerminalProof{}, err
		}
		if proof != nil {
			return AgentTerminalProof{
				ChildOperationID: proof.ChildOperationID, AttemptID: proof.AttemptID,
				TaskID: proof.TaskID, AssignmentID: proof.AssignmentID,
				AttemptGeneration: proof.AttemptGeneration, ReceiptRevision: proof.ReceiptRevision,
				ReceiptDigest: proof.ReceiptDigest, ProgressKey: proof.ProgressKey,
				ProgressDigest: proof.ProgressDigest, TerminalTaskDigest: proof.TerminalTaskDigest,
				Terminal: AgentTerminal(proof.Terminal), ResultDigest: proof.ResultDigest,
				ErrorDigest: proof.ErrorDigest, CheckpointDigest: proof.CheckpointDigest,
			}, nil
		}
		select {
		case <-ctx.Done():
			return AgentTerminalProof{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func operationFromEtcd(value etcdinfra.HierarchyDeletionOperation) Operation {
	operation := Operation{
		ID: value.Tombstone.OperationID, TaskOperationID: value.Tombstone.TaskOperationID,
		Kind: OperationKind(value.Tombstone.OperationKind), TargetKind: TargetKind(value.Tombstone.TargetKind),
		TargetID: value.Tombstone.TargetID, TaskID: value.Tombstone.CurrentTaskID,
		Phase: Phase(value.Tombstone.Phase), SnapshotRevision: value.Tombstone.SnapshotRevision,
		CoordinationEpoch: value.Tombstone.DeletionEpoch, DeadlineAt: value.Tombstone.AttemptDeadline,
		PlanSealed: value.Tombstone.PlanCount != nil && value.Tombstone.PlanDigest != nil,
		PlanCursor: int(value.PlanCursor), SucceededCount: int(value.SucceededCount),
		FailedCount: int(value.FailedCount), CreatedAt: value.Tombstone.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
	if value.Tombstone.PlanCount != nil {
		operation.ActionCount = int(*value.Tombstone.PlanCount)
	}
	if value.Tombstone.Terminal != nil {
		retainUntil := value.Tombstone.Terminal.RetainUntil
		operation.RetainUntil = &retainUntil
	}
	return operation
}

func operationToEtcd(value Operation) etcdinfra.HierarchyDeletionOperation {
	operation := etcdinfra.HierarchyDeletionOperation{
		Tombstone: hierarchydeletion.HierarchyDeletionTombstone{
			OperationID: value.ID, TaskOperationID: value.TaskOperationID,
			OperationKind: hierarchydeletion.HierarchyDeletionOperationKind(value.Kind),
			TargetKind:    hierarchydeletion.HierarchyDeletionTargetKind(value.TargetKind), TargetID: value.TargetID,
			CurrentTaskID: value.TaskID, SnapshotRevision: value.SnapshotRevision,
			DeletionEpoch: value.CoordinationEpoch, Phase: hierarchydeletion.HierarchyDeletionPhase(value.Phase),
			AttemptDeadline: value.DeadlineAt, CreatedAt: value.CreatedAt,
			Checkpoint: hierarchydeletion.HierarchyDeletionCheckpoint{
				NextOrdinal: int64(value.PlanCursor), CompletedCount: int64(value.SucceededCount),
			},
		},
		PlanCursor: int64(value.PlanCursor), SucceededCount: int64(value.SucceededCount),
		FailedCount: int64(value.FailedCount), UpdatedAt: value.UpdatedAt,
	}
	if value.PlanSealed {
		count := int64(value.ActionCount)
		operation.Tombstone.PlanCount = &count
	}
	return operation
}

func actionToEtcd(value Action) hierarchydeletion.HierarchyDeletionAction {
	converted := hierarchydeletion.HierarchyDeletionAction{
		Schema: 1, ParentOperationID: value.OperationID, NodeID: value.NodeID,
		Ordinal: int64(value.Ordinal), ActionKind: hierarchydeletion.HierarchyDeletionActionKind(value.Kind),
		TargetKind: hierarchydeletion.HierarchyDeletionActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: make([]int64, len(value.PrerequisiteOrdinals)),
		ProcedureKind: hierarchydeletion.HierarchyDeletionProcedureKind(value.Procedure.Kind),
	}
	for index, ordinal := range value.PrerequisiteOrdinals {
		converted.PrerequisiteOrdinals[index] = int64(ordinal)
	}
	if value.Procedure.AgentChild != nil {
		converted.AgentProcedure = &hierarchydeletion.HierarchyDeletionAgentProcedure{
			ChildOperationID: value.Procedure.AgentChild.ChildOperationID,
			TaskType:         taskjournal.TaskType(value.Procedure.AgentChild.TaskType),
			TypedProcedure:   value.Procedure.AgentChild.TypedProcedure,
			InputDigest:      value.Procedure.AgentChild.InputDigest,
			TimeoutSeconds:   int64(value.Procedure.AgentChild.Timeout.Seconds()),
		}
	}
	if value.Procedure.ControllerFinalizer != nil {
		procedure := value.Procedure.ControllerFinalizer
		converted.ControllerProcedure = &hierarchydeletion.HierarchyDeletionControllerProcedure{
			Finalizer: procedure.Finalizer, FixedInputRevision: procedure.FixedInputRevision,
			CompareTemplateDigest:       procedure.CompareTemplateDigest,
			MutationTemplateDigest:      procedure.MutationTemplateDigest,
			PostconditionTemplateDigest: procedure.PostconditionTemplateDigest,
		}
	}
	return converted
}

func actionFromEtcd(
	value hierarchydeletion.HierarchyDeletionAction,
	operation etcdinfra.HierarchyDeletionOperation,
) Action {
	prerequisites := make([]int, len(value.PrerequisiteOrdinals))
	for index, ordinal := range value.PrerequisiteOrdinals {
		prerequisites[index] = int(ordinal)
	}
	domainOperation := operationFromEtcd(operation)
	return Action{
		ID: stableActionID(
			domainOperation.ID,
			int(value.Ordinal),
			ActionKind(value.ActionKind),
			ActionTargetKind(value.TargetKind),
			value.TargetID,
		),
		NodeID: value.NodeID, Ordinal: int(value.Ordinal), OperationID: value.ParentOperationID,
		Kind: ActionKind(value.ActionKind), TargetKind: ActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: prerequisites,
		State: ActionPending, Procedure: procedureFromEtcd(value.ProcedureKind, value.AgentProcedure, value.ControllerProcedure),
		UpdatedAt: operation.UpdatedAt,
	}
}

func procedureFromEtcd(
	kind hierarchydeletion.HierarchyDeletionProcedureKind,
	agent *hierarchydeletion.HierarchyDeletionAgentProcedure,
	controller *hierarchydeletion.HierarchyDeletionControllerProcedure,
) Procedure {
	procedure := Procedure{Kind: ProcedureKind(kind)}
	if agent != nil {
		procedure.AgentChild = &AgentChildProcedure{
			ChildOperationID: agent.ChildOperationID, TaskType: string(agent.TaskType),
			TypedProcedure: agent.TypedProcedure, InputDigest: agent.InputDigest,
			Timeout: time.Duration(agent.TimeoutSeconds) * time.Second,
		}
	}
	if controller != nil {
		procedure.ControllerFinalizer = &ControllerFinalizerProcedure{
			Finalizer: controller.Finalizer, FixedInputRevision: controller.FixedInputRevision,
			CompareTemplateDigest:       controller.CompareTemplateDigest,
			MutationTemplateDigest:      controller.MutationTemplateDigest,
			PostconditionTemplateDigest: controller.PostconditionTemplateDigest,
		}
	}
	return procedure
}

func procedureInputFromEtcd(value hierarchydeletionplanning.HierarchyDeletionProcedureInput) ProcedureInput {
	input := ProcedureInput{Kind: ProcedureKind(value.Kind)}
	if value.AgentChild != nil {
		input.AgentChild = &AgentChildInput{
			TaskType: string(value.AgentChild.TaskType), TypedProcedure: value.AgentChild.TypedProcedure,
			InputDigest: value.AgentChild.InputDigest,
			Timeout:     time.Duration(value.AgentChild.TimeoutSeconds) * time.Second,
		}
	}
	if value.ControllerFinalizer != nil {
		controller := value.ControllerFinalizer
		input.ControllerFinalizer = &ControllerFinalizerInput{
			Finalizer: controller.Finalizer, TargetKind: ActionTargetKind(controller.TargetKind),
			TargetID: controller.TargetID, FixedInputRevision: controller.FixedInputRevision,
			FixedInputDigest: controller.FixedInputDigest,
			BatchOrdinal:     int(controller.BatchOrdinal), BatchCount: int(controller.BatchCount),
		}
	}
	return input
}

func procedureInputToEtcd(value ProcedureInput) hierarchydeletionplanning.HierarchyDeletionProcedureInput {
	input := hierarchydeletionplanning.HierarchyDeletionProcedureInput{Kind: hierarchydeletion.HierarchyDeletionProcedureKind(value.Kind)}
	if value.AgentChild != nil {
		input.AgentChild = &hierarchydeletionplanning.HierarchyDeletionAgentInput{
			TaskType: taskjournal.TaskType(value.AgentChild.TaskType), TypedProcedure: value.AgentChild.TypedProcedure,
			InputDigest: value.AgentChild.InputDigest, TimeoutSeconds: int64(value.AgentChild.Timeout.Seconds()),
		}
	}
	if value.ControllerFinalizer != nil {
		controller := value.ControllerFinalizer
		input.ControllerFinalizer = &hierarchydeletionplanning.HierarchyDeletionControllerFinalizerInput{
			Finalizer: controller.Finalizer, TargetKind: hierarchydeletion.HierarchyDeletionActionTargetKind(controller.TargetKind),
			TargetID: controller.TargetID, FixedInputRevision: controller.FixedInputRevision,
			FixedInputDigest: controller.FixedInputDigest,
			BatchOrdinal:     int64(controller.BatchOrdinal), BatchCount: int64(controller.BatchCount),
		}
	}
	return input
}

func plannedActionToEtcd(value PlannedAction) etcdinfra.HierarchyDeletionPlannedAction {
	prerequisites := make([]int64, len(value.PrerequisiteOrdinals))
	for index, ordinal := range value.PrerequisiteOrdinals {
		prerequisites[index] = int64(ordinal)
	}
	return etcdinfra.HierarchyDeletionPlannedAction{
		ID: value.ID, NodeID: value.NodeID, Ordinal: int64(value.Ordinal), ParentOperationID: value.OperationID,
		ActionKind: hierarchydeletion.HierarchyDeletionActionKind(value.Kind),
		TargetKind: hierarchydeletion.HierarchyDeletionActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: prerequisites,
		ProcedureInput: procedureInputToEtcd(value.ProcedureInput),
	}
}

func idempotencyKeyHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

var _ Repository = (*EtcdRepository)(nil)
var _ AgentActionExecutor = (*EtcdRepository)(nil)
