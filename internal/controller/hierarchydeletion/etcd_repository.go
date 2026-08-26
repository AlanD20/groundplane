package hierarchydeletion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EtcdRepository struct {
	journal     *etcdinfra.HierarchyDeletionRepository
	idempotency *etcdinfra.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
	clock       Clock
}

func NewEtcdRepository(
	journal *etcdinfra.HierarchyDeletionRepository,
	idempotency *etcdinfra.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
	clock Clock,
) (*EtcdRepository, error) {
	if journal == nil || idempotency == nil || coordinator == nil || clock == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion etcd adapter dependencies are required")
	}
	return &EtcdRepository{
		journal: journal, idempotency: idempotency, coordinator: coordinator, clock: clock,
	}, nil
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
	resolution, found, err := repository.coordinator.ResolveExisting(
		ctx, repository.idempotency, marker.Locator, protected,
	)
	if err != nil {
		return BeginResult{}, err
	}
	if found {
		return repository.replayedBegin(ctx, resolution)
	}
	created, err := repository.journal.Begin(ctx, etcdinfra.HierarchyDeletionBegin{
		OperationID: begin.OperationID, TaskOperationID: begin.TaskOperationIDCandidate,
		OperationKind: etcdinfra.HierarchyDeletionOperationKind(begin.OperationKind),
		TargetKind:    etcdinfra.HierarchyDeletionTargetKind(begin.TargetKind), TargetID: begin.TargetID,
		TaskID: begin.TaskIDCandidate, IdempotencyHash: idempotencyKeyHash(begin.IdempotencyKey),
		Marker: marker, CreatedAt: begin.CreatedAt, DeadlineAt: begin.DeadlineAt,
	})
	if err != nil {
		kind, isKind := errs.KindOf(err)
		if !errors.Is(err, context.DeadlineExceeded) && (!isKind || kind != errs.KindStorageUnavailable) {
			return BeginResult{}, err
		}
		resolution, resolveErr := repository.coordinator.ResolveUnknown(
			ctx, repository.idempotency, marker.Locator, protected, err,
		)
		if resolveErr != nil {
			return BeginResult{}, resolveErr
		}
		return repository.replayedBegin(ctx, resolution)
	}
	resolution, err = repository.coordinator.ResolveKnown(ctx, protected, created.Idempotency)
	if err != nil {
		return BeginResult{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return repository.replayedBegin(ctx, resolution)
	}
	return BeginResult{Operation: operationFromEtcd(created.Operation), Existing: created.Existing}, nil
}

func (repository *EtcdRepository) protectBegin(
	ctx context.Context,
	begin BeginDeletion,
) (idempotentintent.ProtectedEvidence, etcdinfra.IdempotencyMarker, error) {
	intent := idempotentintent.CanonicalIntentV1{
		Method: begin.IdempotencyIntent.Method, Route: begin.IdempotencyIntent.RouteTemplate,
		Scope: idempotentintent.Scope{
			Kind: idempotencyScopeKind(begin.IdempotencyIntent.ScopeKind),
			ID:   begin.IdempotencyIntent.ScopeID,
		},
		Path:  make([]idempotentintent.PathBinding, len(begin.IdempotencyIntent.PathBindings)),
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	}
	for index, binding := range begin.IdempotencyIntent.PathBindings {
		intent.Path[index] = idempotentintent.PathBinding{Name: binding.Name, Value: binding.Value}
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, intent)
	if err != nil {
		return idempotentintent.ProtectedEvidence{}, etcdinfra.IdempotencyMarker{}, err
	}
	protected, err := repository.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotentintent.ProtectedEvidence{}, etcdinfra.IdempotencyMarker{}, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		protected.Destroy()
		return idempotentintent.ProtectedEvidence{}, etcdinfra.IdempotencyMarker{}, err
	}
	body, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: begin.TaskIDCandidate})
	if err != nil {
		protected.Destroy()
		clear(durable.Ciphertext)
		return idempotentintent.ProtectedEvidence{}, etcdinfra.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	marker := etcdinfra.IdempotencyMarker{
		Kind: etcdinfra.IdempotencyMarkerTask, State: etcdinfra.IdempotencyMarkerPending,
		Locator: etcdinfra.IdempotencyLocator{
			ScopeKind: etcdinfra.IdempotencyScopeKind(begin.IdempotencyIntent.ScopeKind),
			ScopeID:   begin.IdempotencyIntent.ScopeID, Method: begin.IdempotencyIntent.Method,
			Route: begin.IdempotencyIntent.RouteTemplate, Key: begin.IdempotencyKey,
		},
		Intent: durable,
		Response: etcdinfra.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
		TaskID: begin.TaskIDCandidate, CreatedAt: begin.CreatedAt, UpdatedAt: begin.CreatedAt,
	}
	return protected, marker, nil
}

func (repository *EtcdRepository) replayedBegin(
	ctx context.Context,
	resolution idempotentintent.Resolution,
) (BeginResult, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay || resolution.Response.Status != http.StatusAccepted {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay response is invalid")
	}
	var response struct {
		TaskID string `json:"task_id"`
	}
	if json.Unmarshal(resolution.Response.Body, &response) != nil || response.TaskID == "" {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay Task is invalid")
	}
	operation, err := repository.journal.OperationByTask(ctx, response.TaskID)
	if err != nil {
		return BeginResult{}, err
	}
	return BeginResult{Operation: operationFromEtcd(operation), Existing: true}, nil
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
	encoded := make([]etcdinfra.HierarchyDeletionAction, len(actions))
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
			Terminal:     etcdinfra.HierarchyDeletionAgentTerminal(proof.Terminal),
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
		ctx, etcdinfra.HierarchyDeletionTargetKind(target), id, revision,
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
		Tombstone: etcdinfra.HierarchyDeletionTombstone{
			OperationID: value.ID, TaskOperationID: value.TaskOperationID,
			OperationKind: etcdinfra.HierarchyDeletionOperationKind(value.Kind),
			TargetKind:    etcdinfra.HierarchyDeletionTargetKind(value.TargetKind), TargetID: value.TargetID,
			CurrentTaskID: value.TaskID, SnapshotRevision: value.SnapshotRevision,
			DeletionEpoch: value.CoordinationEpoch, Phase: etcdinfra.HierarchyDeletionPhase(value.Phase),
			AttemptDeadline: value.DeadlineAt, CreatedAt: value.CreatedAt,
			Checkpoint: etcdinfra.HierarchyDeletionCheckpoint{
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

func actionToEtcd(value Action) etcdinfra.HierarchyDeletionAction {
	converted := etcdinfra.HierarchyDeletionAction{
		Schema: 1, ParentOperationID: value.OperationID, NodeID: value.NodeID,
		Ordinal: int64(value.Ordinal), ActionKind: etcdinfra.HierarchyDeletionActionKind(value.Kind),
		TargetKind: etcdinfra.HierarchyDeletionActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: make([]int64, len(value.PrerequisiteOrdinals)),
		ProcedureKind: etcdinfra.HierarchyDeletionProcedureKind(value.Procedure.Kind),
	}
	for index, ordinal := range value.PrerequisiteOrdinals {
		converted.PrerequisiteOrdinals[index] = int64(ordinal)
	}
	if value.Procedure.AgentChild != nil {
		converted.AgentProcedure = &etcdinfra.HierarchyDeletionAgentProcedure{
			ChildOperationID: value.Procedure.AgentChild.ChildOperationID,
			TaskType:         etcdinfra.TaskType(value.Procedure.AgentChild.TaskType),
			TypedProcedure:   value.Procedure.AgentChild.TypedProcedure,
			InputDigest:      value.Procedure.AgentChild.InputDigest,
			TimeoutSeconds:   int64(value.Procedure.AgentChild.Timeout.Seconds()),
		}
	}
	if value.Procedure.ControllerFinalizer != nil {
		procedure := value.Procedure.ControllerFinalizer
		converted.ControllerProcedure = &etcdinfra.HierarchyDeletionControllerProcedure{
			Finalizer: procedure.Finalizer, FixedInputRevision: procedure.FixedInputRevision,
			CompareTemplateDigest:       procedure.CompareTemplateDigest,
			MutationTemplateDigest:      procedure.MutationTemplateDigest,
			PostconditionTemplateDigest: procedure.PostconditionTemplateDigest,
		}
	}
	return converted
}

func actionFromEtcd(
	value etcdinfra.HierarchyDeletionAction,
	operation etcdinfra.HierarchyDeletionOperation,
) Action {
	prerequisites := make([]int, len(value.PrerequisiteOrdinals))
	for index, ordinal := range value.PrerequisiteOrdinals {
		prerequisites[index] = int(ordinal)
	}
	domainOperation := operationFromEtcd(operation)
	return Action{
		ID:     stableActionID(domainOperation.ID, int(value.Ordinal), ActionKind(value.ActionKind), ActionTargetKind(value.TargetKind), value.TargetID),
		NodeID: value.NodeID, Ordinal: int(value.Ordinal), OperationID: value.ParentOperationID,
		Kind: ActionKind(value.ActionKind), TargetKind: ActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: prerequisites,
		State: ActionPending, Procedure: procedureFromEtcd(value.ProcedureKind, value.AgentProcedure, value.ControllerProcedure),
		UpdatedAt: operation.UpdatedAt,
	}
}

func procedureFromEtcd(
	kind etcdinfra.HierarchyDeletionProcedureKind,
	agent *etcdinfra.HierarchyDeletionAgentProcedure,
	controller *etcdinfra.HierarchyDeletionControllerProcedure,
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

func procedureInputFromEtcd(value etcdinfra.HierarchyDeletionProcedureInput) ProcedureInput {
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

func procedureInputToEtcd(value ProcedureInput) etcdinfra.HierarchyDeletionProcedureInput {
	input := etcdinfra.HierarchyDeletionProcedureInput{Kind: etcdinfra.HierarchyDeletionProcedureKind(value.Kind)}
	if value.AgentChild != nil {
		input.AgentChild = &etcdinfra.HierarchyDeletionAgentInput{
			TaskType: etcdinfra.TaskType(value.AgentChild.TaskType), TypedProcedure: value.AgentChild.TypedProcedure,
			InputDigest: value.AgentChild.InputDigest, TimeoutSeconds: int64(value.AgentChild.Timeout.Seconds()),
		}
	}
	if value.ControllerFinalizer != nil {
		controller := value.ControllerFinalizer
		input.ControllerFinalizer = &etcdinfra.HierarchyDeletionControllerFinalizerInput{
			Finalizer: controller.Finalizer, TargetKind: etcdinfra.HierarchyDeletionActionTargetKind(controller.TargetKind),
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
		ActionKind: etcdinfra.HierarchyDeletionActionKind(value.Kind),
		TargetKind: etcdinfra.HierarchyDeletionActionTargetKind(value.TargetKind), TargetID: value.TargetID,
		TargetRevision: value.TargetRevision, PrerequisiteOrdinals: prerequisites,
		ProcedureInput: procedureInputToEtcd(value.ProcedureInput),
	}
}

func idempotencyKeyHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func idempotencyScopeKind(target TargetKind) idempotentintent.ScopeKind {
	switch target {
	case TargetTenant:
		return idempotentintent.ScopeTenant
	case TargetProject:
		return idempotentintent.ScopeProject
	case TargetEnvironment:
		return idempotentintent.ScopeEnvironment
	default:
		return ""
	}
}

var _ Repository = (*EtcdRepository)(nil)
var _ AgentActionExecutor = (*EtcdRepository)(nil)
