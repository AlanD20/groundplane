package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyDeletionPlannedAction struct {
	ID                   string
	NodeID               string
	Ordinal              int64
	ParentOperationID    string
	ActionKind           HierarchyDeletionActionKind
	TargetKind           HierarchyDeletionActionTargetKind
	TargetID             string
	TargetRevision       int64
	PrerequisiteOrdinals []int64
	ProcedureInput       HierarchyDeletionProcedureInput
}

func (repository *HierarchyDeletionRepository) BindActions(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	planned []HierarchyDeletionPlannedAction,
) ([]HierarchyDeletionAction, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return nil, err
	}
	if current.Tombstone.Phase != HierarchyDeletionPlanning || current.Tombstone.PlanCount != nil ||
		current.Tombstone.PlanDigest != nil || current.Tombstone.OperationID != operation.Tombstone.OperationID {
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion plan is not bindable")
	}
	bound := make([]HierarchyDeletionAction, len(planned))
	for index := range planned {
		bound[index], err = bindHierarchyDeletionAction(current, planned[index])
		if err != nil {
			return nil, err
		}
	}
	return bound, nil
}

func bindHierarchyDeletionAction(
	operation HierarchyDeletionOperation,
	planned HierarchyDeletionPlannedAction,
) (HierarchyDeletionAction, error) {
	if planned.ID == "" || planned.NodeID == "" || planned.ParentOperationID != operation.Tombstone.OperationID ||
		planned.Ordinal < 0 || planned.TargetID == "" || planned.TargetRevision <= 0 ||
		!slices.IsSorted(planned.PrerequisiteOrdinals) {
		return HierarchyDeletionAction{}, errs.New(errs.KindValidationFailed, "hierarchy deletion planned action is invalid")
	}
	action := HierarchyDeletionAction{
		Schema: 1, ParentOperationID: planned.ParentOperationID, NodeID: planned.NodeID,
		Ordinal: planned.Ordinal, ActionKind: planned.ActionKind, TargetKind: planned.TargetKind,
		TargetID: planned.TargetID, TargetRevision: planned.TargetRevision,
		PrerequisiteOrdinals: append([]int64(nil), planned.PrerequisiteOrdinals...),
		ProcedureKind:        planned.ProcedureInput.Kind,
	}
	switch planned.ProcedureInput.Kind {
	case HierarchyDeletionProcedureAgent:
		input := planned.ProcedureInput.AgentChild
		if input == nil || planned.ProcedureInput.ControllerFinalizer != nil {
			return HierarchyDeletionAction{}, errs.New(errs.KindValidationFailed, "hierarchy deletion Agent binding input is invalid")
		}
		action.AgentProcedure = &HierarchyDeletionAgentProcedure{
			ChildOperationID: hierarchyDeletionStableOperationID(planned.ParentOperationID, planned.NodeID),
			TaskType:         input.TaskType, TypedProcedure: input.TypedProcedure,
			InputDigest: input.InputDigest, TimeoutSeconds: input.TimeoutSeconds,
		}
	case HierarchyDeletionProcedureController:
		input := planned.ProcedureInput.ControllerFinalizer
		if input == nil || planned.ProcedureInput.AgentChild != nil || input.TargetKind != planned.TargetKind ||
			input.TargetID != planned.TargetID || input.FixedInputRevision != planned.TargetRevision ||
			!validHierarchyDeletionDigest(input.FixedInputDigest) || input.BatchOrdinal < 0 ||
			input.BatchCount <= 0 || input.BatchOrdinal >= input.BatchCount {
			return HierarchyDeletionAction{}, errs.New(errs.KindValidationFailed, "hierarchy deletion Controller binding input is invalid")
		}
		procedure, err := bindHierarchyDeletionControllerProcedure(planned, *input)
		if err != nil {
			return HierarchyDeletionAction{}, err
		}
		action.ControllerProcedure = &procedure
	default:
		return HierarchyDeletionAction{}, errs.New(errs.KindValidationFailed, "hierarchy deletion procedure input kind is invalid")
	}
	if err := validateHierarchyDeletionAction(action); err != nil {
		return HierarchyDeletionAction{}, err
	}
	return action, nil
}

type hierarchyDeletionTemplate struct {
	Version            int                               `json:"version"`
	Domain             string                            `json:"domain"`
	ParentOperationID  string                            `json:"parent_operation_id"`
	NodeID             string                            `json:"node_id"`
	Ordinal            int64                             `json:"ordinal"`
	ActionKind         HierarchyDeletionActionKind       `json:"action_kind"`
	TargetKind         HierarchyDeletionActionTargetKind `json:"target_kind"`
	TargetID           string                            `json:"target_id"`
	TargetRevision     int64                             `json:"target_revision"`
	Finalizer          string                            `json:"finalizer"`
	FixedInputDigest   string                            `json:"fixed_input_digest"`
	BatchOrdinal       int64                             `json:"batch_ordinal"`
	BatchCount         int64                             `json:"batch_count"`
	SymbolicSlots      []string                          `json:"symbolic_slots"`
	FixedOperandSchema []string                          `json:"fixed_operand_schema"`
}

func bindHierarchyDeletionControllerProcedure(
	planned HierarchyDeletionPlannedAction,
	input HierarchyDeletionControllerFinalizerInput,
) (HierarchyDeletionControllerProcedure, error) {
	if hierarchyDeletionControllerFinalizer(planned.ActionKind) != input.Finalizer {
		return HierarchyDeletionControllerProcedure{}, errs.Newf(
			errs.KindValidationFailed, "hierarchy deletion finalizer %q is unsupported for %q",
			input.Finalizer, planned.ActionKind,
		)
	}
	compare, err := hierarchyDeletionTemplateDigest(planned, input, "compare", []string{
		"target.mod_revision:int64@0", "tombstone.mod_revision:int64@1", "fence.mod_revision:int64@2",
		"completion.create_revision:int64@3", "predecessor.checkpoint_digest:sha256@4", "fence.generation:int64@5",
	})
	if err != nil {
		return HierarchyDeletionControllerProcedure{}, err
	}
	mutation, err := hierarchyDeletionTemplateDigest(planned, input, "mutation", []string{
		"completion.digest:sha256@0", "checkpoint.digest:sha256@1", "fence.generation:int64@2", "terminal.time:rfc3339nano@3",
	})
	if err != nil {
		return HierarchyDeletionControllerProcedure{}, err
	}
	postcondition, err := hierarchyDeletionTemplateDigest(planned, input, "postcondition", []string{
		"target.absent:bool@0", "completion.digest:sha256@1", "checkpoint.next_ordinal:int64@2", "fence.generation:int64@3",
	})
	if err != nil {
		return HierarchyDeletionControllerProcedure{}, err
	}
	return HierarchyDeletionControllerProcedure{
		Finalizer: input.Finalizer, FixedInputRevision: input.FixedInputRevision,
		CompareTemplateDigest: compare, MutationTemplateDigest: mutation,
		PostconditionTemplateDigest: postcondition,
	}, nil
}

func hierarchyDeletionTemplateDigest(
	planned HierarchyDeletionPlannedAction,
	input HierarchyDeletionControllerFinalizerInput,
	domain string,
	slots []string,
) (string, error) {
	template := hierarchyDeletionTemplate{
		Version: 1, Domain: domain, ParentOperationID: planned.ParentOperationID,
		NodeID: planned.NodeID, Ordinal: planned.Ordinal, ActionKind: planned.ActionKind,
		TargetKind: planned.TargetKind, TargetID: planned.TargetID, TargetRevision: planned.TargetRevision,
		Finalizer: input.Finalizer, FixedInputDigest: input.FixedInputDigest,
		BatchOrdinal: input.BatchOrdinal, BatchCount: input.BatchCount,
		SymbolicSlots: append([]string(nil), slots...),
		FixedOperandSchema: []string{
			"primary-and-index-keys:derived-from-fixed-input-v1",
			"resource-compares:exact-fixed-revision-v1",
			"resource-mutations:closed-finalizer-v1",
			"completion-envelope:not-recursively-expanded-v1",
		},
	}
	value, err := json.Marshal(template)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}
