package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredAttachGenerationReader interface {
	GetBlueprintAttachInputGeneration(
		context.Context,
		string,
		string,
	) (keyvalue.Versioned[blueprintattachinputs.Generation], bool, error)
	GetBlueprintAttachOutputGeneration(
		context.Context,
		string,
		string,
		int64,
	) (keyvalue.Versioned[blueprintattachoutputs.Generation], bool, error)
}

func (service *Service) loadAuthoredAttachGenerationAuthority(
	ctx context.Context,
	input authoredParentInput,
	snapshot blueprintunits.Snapshot,
) (map[string]blueprintattachinputs.Generation, error) {
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach input authority is not configured")
	}
	result := make(map[string]blueprintattachinputs.Generation)
	applied := make(map[string]blueprintunits.AppliedRecord)
	for _, candidate := range snapshot.Applied {
		if candidate.Record.Target.Kind == ids.KindAttach {
			applied[candidate.Record.Target.ID] = candidate.Record
		}
	}
	failed := true
	defer func() {
		if failed {
			clearAuthoredAttachGenerationAuthority(result)
		}
	}()
	for _, identity := range input.identities.Attaches {
		generation, current, err := reader.GetBlueprintAttachInputGeneration(
			ctx, input.desired.RevisionID, identity.ID,
		)
		if err != nil {
			return nil, err
		}
		appliedAttach, hasApplied := applied[identity.ID]
		if !current {
			if !hasApplied || appliedAttach.State != blueprintunits.Applied ||
				ids.Validate(ids.KindTask, appliedAttach.ParentTaskID) != nil ||
				ids.Validate(ids.KindTask, appliedAttach.SourceTaskID) != nil {
				continue
			}
			generation, current, err = reader.GetBlueprintAttachInputGeneration(
				ctx, appliedAttach.ParentTaskID, identity.ID,
			)
			if err != nil {
				return nil, err
			}
			if !current {
				return nil, errs.New(errs.KindStateConflict, "Applied Blueprint Attach input generation is missing")
			}
		}
		parentOwned := generation.Record.OwnerKind == blueprintattachinputs.OwnerParent &&
			generation.Record.OwnerTaskID == input.desired.RevisionID && generation.Record.Transfer == nil
		childOwned := generation.Record.OwnerKind == blueprintattachinputs.OwnerChild &&
			generation.Record.Transfer != nil &&
			generation.Record.Transfer.ParentTaskID == generation.Record.ParentTaskID &&
			generation.Record.Transfer.ChildTaskID == generation.Record.OwnerTaskID
		if generation.Record.EnvironmentID != input.desired.EnvironmentID ||
			generation.Record.AttachID != identity.ID ||
			(generation.Record.ParentTaskID != input.desired.RevisionID && !childOwned) ||
			(!parentOwned && !childOwned) {
			blueprintattachinputs.Clear(&generation.Record)
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach input generation ownership changed")
		}
		if generation.Record.ParentTaskID != input.desired.RevisionID &&
			(!hasApplied || appliedAttach.State != blueprintunits.Applied ||
				appliedAttach.ParentTaskID != generation.Record.ParentTaskID ||
				appliedAttach.SourceTaskID != generation.Record.OwnerTaskID) {
			blueprintattachinputs.Clear(&generation.Record)
			return nil, errs.New(errs.KindStateConflict, "Retained Blueprint Attach input is not applied")
		}
		if childOwned && hasApplied && appliedAttach.State == blueprintunits.Applied &&
			appliedAttach.SourceTaskID == generation.Record.OwnerTaskID {
			output, found, err := reader.GetBlueprintAttachOutputGeneration(
				ctx, identity.ID, appliedAttach.SourceTaskID, snapshot.ReadRevision,
			)
			if err != nil {
				blueprintattachinputs.Clear(&generation.Record)
				return nil, err
			}
			if !found || output.Record.EnvironmentID != input.desired.EnvironmentID ||
				output.Record.ParentTaskID != generation.Record.ParentTaskID ||
				output.Record.OperationID != generation.Record.OperationID ||
				output.Record.PlanID != appliedAttach.SourcePlanID ||
				output.Record.AssignmentID != appliedAttach.SourceAssignment ||
				output.Record.ExecutionEpoch != appliedAttach.ExecutionEpoch {
				blueprintattachoutputs.Clear(&output.Record)
				blueprintattachinputs.Clear(&generation.Record)
				return nil, errs.New(errs.KindStateConflict, "Applied Blueprint Attach output generation changed")
			}
			blueprintattachoutputs.Clear(&output.Record)
		}
		result[identity.ID] = generation.Record
	}
	failed = false
	return result, nil
}

func clearAuthoredAttachGenerationAuthority(generations map[string]blueprintattachinputs.Generation) {
	for id, generation := range generations {
		blueprintattachinputs.Clear(&generation)
		generations[id] = generation
	}
}
