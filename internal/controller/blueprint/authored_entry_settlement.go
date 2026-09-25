package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredEntrySettlementRepository interface {
	SettleBlueprintEntry(context.Context, etcd.BlueprintEntrySettlement) error
}

// SettleEntry resolves one Entry from the parent's sealed input. The repository
// owns the final head/epoch/parent compares and the atomic value plus receipt.
func (service *Service) SettleEntry(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) error {
	if service == nil || ctx == nil || unit.Target.Kind != ids.KindEnvEntry || unit.Removal ||
		snapshot.HeadTaskID != parent.ID || snapshot.EnvironmentID != parent.Owner.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Entry settlement is not configured")
	}
	settler, ok := service.repository.(authoredEntrySettlementRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Entry settlement repository is not configured")
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return err
	}
	records, err := authoredEntryRecords(input.desired, input.identities)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Entry.ID != unit.Target.ID {
			continue
		}
		spec, found := input.desired.Input.Entries[record.BlueprintKey]
		if !found {
			return errs.New(errs.KindStateConflict, "Blueprint Entry authored source is missing")
		}
		desired, err := core.ProjectEntrySpec(record.BlueprintKey, spec, record.Entry.ID)
		if err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
		generator := service.entryGeneration
		if desired.Source.Kind == core.SourceFact {
			resolver, err := service.authoredEntryFactResolver(ctx, input, snapshot)
			if err != nil {
				return err
			}
			defer resolver.clear()
			generator = generator.WithFactResolver(resolver)
		}
		generation, err := generator.Generate(
			ctx, parent.Owner.ProjectID, parent.Owner.EnvironmentID,
			desired, record.CurrentValueGenerationID, service.now().UTC(),
		)
		if err != nil {
			return err
		}
		defer entrygeneration.ClearEntryValueGeneration(&generation)
		return settler.SettleBlueprintEntry(ctx, etcd.BlueprintEntrySettlement{
			Parent: parent, Unit: unit, Entry: record, Generation: generation,
			OperationID: ids.New(ids.KindOperation),
		})
	}
	return errs.New(errs.KindStateConflict, "Blueprint Entry is not in the authored revision")
}

type pinnedEntryFactResolver struct {
	service       *Service
	environmentID string
	inputs        map[string]blueprintattachinputs.Generation
	outputs       map[string]blueprintattachoutputs.Generation
}

func (service *Service) authoredEntryFactResolver(
	ctx context.Context,
	input authoredParentInput,
	snapshot blueprintunits.Snapshot,
) (*pinnedEntryFactResolver, error) {
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach fact reader is not configured")
	}
	inputs, err := service.loadAuthoredAttachGenerationAuthority(ctx, input, snapshot)
	if err != nil {
		return nil, err
	}
	resolver := &pinnedEntryFactResolver{
		service: service, environmentID: input.desired.EnvironmentID, inputs: inputs,
		outputs: make(map[string]blueprintattachoutputs.Generation),
	}
	failed := true
	defer func() {
		if failed {
			resolver.clear()
		}
	}()
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind != ids.KindAttach || applied.Record.State != blueprintunits.Applied {
			continue
		}
		generation, found := inputs[applied.Record.Target.ID]
		if !found || generation.OwnerKind != blueprintattachinputs.OwnerChild ||
			generation.OwnerTaskID != applied.Record.SourceTaskID {
			continue
		}
		output, found, err := reader.GetBlueprintAttachOutputGeneration(
			ctx, generation.AttachID, applied.Record.SourceTaskID, snapshot.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		if !found || output.Record.EnvironmentID != input.desired.EnvironmentID ||
			output.Record.ParentTaskID != generation.ParentTaskID ||
			output.Record.OperationID != generation.OperationID ||
			output.Record.PlanID != applied.Record.SourcePlanID ||
			output.Record.AssignmentID != applied.Record.SourceAssignment ||
			output.Record.ExecutionEpoch != applied.Record.ExecutionEpoch {
			blueprintattachoutputs.Clear(&output.Record)
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach output fact changed")
		}
		resolver.outputs[generation.AttachID] = output.Record
	}
	failed = false
	return resolver, nil
}

func (resolver *pinnedEntryFactResolver) ResolveFact(
	ctx context.Context,
	environmentID string,
	reference core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	if resolver == nil || environmentID != resolver.environmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Entry fact Environment changed")
	}
	for attachID, generation := range resolver.inputs {
		if generation.AttachName != reference.Attach {
			continue
		}
		output, found := resolver.outputs[attachID]
		if !found {
			return errs.New(errs.KindStateConflict, "Blueprint Attach output fact is not applied")
		}
		return resolver.service.attachFacts.ResolveBlueprintOutputFact(
			ctx, generation, output, reference, destinationSecret, consume,
		)
	}
	return errs.New(errs.KindStateConflict, "Blueprint Entry fact Attach is not pinned")
}

func (resolver *pinnedEntryFactResolver) clear() {
	if resolver == nil {
		return
	}
	clearAuthoredAttachGenerationAuthority(resolver.inputs)
	for key, output := range resolver.outputs {
		blueprintattachoutputs.Clear(&output)
		resolver.outputs[key] = output
	}
}
