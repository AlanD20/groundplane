package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredEntrySettlementRepository interface {
	PrepareBlueprintEntrySettlement(context.Context, etcd.BlueprintEntrySettlement) (workqueue.Change, error)
}

// PrepareEntrySettlement resolves one Entry from captured input. The returned
// value, receipt and work transition must commit together; preparation has no
// storage effects. The caller clears the returned mutation values after commit.
func (service *Service) PrepareEntrySettlement(
	ctx context.Context,
	work workqueue.View,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) (workqueue.Change, error) {
	parent := work.Task.Task
	if service == nil || ctx == nil || unit.Target.Kind != ids.KindEnvEntry ||
		snapshot.HeadTaskID != parent.ID || snapshot.EnvironmentID != parent.Scope.EnvironmentID ||
		work.ReadRevision != snapshot.ReadRevision || work.Item.Item.TargetID != unit.Target.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry settlement is not configured")
	}
	if unit.Removal {
		return service.prepareEntryOmission(ctx, work, unit)
	}
	settler, ok := service.repository.(authoredEntrySettlementRepository)
	if !ok {
		return workqueue.Change{}, errs.New(
			errs.KindInternal,
			"Blueprint Entry settlement repository is not configured",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return workqueue.Change{}, err
	}
	records, err := projectionrecord.AuthoredEntryRecords(input.desired, input.identities)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, record := range records {
		if record.Entry.ID != unit.Target.ID {
			continue
		}
		spec, found := input.desired.Input.Entries[record.BlueprintKey]
		if !found {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry authored source is missing")
		}
		desired, err := core.ProjectEntrySpec(record.BlueprintKey, spec, record.Entry.ID)
		if err != nil {
			return workqueue.Change{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		var factConditions []keyvalue.Condition
		generator := service.entryGeneration
		if desired.Source.Kind == core.SourceFact {
			if desired.Source.Fact == nil {
				return workqueue.Change{}, errs.New(
					errs.KindValidationFailed,
					"Blueprint Entry fact reference is missing",
				)
			}
			resolver, err := service.authoredEntryFactResolver(ctx, input, snapshot, desired.Source.Fact.Attach)
			if err != nil {
				return workqueue.Change{}, err
			}
			defer resolver.clear()
			factConditions = resolver.conditions
			generator = generator.WithFactResolver(resolver)
		}
		generation, err := generator.Generate(
			ctx, parent.Scope.ProjectID, parent.Scope.EnvironmentID,
			desired, record.CurrentValueGenerationID, service.now().UTC(),
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer entrygeneration.ClearEntryValueGeneration(&generation)
		change, err := settler.PrepareBlueprintEntrySettlement(ctx, etcd.BlueprintEntrySettlement{
			Work: work, Unit: unit, Entry: record, Generation: generation,
			At: service.now().UTC(),
		})
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, factConditions...)
		return change, nil
	}
	return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry is not in the authored revision")
}

type appliedAttachBindingReader interface {
	ReadAppliedAttachBinding(
		context.Context,
		string,
		string,
		int64,
	) (attachments.AppliedBinding, []keyvalue.Condition, error)
}

type pinnedEntryFactResolver struct {
	service       *Service
	environmentID string
	bindings      map[string]attachments.AppliedBinding
	conditions    []keyvalue.Condition
}

func (service *Service) authoredEntryFactResolver(
	ctx context.Context,
	input authoredParentInput,
	snapshot blueprintunits.Snapshot,
	attachName string,
) (*pinnedEntryFactResolver, error) {
	reader, ok := service.repository.(appliedAttachBindingReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Applied Attach fact reader is not configured")
	}
	resolver := &pinnedEntryFactResolver{
		service:       service,
		environmentID: input.desired.EnvironmentID,
		bindings:      make(map[string]attachments.AppliedBinding),
	}
	succeeded := false
	defer func() {
		if !succeeded {
			resolver.clear()
		}
	}()
	applied := make(map[string]bool)
	for _, receipt := range snapshot.Applied {
		if receipt.Record.Target.Kind == ids.KindAttach && receipt.Record.State == blueprintunits.Applied {
			applied[receipt.Record.Target.ID] = true
		}
	}
	for _, identity := range input.identities.Attaches {
		if identity.Name != attachName || !applied[identity.ID] {
			continue
		}
		binding, conditions, err := reader.ReadAppliedAttachBinding(
			ctx,
			resolver.environmentID,
			identity.ID,
			snapshot.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		if binding.Consumer.Name != identity.Name {
			binding.Clear()
			return nil, errs.New(errs.KindStateConflict, "Applied Attach name differs from the Blueprint")
		}
		resolver.bindings[identity.Name] = binding
		resolver.conditions = append(resolver.conditions, conditions...)
	}
	succeeded = true
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
	binding, found := resolver.bindings[reference.Attach]
	if !found {
		return errs.New(errs.KindStateConflict, "Blueprint Entry fact Attach is not applied")
	}
	return resolver.service.attachFacts.ResolveAppliedAttachFact(ctx, binding, reference, destinationSecret, consume)
}

func (resolver *pinnedEntryFactResolver) clear() {
	if resolver == nil {
		return
	}
	for key, binding := range resolver.bindings {
		binding.Clear()
		delete(resolver.bindings, key)
	}
}
