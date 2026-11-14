package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

func (service *Service) prepareAuthoredDetachInputs(ctx context.Context, parent workexecution.Task,
	plan blueprintunits.DesiredPlan, owned environmentprojection.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
) (inputs []attachremoval.BlueprintInput, resultErr error) {
	defer func() {
		if resultErr != nil {
			clearAuthoredDetachInputs(inputs)
			inputs = nil
		}
	}()
	for _, unit := range plan.Units {
		if unit.Target.Kind != ids.KindAttach || !unit.Removal {
			continue
		}
		input, err := service.repository.CaptureBlueprintDetach(ctx, parent, unit, owned, snapshot)
		if err != nil {
			return inputs, err
		}
		inputs = append(inputs, input)
		captured := &inputs[len(inputs)-1]
		if captured.Snapshot != nil {
			source := captured.Snapshot.Source
			if attachremoval.NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" {
				captured.Hook, err = service.attachFacts.SealBackingHookInputsAtRevision(ctx, captured.OperationID,
					source.Attach.BackingProjectID, *source.Backing.Desired.Hooks, source.SnapshotRevision)
				if err != nil {
					return inputs, err
				}
				if captured.Hook != nil {
					captured.Sources = append([]tasksecretpinrecord.Record(nil), captured.Hook.SecretSources...)
				}
			}
		}
		if err := captured.Validate(ctx, false); err != nil {
			return inputs, err
		}
	}

	for index := range inputs {
		captured := &inputs[index]
		pending := make([]blueprintunits.ResourceKey, 0)
		for _, unit := range plan.Units {
			if unit.Target.Kind == ids.KindAttach && unit.Target.ID == captured.AttachID {
				pending = append(pending, unit.After...)
			}
		}
		visited := make(map[blueprintunits.ResourceKey]bool)
		for len(pending) != 0 {
			target := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if visited[target] {
				continue
			}
			visited[target] = true
			for _, unit := range plan.Units {
				if unit.Target == target {
					pending = append(pending, unit.After...)
					break
				}
			}
			if target.Kind != ids.KindAttach || target.ID == captured.AttachID {
				continue
			}
			for _, previous := range inputs {
				if previous.AttachID == target.ID && previous.ConsumerServiceID == captured.ConsumerServiceID {
					captured.RemovedBefore = append(captured.RemovedBefore, previous.AttachID)
				}
			}
		}
		slices.Sort(captured.RemovedBefore)
		captured.RemovedBefore = slices.Compact(captured.RemovedBefore)
		if err := captured.Validate(ctx, false); err != nil {
			return inputs, err
		}
	}
	return inputs, nil
}

func clearAuthoredDetachInputs(inputs []attachremoval.BlueprintInput) {
	for index := range inputs {
		inputs[index].Clear()
	}
}
