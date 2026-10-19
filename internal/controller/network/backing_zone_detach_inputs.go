package network

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareBackingZoneDetachInputs consumes snapshots only after validating the
// complete frozen set. Returned inputs own their encrypted source bytes. On any
// failure both transferred inputs and the remaining snapshots are cleared.
func prepareBackingZoneDetachInputs(ctx context.Context, task workexecution.Task, placement workexecution.Placement,
	revision int64, snapshots []attachremoval.DetachSnapshot,
) (inputs []attachremoval.DetachInput, plans []CascadeDetachPlan, resultErr error) {
	defer func() {
		for index := range snapshots {
			snapshots[index].Clear()
		}
		if resultErr != nil {
			for index := range inputs {
				inputs[index].Clear()
			}
			inputs, plans = nil, nil
		}
	}()
	if task.Validate() != nil || task.Action != workexecution.ActionRemove || task.PublicationComplete ||
		task.ExpectedItems != 1 || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		task.Scope.TenantID != "" || task.Scope.ProjectID == "" || task.Scope.EnvironmentID == "" ||
		ids.Validate(ids.KindNetwork, task.TargetID) != nil || placement.Validate() != nil ||
		placement.Executor != workexecution.Agent || revision <= 0 || len(snapshots) >= int(workexecution.MaximumTaskItems) {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"Backing Zone removal requires its unpublished Task and frozen consumer snapshots",
		)
	}
	captured := make([]keyvalue.Versioned[attachments.Record], 0, len(snapshots))
	byID := make(map[string]int, len(snapshots))
	for index, snapshot := range snapshots {
		source := snapshot.Source
		if snapshot.Validate() != nil || source.SnapshotRevision != revision ||
			source.Attach.BackingNetworkID != task.TargetID || source.Attach.BackingProjectID != task.Scope.ProjectID ||
			source.Attach.BackingEnvironmentID != task.Scope.EnvironmentID {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"Backing Zone consumer snapshot differs from the approved removal",
			)
		}
		if _, exists := byID[source.Attach.ID]; exists {
			return nil, nil, errs.New(errs.KindValidationFailed, "Backing Zone consumer snapshot is duplicated")
		}
		byID[source.Attach.ID] = index
		captured = append(captured, keyvalue.Versioned[attachments.Record]{Record: source.Attach,
			Revision: source.AttachRevision, ReadRevision: revision})
	}
	ordered, err := orderBackingZoneCascadeAttaches(captured)
	if err != nil {
		return nil, nil, err
	}
	removedByService := make(map[string][]string)
	for ordinal, source := range ordered {
		snapshot := &snapshots[byID[source.Record.ID]]
		step := &attachremoval.CascadeStep{ZoneID: task.TargetID, Ordinal: uint32(ordinal), Count: uint32(len(ordered)),
			RemovedBefore: slices.Clone(removedByService[source.Record.ServiceID])}
		slices.Sort(step.RemovedBefore)
		workID := ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "backing-zone-detach/"+source.Record.ID)
		consumer, err := attachplan.ConsumerTransition(ctx, *snapshot, step,
			ids.DeriveAt(ids.KindConfig, task.CreatedAt, workID, "consumer-transition"))
		if err != nil {
			return inputs, plans, err
		}
		input, err := attachremoval.NewDetachInput(ctx, task.ID, workID, snapshot, step, consumer)
		if consumer != nil {
			clear(consumer.Baseline.CanonicalYaml)
			clear(consumer.Candidate.CanonicalYaml)
		}
		if err != nil {
			return inputs, plans, err
		}
		inputs = append(inputs, input)
		reference, err := input.Reference(ctx, ids.DeriveAt(ids.KindPlan, task.CreatedAt, workID, "captured-input"))
		if err != nil {
			return inputs, plans, err
		}
		itemPlacement := placement
		if !input.HasHostEffects() {
			itemPlacement.Executor = workexecution.Controller
		}
		plans = append(
			plans,
			CascadeDetachPlan{AttachID: source.Record.ID, Placement: itemPlacement, Input: &reference},
		)
		if source.Record.Status != "detached" {
			removedByService[source.Record.ServiceID] = append(
				removedByService[source.Record.ServiceID],
				source.Record.ID,
			)
		}
	}
	return inputs, plans, nil
}
