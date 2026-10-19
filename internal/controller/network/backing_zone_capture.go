package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingZoneHookInputs interface {
	SealBackingHookInputsAtRevision(
		context.Context,
		string,
		string,
		backinghook.Configuration,
		int64,
	) (*taskconfiguration.BackingHookEncryptedInputs, error)
}

type capturedBackingZoneWork struct {
	Graph  BackingZoneWorkGraph
	Inputs []attachremoval.DetachInput
	Hooks  []attachremoval.WorkCapture
}

func (captured *capturedBackingZoneWork) Clear() {
	for index := range captured.Inputs {
		captured.Inputs[index].Clear()
	}
	for index := range captured.Hooks {
		captured.Hooks[index].Clear()
	}
}

// captureBackingZoneWork resolves every hook against the same approved snapshot
// before handing the encrypted inputs to storage. Publication resumes from those
// bytes; it never asks the fact service to reopen an old Secret revision later.
func captureBackingZoneWork(ctx context.Context, facts backingZoneHookInputs, task workexecution.Task,
	bridge workexecution.Item, revision int64, impact string, snapshots []attachremoval.DetachSnapshot,
) (captured capturedBackingZoneWork, resultErr error) {
	defer func() {
		if resultErr != nil {
			captured.Clear()
		}
	}()
	if facts == nil {
		for index := range snapshots {
			snapshots[index].Clear()
		}
		return captured, errs.New(errs.KindInternal, "Backing Zone removal requires encrypted hook capture")
	}
	sources := make([]keyvalue.Versioned[attachments.Record], 0, len(snapshots))
	for _, snapshot := range snapshots {
		sources = append(sources, keyvalue.Versioned[attachments.Record]{Record: snapshot.Source.Attach,
			Revision: snapshot.Source.AttachRevision, ReadRevision: snapshot.Source.SnapshotRevision})
	}
	inputs, plans, err := prepareBackingZoneDetachInputs(ctx, task, bridge.Placement, revision, snapshots)
	if err != nil {
		return captured, err
	}
	captured.Inputs = inputs
	captured.Graph, err = BuildBackingZoneWorkGraph(task, bridge, revision, impact, sources, plans)
	if err != nil {
		return captured, err
	}
	for index, input := range inputs {
		item := captured.Graph.Items[index]
		capture, err := attachremoval.NewDetachCapture(captured.Graph.Task, item, input)
		if err != nil {
			return captured, err
		}
		source := input.Snapshot.Source
		if attachremoval.NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" &&
			source.Backing.Desired.Hooks != nil {
			capture.Hook, err = facts.SealBackingHookInputsAtRevision(ctx, capture.OperationID,
				source.Attach.BackingProjectID, *source.Backing.Desired.Hooks, revision)
			if err != nil {
				capture.Clear()
				return captured, err
			}
			if capture.Hook != nil {
				capture.Sources = append([]tasksecretpinrecord.Record(nil), capture.Hook.SecretSources...)
			}
		}
		captured.Hooks = append(captured.Hooks, capture)
	}
	return captured, nil
}
