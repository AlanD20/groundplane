package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func DetachResultKey(workID string) string { return DetachInputPrefix(workID) + "result" }

// PrepareDetachCompletion removes only the binding, facts and reverse indexes
// proven by the final captured command checkpoint. The caller MUST join this
// change to native attempt completion; no resource is removed independently.
func (repository *InputRepository) PrepareDetachCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, error) {
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != uint64(input.CommandCount())*2 || at.Before(view.Checkpoint.Record.RecordedAt) ||
		input.Attempt.Direction != workexecution.Forward {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach has no complete active command checkpoint")
	}
	capture, err := repository.LoadDetachCapture(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if err := validateDetachDeliverySource(ctx, capture, view, input); err != nil {
		return workqueue.Change{}, err
	}
	key := WorkReportKey(input.Attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	report, err := attachexecution.DecodeRemovalReport(input, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	checkpoint, err := report.Checkpoint(input)
	if err != nil || !report.Progress.Complete(input) || !sameCheckpoint(checkpoint, *view.Checkpoint.Record) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach completion differs from its saved command evidence")
	}
	change, err := repository.PrepareDetachCheckpoint(ctx, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	resource, err := repository.prepareDetachRemoval(ctx, view.Work, capture.Input)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	authority, err := input.Authority()
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	raw, err := EncodeWorkResult(WorkResult{Attempt: input.Attempt, Authority: authority, Report: report})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: DetachResultKey(input.Attempt.WorkItemID)},
		keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	change.Mutations = append(change.Mutations, resource.Mutations...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: DetachResultKey(input.Attempt.WorkItemID), Value: raw})
	return change, nil
}

func (repository *InputRepository) prepareDetachRemoval(ctx context.Context, view workqueue.View,
	captured DetachInput,
) (workqueue.Change, error) {
	current, guards, err := repository.readOwnedDetach(ctx, view, captured)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, mutations, values, err := attachrecord.PrepareAttachRemoval(ctx, repository.store, current, view.ReadRevision)
	if err != nil {
		for _, value := range values {
			clear(value)
		}
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(guards, conditions...), Mutations: mutations}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, current.Record.EnvironmentID, view.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	source := captured.Snapshot.Source
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, view.ReadRevision,
		hierarchydeletion.MutationScope{TenantID: source.TenantID, ProjectID: source.ProjectID}, nil, nil)
	if err != nil {
		clear(epoch.Value)
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	// Transfer the epoch value buffers into the returned atomic fragment.
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Conditions = append(change.Conditions, ancestry.Conditions()...)
	change.Mutations = append(change.Mutations, ancestry.Mutations()...)
	change.Mutations = append(change.Mutations, epoch)
	progress, err := repository.prepareCascadeAdvance(ctx, view, captured)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, progress.Conditions...)
	change.Mutations = append(change.Mutations, progress.Mutations...)
	return change, nil
}
