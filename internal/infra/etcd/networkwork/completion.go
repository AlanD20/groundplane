package networkwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareCompletionProof requires the final acknowledged host observation, not
// merely a successful helper exit. The operation owner joins this proof with
// resource publication and queue completion in one transaction. In particular,
// this proof alone never deletes a Network definition or releases resource claims.
func PrepareCompletionProof(ctx context.Context, reader ReportReader, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, at time.Time,
) (workqueue.Change, error) {
	attempt, checkpoint := view.Attempt.Attempt, view.Checkpoint.Record
	if reader == nil || input == nil || !view.ActiveAttempt || attempt.FinishedAt != nil ||
		checkpoint == nil || checkpoint.AttemptID != attempt.ID || view.Work.ReadRevision <= 0 ||
		at.IsZero() || at.Before(checkpoint.RecordedAt) {
		return workqueue.Change{}, conflict("network completion requires its final acknowledged observation")
	}
	key := ReportKey(attempt.ID, checkpoint.Sequence)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 ||
		read.Values[0] == nil || read.Values[0].Key != key || read.Values[0].ModRevision <= 0 ||
		read.Values[0].ModRevision > view.Work.ReadRevision {
		return workqueue.Change{}, conflict("network completion has no retained checkpoint report")
	}
	progress, err := networkexecution.DecodeProgress(attempt, input.Action, input.NetworkId, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if progress.Stage != networkexecution.Complete || progress.InFlight || progress.Observation == nil {
		return workqueue.Change{}, conflict("network operation has not completed all required stages")
	}
	// Reuse exact replay validation to bind the final report to its captured
	// content and saved checkpoint. It adds no permission to perform another effect.
	change, err := PrepareCheckpoint(ctx, reader, view, input, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
	)
	return change, nil
}
