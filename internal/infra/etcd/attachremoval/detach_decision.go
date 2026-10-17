package attachremoval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type detachDecision struct {
	ID         string                `json:"id"`
	TaskID     string                `json:"task_id"`
	WorkItemID string                `json:"work_item_id"`
	AttachID   string                `json:"attach_id"`
	Input      workexecution.Input   `json:"input"`
	Outcome    workexecution.Outcome `json:"outcome"`
	At         time.Time             `json:"at"`
}

func DetachDecisionKey(workID, evidenceID string) string {
	return DetachInputPrefix(workID) + "decisions/" + evidenceID
}

// PrepareDetachDecision handles a captured no-host-effect binding or a stopped
// never-assigned action. A successful metadata-only removal must additionally
// join PrepareDecisionFence before commit; this method grants no host effect.
func (repository *InputRepository) PrepareDetachDecision(ctx context.Context, view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.Operation != workexecution.Detach || item.Input == nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || !task.PublicationComplete || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach cannot settle as an unassigned decision")
	}
	input, guards, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	outcome := workexecution.Succeeded
	switch {
	case task.Stop != nil:
		outcome = task.Stop.Outcome
	case item.ForwardRevoked:
		outcome = workexecution.Superseded
	case !at.Before(item.Deadline) || !at.Before(task.Deadline):
		outcome = workexecution.Failed
	case input.HasHostEffects() || item.Placement.Executor != workexecution.Controller:
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach's captured effects require host execution")
	}
	var resource workqueue.Change
	if outcome == workexecution.Succeeded {
		resource, err = repository.prepareDetachRemoval(ctx, view, input)
	} else {
		resource, err = repository.PrepareDetachFailed(ctx, view)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	evidenceID := ids.New(ids.KindPlan)
	raw, err := recordcodec.Encode("detach-decision", detachDecision{ID: evidenceID, TaskID: task.ID, WorkItemID: item.ID,
		AttachID: input.Snapshot.Source.Attach.ID, Input: *item.Input, Outcome: outcome, At: at})
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(raw)
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{Outcome: outcome,
		EffectsSettled: true, Evidence: workexecution.Input{ID: evidenceID,
			SHA256: hex.EncodeToString(digest[:])}}, at)
	if err != nil {
		clear(raw)
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: DetachDecisionKey(item.ID, evidenceID)})
	change.Mutations = append(change.Mutations, resource.Mutations...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: DetachDecisionKey(item.ID, evidenceID), Value: raw})
	return change, nil
}
