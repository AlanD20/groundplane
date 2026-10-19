package attachprovision

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
)

func UnstartedKey(workID string) string { return "/v1/attach-provision-unstarted/" + workID }

type UnstartedResult struct {
	TaskID     string                `json:"task_id"`
	WorkItemID string                `json:"work_item_id"`
	AttachID   string                `json:"attach_id"`
	Input      workexecution.Input   `json:"input"`
	Outcome    workexecution.Outcome `json:"outcome"`
	At         time.Time             `json:"at"`
}

// PrepareUnstarted closes prepared provisioning that never received permission
// to execute. Its pending binding remains visible as failed, with captured facts
// and references retained; this is not a claim that a database was removed.
func (repository *InputRepository) PrepareUnstarted(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Operation != workexecution.Attach || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.Input == nil || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, checkpointConflict("Attach provisioning cannot settle as never assigned")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, checkpointConflict("Attach provisioning has not been stopped or expired")
	}
	source, err := repository.LoadCaptured(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	var binding workqueue.Change
	if !source.Generation.Record.SettlesInController() || task.Action == workexecution.ActionAttach {
		binding, err = repository.PrepareFailedBinding(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	transferred := false
	defer func() {
		if !transferred {
			keyvalue.ZeroMutationBytes(binding.Mutations)
		}
	}()
	value, err := recordcodec.Encode("attach-provision-unstarted", UnstartedResult{
		TaskID: task.ID, WorkItemID: item.ID, AttachID: item.TargetID, Input: *item.Input, Outcome: outcome, At: at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, EffectsSettled: true, Evidence: workexecution.Input{
			ID: ids.DeriveAt(
				ids.KindPlan,
				item.CreatedAt,
				item.ID,
				"attach-provision-unstarted",
			), SHA256: hex.EncodeToString(digest[:]),
		},
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: UnstartedKey(item.ID)})
	change.Mutations = append(change.Mutations, binding.Mutations...)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: UnstartedKey(item.ID), Value: value},
	)
	transferred = true
	return change, nil
}
