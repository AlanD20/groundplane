package networkwork

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

func UnstartedKey(workID string) string { return "/v1/network-unstarted/" + workID }

type UnstartedResult struct {
	TaskID     string                `json:"task_id"`
	WorkItemID string                `json:"work_item_id"`
	NetworkID  string                `json:"network_id"`
	Input      workexecution.Input   `json:"input"`
	Outcome    workexecution.Outcome `json:"outcome"`
	At         time.Time             `json:"at"`
}

func (repository *InputRepository) PrepareUnstarted(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Operation != workexecution.Network || item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Network cannot settle as never assigned")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Network work has not been stopped or expired")
	}
	input, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.NetworkId != item.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, conflict("Network settlement differs from its captured resource")
	}
	value, err := recordcodec.Encode("network-unstarted", UnstartedResult{TaskID: task.ID, WorkItemID: item.ID,
		NetworkID: input.NetworkId, Input: *item.Input, Outcome: outcome, At: at})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, EffectsSettled: true, Evidence: workexecution.Input{
			ID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "network-unstarted"), SHA256: hex.EncodeToString(digest[:])},
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: UnstartedKey(item.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: UnstartedKey(item.ID), Value: value})
	return change, nil
}
