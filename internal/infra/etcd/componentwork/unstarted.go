package componentwork

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

func UnstartedKey(workID string) string { return "/v1/component-unstarted/" + workID }

type UnstartedResult struct {
	TaskID      string                `json:"task_id"`
	WorkItemID  string                `json:"work_item_id"`
	ComponentID string                `json:"component_id"`
	Input       workexecution.Input   `json:"input"`
	Outcome     workexecution.Outcome `json:"outcome"`
	At          time.Time             `json:"at"`
}

func (repository *InputRepository) PrepareUnstarted(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Operation != workexecution.Component || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil ||
		!task.PublicationComplete ||
		task.FinishedAt != nil ||
		task.CollectionStartedAt != nil ||
		at.IsZero() ||
		at.Location() != time.UTC ||
		at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Component cannot settle as never assigned")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Component work has not been stopped or expired")
	}
	input, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.ComponentId != item.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, conflict("Component settlement differs from its captured resource")
	}
	value, err := recordcodec.Encode("component-unstarted", UnstartedResult{TaskID: task.ID, WorkItemID: item.ID,
		ComponentID: input.ComponentId, Input: *item.Input, Outcome: outcome, At: at})
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
				"component-unstarted",
			), SHA256: hex.EncodeToString(digest[:])},
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	fence, err := repository.settleAddresses(ctx, view, capture, capture.Current)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: UnstartedKey(item.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: UnstartedKey(item.ID), Value: value},
	)
	route, err := repository.failRoute(ctx, view, capture.Route)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, route.Conditions...)
	change.Mutations = append(change.Mutations, route.Mutations...)
	return change, nil
}
