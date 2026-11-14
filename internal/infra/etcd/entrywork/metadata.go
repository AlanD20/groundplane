package entrywork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type MetadataResult struct {
	TaskID                string    `json:"task_id"`
	WorkItemID            string    `json:"work_item_id"`
	EnvironmentID         string    `json:"environment_id"`
	TargetID              string    `json:"target_id"`
	RevisionID            string    `json:"revision_id"`
	ConfigurationRevision int64     `json:"configuration_revision"`
	At                    time.Time `json:"at"`
}

func MetadataKey(workID string) string { return "/v1/entry-work-metadata/" + workID }

func metadataResultID(item workexecution.Item) string {
	return ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "entry-metadata-result")
}

func prepareMetadataDecision(task workexecution.Task, item workexecution.Item, configurationRevision int64,
	marker idempotency.IdempotencyMarker, at time.Time,
) (workqueue.Change, error) {
	value, err := recordcodec.Encode("entry-metadata-result", MetadataResult{
		TaskID: task.ID, WorkItemID: item.ID, EnvironmentID: task.Scope.EnvironmentID,
		TargetID: task.TargetID, RevisionID: task.ID, ConfigurationRevision: configurationRevision, At: at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareSettledDecision(task, item, workexecution.Input{
		ID: metadataResultID(item), SHA256: hex.EncodeToString(digest[:]),
	}, marker, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: MetadataKey(item.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: MetadataKey(item.ID), Value: value})
	return change, nil
}

func (repository *InputRepository) metadataConditions(ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || !definitionTarget(task) ||
		view.ReadRevision <= 0 || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		item.TaskID != task.ID || item.Operation != workexecution.EntryConfiguration ||
		item.TargetID != task.Scope.EnvironmentID || item.Placement.Executor != workexecution.Controller ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Input != nil || item.Result == nil ||
		item.AttemptNumber != 0 || item.Outcome != workexecution.Succeeded || item.Cleanup != workexecution.CleanupNone {
		return nil, conflict("Entry metadata requires its settled Controller action")
	}
	read, err := repository.read(ctx, []string{MetadataKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Entry metadata result is missing")
	}
	value := read.Values[0]
	result, err := recordcodec.Decode[MetadataResult](value.Value, "entry-metadata-result")
	digest := sha256.Sum256(value.Value)
	if err != nil || result.TaskID != task.ID || result.WorkItemID != item.ID ||
		result.EnvironmentID != task.Scope.EnvironmentID || result.TargetID != task.TargetID || result.RevisionID != task.ID ||
		!result.At.Equal(item.UpdatedAt) || result.ConfigurationRevision < 0 ||
		item.Result.ID != metadataResultID(item) || item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return nil, conflict("Entry metadata result differs from its settled action")
	}
	return []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: MetadataKey(item.ID), ModRevision: value.ModRevision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true}}, nil
}
