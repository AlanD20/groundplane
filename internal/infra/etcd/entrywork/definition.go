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

type DefinitionResult struct {
	TaskID                string    `json:"task_id"`
	WorkItemID            string    `json:"work_item_id"`
	EnvironmentID         string    `json:"environment_id"`
	EntryID               string    `json:"entry_id"`
	RevisionID            string    `json:"revision_id"`
	ConfigurationRevision int64     `json:"configuration_revision"`
	At                    time.Time `json:"at"`
}

func DefinitionKey(workID string) string { return "/v1/entry-work-definition/" + workID }

func PrepareDefinitionPublication(task workexecution.Task, item workexecution.Item, removal Removal,
	configurationRevision int64, marker idempotency.IdempotencyMarker, at time.Time,
) (workqueue.Change, error) {
	if task.Action != workexecution.ActionRemove || item.Operation != workexecution.EntryConfiguration ||
		item.Placement.Executor != workexecution.Controller || configurationRevision < 0 ||
		removal.EntryID != task.TargetID || removal.RevisionID != task.ID {
		return workqueue.Change{}, conflict("Entry definition settlement requires its exact removal")
	}
	value, err := recordcodec.Encode("entry-definition-result", DefinitionResult{
		TaskID: task.ID, WorkItemID: item.ID, EnvironmentID: task.Scope.EnvironmentID,
		EntryID: removal.EntryID, RevisionID: removal.RevisionID, ConfigurationRevision: configurationRevision, At: at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareSettledDecision(task, item, workexecution.Input{
		ID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "entry-definition-result"), SHA256: hex.EncodeToString(digest[:]),
	}, marker, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: DefinitionKey(item.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: DefinitionKey(item.ID), Value: value})
	return change, nil
}

func (repository *InputRepository) retireDefinition(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !final || task.Action != workexecution.ActionRemove || item.Input != nil || item.Result == nil ||
		item.AttemptNumber != 0 || item.Outcome != workexecution.Succeeded || item.Cleanup != workexecution.CleanupNone {
		return false, workqueue.Change{}, conflict("Entry definition retirement requires its completed decision")
	}
	read, err := repository.read(ctx, []string{DefinitionKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, workqueue.Change{}, conflict("Entry definition result is missing")
	}
	value := read.Values[0]
	result, err := recordcodec.Decode[DefinitionResult](value.Value, "entry-definition-result")
	digest := sha256.Sum256(value.Value)
	if err != nil || result.TaskID != task.ID || result.WorkItemID != item.ID ||
		result.EnvironmentID != task.Scope.EnvironmentID || result.EntryID != task.TargetID || result.RevisionID != task.ID ||
		!result.At.Equal(item.UpdatedAt) || result.ConfigurationRevision < 0 ||
		item.Result.ID != ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "entry-definition-result") ||
		item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return false, workqueue.Change{}, conflict("Entry definition result differs from its settled action")
	}
	return true, workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
			{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
			{Key: DefinitionKey(item.ID), ModRevision: value.ModRevision},
			{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: DefinitionKey(item.ID)}},
	}, nil
}
