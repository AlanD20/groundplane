package entrywork

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
	configurationRevision int64, at time.Time,
) (workexecution.Item, workqueue.Change, error) {
	if task.Action != workexecution.ActionRemove || item.Operation != workexecution.EntryConfiguration ||
		item.Placement.Executor != workexecution.Controller || configurationRevision < 0 ||
		removal.EntryID != task.TargetID || removal.RevisionID != task.ID {
		return workexecution.Item{}, workqueue.Change{}, conflict(
			"Entry definition settlement requires its exact removal",
		)
	}
	return prepareDefinitionDecision(task, item, removal.EntryID, configurationRevision, at)
}

// PrepareMetadataPublication settles an Entry authoring action with no host
// effects. Its publisher owns the proof that no runtime or file is affected.
func PrepareMetadataPublication(task workexecution.Task, item workexecution.Item,
	configurationRevision int64, at time.Time,
) (workexecution.Item, workqueue.Change, error) {
	if (task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) ||
		item.Operation != workexecution.EntryConfiguration ||
		item.Placement.Executor != workexecution.Controller ||
		configurationRevision < 0 ||
		!definitionTarget(task) {
		return workexecution.Item{}, workqueue.Change{}, conflict("Entry metadata requires its exact Controller action")
	}
	return prepareMetadataDecision(task, item, configurationRevision, at)
}

func prepareDefinitionDecision(task workexecution.Task, item workexecution.Item, targetID string,
	configurationRevision int64, at time.Time,
) (workexecution.Item, workqueue.Change, error) {
	value, err := recordcodec.Encode("entry-definition-result", DefinitionResult{
		TaskID: task.ID, WorkItemID: item.ID, EnvironmentID: task.Scope.EnvironmentID,
		EntryID: targetID, RevisionID: task.ID, ConfigurationRevision: configurationRevision, At: at,
	})
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	ready, err := workexecution.SettleUnstarted(
		item,
		workexecution.Completion{Outcome: workexecution.Succeeded, EffectsSettled: true, Evidence: workexecution.Input{
			ID: ids.DeriveAt(
				ids.KindPlan,
				item.CreatedAt,
				item.ID,
				"entry-definition-result",
			), SHA256: hex.EncodeToString(digest[:]),
		}},
		at,
	)
	if err != nil {
		clear(value)
		return workexecution.Item{}, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: DefinitionKey(item.ID)}}}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: DefinitionKey(item.ID), Value: value},
	)
	return ready, change, nil
}

func (repository *InputRepository) retireDefinition(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	if !final {
		return false, workqueue.Change{}, conflict("Entry definition retirement requires its completed decision")
	}
	conditions, err := repository.DefinitionConditions(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	key := DefinitionKey(view.Item.Item.ID)
	if view.Item.Item.Result != nil && view.Item.Item.Result.ID == metadataResultID(view.Item.Item) {
		key = MetadataKey(view.Item.Item.ID)
	}
	return true, workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}}, nil
}

// DefinitionConditions binds Task completion and later collection to the same
// immutable Controller result, not merely to an apparently settled item.
func (repository *InputRepository) DefinitionConditions(
	ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Result != nil && item.Result.ID == metadataResultID(item) {
		return repository.metadataConditions(ctx, view)
	}
	if task.Validate() != nil || item.Validate() != nil || view.ReadRevision <= 0 || view.Task.Revision <= 0 ||
		view.Item.Revision <= 0 ||
		task.Action != workexecution.ActionRemove || ids.Validate(ids.KindEnvEntry, task.TargetID) != nil ||
		item.TaskID != task.ID ||
		item.Operation != workexecution.EntryConfiguration ||
		item.TargetID != task.Scope.EnvironmentID ||
		item.Placement.Executor != workexecution.Controller ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Input != nil ||
		item.Result == nil ||
		item.AttemptNumber != 0 ||
		item.Outcome != workexecution.Succeeded ||
		item.Cleanup != workexecution.CleanupNone {
		return nil, conflict("Entry definition requires its settled Controller action")
	}
	read, err := repository.read(ctx, []string{DefinitionKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Entry definition result is missing")
	}
	value := read.Values[0]
	result, err := recordcodec.Decode[DefinitionResult](value.Value, "entry-definition-result")
	digest := sha256.Sum256(value.Value)
	if err != nil || result.TaskID != task.ID || result.WorkItemID != item.ID ||
		result.EnvironmentID != task.Scope.EnvironmentID || result.EntryID != task.TargetID || result.RevisionID != task.ID ||
		!result.At.Equal(item.UpdatedAt) || result.ConfigurationRevision < 0 ||
		item.Result.ID != ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "entry-definition-result") ||
		item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return nil, conflict("Entry definition result differs from its settled action")
	}
	return []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: DefinitionKey(item.ID), ModRevision: value.ModRevision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true}}, nil
}

func definitionTarget(task workexecution.Task) bool {
	if task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate &&
		task.Action != workexecution.ActionRemove {
		return false
	}
	return ids.Validate(ids.KindEnvEntry, task.TargetID) == nil ||
		task.Action == workexecution.ActionUpdate && task.TargetID == task.Scope.EnvironmentID
}
