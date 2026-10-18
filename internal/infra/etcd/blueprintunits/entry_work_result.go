package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// EntryWorkResult records generation identity, never plaintext or ciphertext.
// It is disposable execution evidence; the applied Entry owns its value lifetime.
type EntryWorkResult struct {
	TaskID            string              `json:"task_id"`
	WorkID            string              `json:"work_id"`
	EnvironmentID     string              `json:"environment_id"`
	EntryID           string              `json:"entry_id"`
	ValueGenerationID string              `json:"value_generation_id"`
	Input             workexecution.Input `json:"input"`
	SettledAt         time.Time           `json:"settled_at"`
}

func EntryWorkResultKey(workID string) string { return recordPrefix + "entry-results/" + workID }

func prepareEntryWorkResult(view workqueue.View, generationID string, at time.Time) (workexecution.Input, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Input == nil || ids.Validate(ids.KindConfig, generationID) != nil {
		return workexecution.Input{}, workqueue.Change{}, invalidRecord()
	}
	record := EntryWorkResult{TaskID: task.ID, WorkID: item.ID, EnvironmentID: task.Scope.EnvironmentID,
		EntryID: item.TargetID, ValueGenerationID: generationID, Input: *item.Input, SettledAt: at}
	value, err := recordcodec.Encode("blueprint-entry-work-result", record)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	evidence := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "entry-result"), SHA256: hex.EncodeToString(digest[:])}
	key := EntryWorkResultKey(item.ID)
	return evidence, workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: key}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}
