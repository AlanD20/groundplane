package scriptexecutions

import (
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

func UnstartedKey(workID string) string { return "/v1/script-unstarted/" + workID }

// UnstartedResult records why no command ran, separately from a process outcome.
// The caller must join the captured execution/source comparisons to this change.
type UnstartedResult struct {
	TaskID      string                `json:"task_id"`
	WorkItemID  string                `json:"work_item_id"`
	ExecutionID string                `json:"execution_id"`
	Input       workexecution.Input   `json:"input"`
	Outcome     workexecution.Outcome `json:"outcome"`
	At          time.Time             `json:"at"`
}

func PrepareUnstartedStop(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	execution ScriptExecutionRecord, at time.Time,
) (workqueue.Change, error) {
	if task.Task.Action != workexecution.ActionScript || item.Item.Operation != workexecution.Script ||
		item.Item.TaskID != task.Task.ID || item.Item.AttemptNumber != 0 || item.Item.Input == nil ||
		ValidateScriptExecutionRecord(execution) != nil || execution.CurrentTaskID != task.Task.ID ||
		execution.WorkItemID != item.Item.ID || execution.Input != *item.Item.Input ||
		execution.State != ScriptExecutionNotStarted || execution.StartAuthorized || execution.AttemptID != "" ||
		execution.Executor != nil || execution.Recovery != nil || execution.ReconciliationRequired ||
		at.IsZero() || at.Location() != time.UTC || at.Before(execution.UpdatedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script cannot settle as never started")
	}
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	} else if item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Task.Deadline) && at.Before(item.Item.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script has no stop request or expired deadline")
	}
	value, err := recordcodec.Encode("script-unstarted", UnstartedResult{
		TaskID: task.Task.ID, WorkItemID: item.Item.ID, ExecutionID: execution.ID,
		Input: execution.Input, Outcome: outcome, At: at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{
		Outcome: outcome, EffectsSettled: true,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, item.Item.CreatedAt, item.Item.ID, "script-unstarted"),
			SHA256: hex.EncodeToString(digest[:]),
		},
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := UnstartedKey(item.Item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
