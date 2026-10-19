package workqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func inputSettlementKey(workID string) string { return root + "input-settlements/" + workID }

// InputSettlement is evidence that no executable input or assignment existed.
// It does not claim operation-owned staging was collected or host effects undone.
type InputSettlement struct {
	TaskID       string                `json:"task_id"`
	WorkID       string                `json:"work_id"`
	Outcome      workexecution.Outcome `json:"outcome"`
	Reason       string                `json:"reason"`
	At           time.Time             `json:"at"`
	DependencyID string                `json:"dependency_id,omitempty"`
	Authority    *workexecution.Input  `json:"authority,omitempty"`
}

// PrepareInputDecision records a captured operation policy that makes this
// never-executable item unnecessary. The accepted Task intent authorizes the
// decision; callers join the exact policy/source comparisons before committing.
// This is not success inferred from a missing input or a skipped host command.
func PrepareInputDecision(
	view View,
	authority workexecution.Input,
	outcome workexecution.Outcome,
	reason string,
	at time.Time,
) (Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !waitingInput(item) || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.Result != nil || item.FirstFailure != nil ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		authority.Validate() != nil || authority != task.Intent ||
		outcome != workexecution.Succeeded && outcome != workexecution.Failed && outcome != workexecution.Cancelled &&
			outcome != workexecution.Superseded ||
		at.IsZero() || at.Location() != time.UTC || at.Before(task.UpdatedAt) || at.Before(item.UpdatedAt) ||
		reason == "" || len(reason) > 512 || strings.TrimSpace(reason) != reason ||
		task.Stop != nil && outcome != task.Stop.Outcome {
		return Change{}, invalid("input decision requires an accepted policy and never-executable work")
	}
	return prepareInputSettlement(view, InputSettlement{
		TaskID: task.ID, WorkID: item.ID, Outcome: outcome, Reason: reason, At: at, Authority: &authority,
	})
}

// PrepareInputSupersession closes a captured preparation that has not acquired
// executable input. The owner must prove the newer resource source in the same
// transaction; this receipt cannot stand in for settlement of host effects.
func PrepareInputSupersession(view View, source []keyvalue.Condition, at time.Time) (Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !waitingInput(item) || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.Result != nil || item.FirstFailure != nil ||
		task.Stop != nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		len(
			source,
		) == 0 || at.IsZero() || at.Location() != time.UTC || at.Before(task.UpdatedAt) || at.Before(item.UpdatedAt) {
		return Change{}, invalid("input supersession requires unstarted preparation and current resource evidence")
	}
	change, err := prepareInputSettlement(view, InputSettlement{
		TaskID: task.ID, WorkID: item.ID, Outcome: workexecution.Superseded,
		Reason: "Captured resource configuration was superseded before executable input was published", At: at,
	})
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, source...)
	return change, nil
}

// PrepareInputRejection records an operation owner's permanent refusal before
// any executable input exists. The caller must join the exact resource evidence
// that made preparation impossible; a transient storage error is not rejection.
func PrepareInputRejection(view View, reason string, at time.Time) (Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !waitingInput(item) || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.Result != nil || item.FirstFailure != nil ||
		task.Stop != nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(task.UpdatedAt) || at.Before(item.UpdatedAt) ||
		!at.Before(
			task.Deadline,
		) || !at.Before(item.Deadline) || reason == "" || len(reason) > 512 || strings.TrimSpace(reason) != reason {
		return Change{}, invalid("input rejection requires current unstarted work and an operation-owned reason")
	}
	return prepareInputSettlement(view, InputSettlement{
		TaskID: task.ID, WorkID: item.ID, Outcome: workexecution.Failed, Reason: reason, At: at,
	})
}

// PrepareInputSettlement closes only work that never became executable. Task
// and item CAS race safely with input publication; operation-owned staging is
// retained for its ordinary retirement owner rather than deleted here.
func PrepareInputSettlement(view View, at time.Time) (Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !waitingInput(item) || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.Result != nil || item.FirstFailure != nil ||
		task.FinishedAt != nil || !task.PublicationComplete || task.CollectionStartedAt != nil ||
		!at.Equal(at.UTC()) || at.IsZero() || at.Before(task.UpdatedAt) || at.Before(item.UpdatedAt) {
		return Change{}, invalid("input settlement requires unstarted waiting work")
	}
	outcome, reason := workexecution.Failed, "Executable input was not ready before the deadline"
	if task.Stop != nil {
		outcome = task.Stop.Outcome
		reason = "Stopped before executable input was published"
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return Change{}, invalid("input preparation has neither expired nor been stopped")
	}
	return prepareInputSettlement(view, InputSettlement{
		TaskID: task.ID, WorkID: item.ID, Outcome: outcome, Reason: reason, At: at,
	})
}

// A failed required dependency makes executable input impossible. Compare the
// dependency outcome with settlement so a concurrent permitted retry cannot be
// mistaken for a permanently failed prerequisite.
func PrepareInputDependencyFailure(view View, dependency ItemSnapshot, at time.Time) (Change, error) {
	task, item, prerequisite := view.Task.Task, view.Item.Item, dependency.Item
	if !waitingInput(item) || item.AttemptNumber != 0 || task.Stop != nil ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		dependency.Revision <= 0 || dependency.Revision > view.ReadRevision || prerequisite.Validate() != nil ||
		prerequisite.TaskID != task.ID || !slices.Contains(item.Dependencies, prerequisite.ID) ||
		prerequisite.Phase != workexecution.Settled || prerequisite.Outcome == workexecution.Succeeded {
		return Change{}, invalid("input failure requires a settled unsuccessful captured dependency")
	}
	change, err := prepareInputSettlement(view, InputSettlement{
		TaskID: task.ID, WorkID: item.ID, Outcome: workexecution.Failed,
		Reason: "Required dependency did not succeed", DependencyID: prerequisite.ID, At: at,
	})
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: ItemKey(prerequisite.ID), ModRevision: dependency.Revision},
	)
	return change, nil
}

func prepareInputSettlement(view View, result InputSettlement) (Change, error) {
	item := view.Item.Item
	value, err := encode("execution_input_settlement", result)
	if err != nil {
		return Change{}, err
	}
	digest := sha256.Sum256(value)
	evidence := workexecution.Input{
		ID: ids.DeriveAt(
			ids.KindPlan,
			item.CreatedAt,
			item.ID,
			"input-settlement",
		), SHA256: hex.EncodeToString(digest[:]),
	}
	change, err := PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: result.Outcome, Evidence: evidence, EffectsSettled: true,
	}, result.At)
	if err != nil {
		clear(value)
		return Change{}, err
	}
	key := inputSettlementKey(item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
