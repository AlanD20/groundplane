package handlers

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func applyTaskRecovery(
	response *apiTypes.Task,
	task etcd.TaskRecord,
	events etcd.TaskEventSnapshot,
	recovery *etcd.TaskRecoverySnapshot,
) error {
	if recovery == nil {
		return nil
	}
	lastFailure, err := taskRecoveryFailure(task, events, recovery)
	if err != nil {
		return err
	}
	original := task
	original.Status, original.Result = recovery.PrimaryStatus, &recovery.PrimaryResult
	response.Failure = taskFailure(original)
	response.ReconciliationRequired = recovery.State != "completed"
	response.Recovery = &apiTypes.TaskRecovery{
		State: recovery.State, Summary: taskRecoverySummary(recovery),
		StepIDs:           slices.Clone(recovery.StepIDs),
		Deadline:          recovery.Deadline,
		LastFailure:       lastFailure,
		AutomaticFailures: recovery.AutomaticFailures,
		RetryAfter:        recovery.RetryAfter,
		RetryAvailable:    recovery.RetryAvailable,
		StopReason:        recovery.StopReason,
	}
	if len(recovery.StepIDs) > 0 {
		total := int32(len(recovery.StepIDs))
		response.Recovery.TotalSteps = &total
	}
	if recovery.CompletedSteps != nil {
		completed := int32(*recovery.CompletedSteps)
		response.Recovery.CompletedSteps = &completed
	}
	if recovery.State == "pending" && taskRecoveryStarted(task, events, recovery) {
		response.Recovery.State = "running"
		response.Recovery.Summary = "Recovery has started. Cleanup or restoration has not been verified; recorded progress does not prove the executor is still connected."
	}
	return nil
}

func taskRecoveryStarted(
	task etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
	recovery *etcd.TaskRecoverySnapshot,
) bool {
	for _, checkpoint := range task.EventCheckpoints {
		if checkpoint.Identity.Attempt > recovery.OriginalExecutionEpoch &&
			slices.Contains(recovery.StepIDs, checkpoint.Identity.StepID) {
			return true
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity.Attempt > recovery.OriginalExecutionEpoch &&
			slices.Contains(recovery.StepIDs, event.Identity.StepID) {
			return true
		}
	}
	return false
}

func taskRecoverySummary(recovery *etcd.TaskRecoverySnapshot) string {
	switch recovery.State {
	case "pending":
		return "Recovery is queued. Cleanup or restoration has not been verified."
	case "awaiting_confirmation":
		return "Recovery steps finished; the Controller has not yet accepted the final recovery evidence."
	case "proof_required":
		return "The recovery deadline passed. Automatic execution is stopped; explicit Retry can authorize another bounded recovery attempt. Affected resources remain protected until restoration is verified."
	case "automatic_retries_stopped":
		if recovery.StopReason == "unsafe_replay" {
			return "Automatic recovery stopped because repeating this action may be unsafe. Review its captured failure before explicitly selecting Retry; affected resources remain protected."
		}
		return "Automatic recovery retries stopped after a non-transient failure or an exhausted retry budget. Inspect the failure and use Retry explicitly; affected resources remain protected."
	case "retry_wait":
		return "Recovery is waiting before a bounded retry of its captured restoration steps. Forward actions are not rerun."
	case "completed":
		return "GP verified the captured recovery outcome. This does not undo database writes or arbitrary scripts; the original operation did not succeed."
	default:
		return "Recovery evidence is unavailable."
	}
}

// Step identities separate the restoration procedure from the failed forward
// procedure. Detach and Zone recovery reuse a step, so also require a later
// execution epoch. Sequence orders different steps; epoch orders executions.
func taskRecoveryFailure(
	task etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
	recovery *etcd.TaskRecoverySnapshot,
) (*apiTypes.TaskFailure, error) {
	var latest *apiTypes.TaskFailure
	var attempt uint32
	var sequence uint64
	observe := func(identity taskjournal.TaskEventIdentity, seq uint64, state taskjournal.TaskEventState, failure *taskprogress.Failure) {
		if !slices.Contains(recovery.StepIDs, identity.StepID) || identity.Attempt <= recovery.OriginalExecutionEpoch ||
			identity.Attempt < attempt || identity.Attempt == attempt && seq <= sequence {
			return
		}
		if state != taskjournal.TaskEventStateFailed && state != taskjournal.TaskEventStateTimedOut &&
			state != taskjournal.TaskEventStateAborted {
			return
		}
		attempt, sequence = identity.Attempt, seq
		latest = &apiTypes.TaskFailure{
			Status: string(state), StepID: identity.StepID,
			Summary: "This recovery step did not complete. No more specific cause was retained in its event.",
		}
		if failure != nil {
			latest = taskStepFailure(string(state), identity.StepID, failure)
		}
	}
	for _, event := range task.EventCheckpoints {
		observe(event.Identity, event.Sequence, event.State, event.Failure)
	}
	for _, event := range snapshot.Events {
		payload, err := taskprogress.DecodePayload(event.Payload)
		if err != nil {
			return nil, err
		}
		observe(event.Identity, event.Sequence, event.State, payload.Failure)
	}
	return latest, nil
}
