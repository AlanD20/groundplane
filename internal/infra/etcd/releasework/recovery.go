package releasework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// At most six recovery actions plus sixteen captured hooks retain compact
// checkpoint boundaries. Their bound is separate from a single forward cursor.
const maximumRecoveryBytes = 64 * 1024

// RecoveryRecord retains one proof reference per completed recovery action.
// In particular, final completion cannot erase the quiescence proof required
// before restoration. It contains no logs or copies of runtime configuration.
type RecoveryRecord struct {
	ForwardAttemptID     string                                      `json:"forward_attempt_id"`
	SourceResult         workexecution.Input                         `json:"source_result"`
	Group                *releaseexecution.GroupRestorationAuthority `json:"group,omitempty"`
	Interruption         *releaseexecution.RestorationInterruption   `json:"interruption,omitempty"`
	Report               releaseexecution.RecoveryReport             `json:"report"`
	ActiveHookID         string                                      `json:"active_hook_id,omitempty"`
	HookServingReleaseID string                                      `json:"hook_serving_release_id,omitempty"`
	Disposition          agentpb.WorkReleaseRecoveryDisposition      `json:"disposition"`
	Proofs               [22]*workexecution.Input                    `json:"proofs"`
	Boundaries           [22]*releaseexecution.RecoveryReport        `json:"boundaries"`
}

type RecoverySnapshot struct {
	Record   *RecoveryRecord
	Revision int64
}

func RecoveryKey(attemptID string) string { return "/v1/execution/release-recovery/" + attemptID }

func (record RecoveryRecord) validate(input releaseexecution.RecoveryInput) error {
	if input.Validate() != nil || record.ForwardAttemptID != input.Forward.ID ||
		record.SourceResult != *input.Forward.Result || record.Disposition != input.Disposition ||
		!releaseexecution.EqualGroupRestoration(record.Group, input.Group) ||
		!releaseexecution.EqualRestorationInterruption(record.Interruption, input.Interruption) ||
		record.Report.Ack != nil ||
		record.Report.Validate(input) != nil {
		return invalid()
	}
	completed := record.Report.Progress.Sequence / 2
	if completed > uint64(input.RecoveryActionCount()) {
		return invalid()
	}
	expectedHook := ""
	expectedServing := ""
	if record.Report.Progress.InFlight && record.Report.Progress.Phase == releaseexecution.FailureHooks {
		hook, err := record.Report.Progress.ActiveHook(input)
		if err != nil {
			return err
		}
		expectedHook = hook.ExecutionId
		expectedServing = input.HookServingReleaseID()
	}
	if record.ActiveHookID != expectedHook || record.HookServingReleaseID != expectedServing {
		return invalid()
	}
	for index, proof := range record.Proofs {
		boundary := record.Boundaries[index]
		if (uint64(index) < completed) != (proof != nil) || (proof != nil) != (boundary != nil) ||
			proof != nil && proof.Validate() != nil {
			return invalid()
		}
		if boundary != nil {
			if boundary.Ack == nil || boundary.Validate(input) != nil || !boundary.Progress.InFlight ||
				boundary.Progress.Sequence != uint64(
					index,
				)*2+1 || boundary.Progress.UpdatedAt.After(record.Report.Progress.UpdatedAt) {
				return invalid()
			}
			if index > 0 &&
				(boundary.Progress.Evidence == nil || *boundary.Progress.Evidence != *record.Proofs[index-1] ||
					boundary.Progress.UpdatedAt.Before(record.Boundaries[index-1].Progress.UpdatedAt)) {
				return invalid()
			}
		}
	}
	if completed > 0 &&
		(record.Report.Progress.Evidence == nil || *record.Report.Progress.Evidence != *record.Proofs[completed-1]) {
		return invalid()
	}
	return nil
}

func LoadRecovery(
	ctx context.Context,
	store Reader,
	input releaseexecution.RecoveryInput,
	revision int64,
) (RecoverySnapshot, error) {
	if keyvalue.ValidateContext(ctx) != nil || store == nil || revision <= 0 || input.Validate() != nil {
		return RecoverySnapshot{}, invalid()
	}
	key := RecoveryKey(input.Input.Attempt.ID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return RecoverySnapshot{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return RecoverySnapshot{}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision ||
		len(value.Value) > maximumRecoveryBytes {
		return RecoverySnapshot{}, invalid()
	}
	record, err := recordcodec.Decode[RecoveryRecord](value.Value, "release-work-recovery")
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if err := record.validate(input); err != nil {
		return RecoverySnapshot{}, err
	}
	return RecoverySnapshot{Record: &record, Revision: value.ModRevision}, nil
}

// PrepareRecovery must be joined with the operation owner's verified proof for
// Confirm and captured recovery authority for Begin. It does not authorize or
// perform host mutations independently, and Abort cannot skip required cleanup.
func PrepareRecovery(
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
	previous RecoverySnapshot,
	report releaseexecution.RecoveryReport,
) (workqueue.Change, error) {
	attempt := input.Input.Attempt
	if report.Ack != nil || report.Validate(input) != nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		(input.Group == nil && view.Work.Item.Item.Operation != workexecution.Release || input.Group != nil && view.Work.Item.Item.Operation != workexecution.ReleaseRestoration) ||
		!workexecution.SameAssignment(view.Attempt.Attempt, attempt) ||
		view.Attempt.Attempt.StartedAt == nil || attempt.StartedAt == nil || !view.Attempt.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
		previous.Revision < 0 || (previous.Record == nil) != (previous.Revision == 0) || (previous.Record == nil) != (view.Checkpoint.Record == nil) {
		return workqueue.Change{}, invalid()
	}
	record := RecoveryRecord{
		ForwardAttemptID: input.Forward.ID,
		SourceResult:     *input.Forward.Result,
		Disposition:      input.Disposition,
		Group: releaseexecution.CloneGroupRestoration(
			input.Group,
		),
		Interruption: releaseexecution.CloneRestorationInterruption(input.Interruption),
		Report:       report,
	}
	var prior *releaseexecution.RecoveryProgress
	if previous.Record != nil {
		if err := previous.Record.validate(input); err != nil {
			return workqueue.Change{}, err
		}
		expected, err := previous.Record.Report.Checkpoint(input)
		if err != nil || view.Checkpoint.Record.Sequence < expected.Sequence ||
			view.Checkpoint.Record.Sequence == expected.Sequence &&
				(!releaseexecution.EqualCheckpoint(expected, *view.Checkpoint.Record) || previous.Revision != view.Checkpoint.Revision) ||
			view.Checkpoint.Record.Sequence > expected.Sequence &&
				(!previous.Record.Report.Progress.InFlight || previous.Record.Report.Progress.Phase != releaseexecution.FailureHooks || report.Progress.InFlight) {
			return workqueue.Change{}, invalid()
		}
		prior = &previous.Record.Report.Progress
		record.Proofs = previous.Record.Proofs
		record.Boundaries = previous.Record.Boundaries
	}
	if err := releaseexecution.ValidateRecoveryTransition(input, prior, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	currentSequence := uint64(0)
	if view.Checkpoint.Record != nil {
		currentSequence = view.Checkpoint.Record.Sequence
	}
	replay := previous.Record != nil && report.Sequence == previous.Record.Report.Sequence &&
		report.StageID == previous.Record.Report.StageID &&
		releaseexecution.EqualRecoveryProgress(report.Progress, previous.Record.Report.Progress)
	if !replay && (currentSequence == ^uint64(0) || report.Sequence != currentSequence+1) {
		return workqueue.Change{}, invalid()
	}
	if report.Progress.InFlight && report.Progress.Phase == releaseexecution.FailureHooks {
		hook, err := report.Progress.ActiveHook(input)
		if err != nil {
			return workqueue.Change{}, err
		}
		record.ActiveHookID = hook.ExecutionId
		record.HookServingReleaseID = input.HookServingReleaseID()
	}
	if !report.Progress.InFlight {
		index := report.Progress.Sequence/2 - 1
		evidence := *report.Progress.Evidence
		if record.Proofs[index] != nil && *record.Proofs[index] != evidence {
			return workqueue.Change{}, invalid()
		}
		record.Proofs[index] = &evidence
		if record.Boundaries[index] == nil {
			boundary, err := activeRecoveryEffect(input, previous)
			if err != nil {
				return workqueue.Change{}, err
			}
			record.Boundaries[index] = &boundary
		}
	}
	if err := record.validate(input); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("release-work-recovery", record)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(value) > maximumRecoveryBytes {
		clear(value)
		return workqueue.Change{}, invalid()
	}
	checkpoint, err := report.Checkpoint(input)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: RecoveryKey(attempt.ID), ModRevision: previous.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: RecoveryKey(attempt.ID), Value: value},
	)
	return change, nil
}
