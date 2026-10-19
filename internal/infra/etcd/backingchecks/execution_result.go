package backingchecks

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutionResult keeps probe evidence separate from cleanup success. It holds
// neither encrypted credentials nor the consumer's captured configuration.
type ExecutionResult struct {
	Owner           Owner                 `json:"owner"`
	AttemptID       string                `json:"attempt_id"`
	WorkOutcome     workexecution.Outcome `json:"work_outcome"`
	ProbeOutcome    Outcome               `json:"probe_outcome"`
	Failure         string                `json:"failure,omitempty"`
	Stages          []backingaccess.Stage `json:"stages,omitempty"`
	CleanupComplete bool                  `json:"cleanup_complete"`
	RecordedAt      time.Time             `json:"recorded_at"`
}

func ResultKey(attemptID string) string { return "/v1/private/backing-check-results/" + attemptID }

// PrepareResult joins execution completion in the caller's atomic transaction.
// A missing helper response never certifies cleanup or access.
func PrepareResult(snapshot keyvalue.Versioned[Record], task workqueue.TaskSnapshot,
	attempt workexecution.Attempt, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || record.Validate() != nil || task.Revision <= 0 || task.Task.Validate() != nil ||
		attempt.Validate() != nil || attempt.FinishedAt != nil || attempt.Executor.Executor != workexecution.Controller ||
		record.Owner.TaskID != task.Task.ID || attempt.TaskID != task.Task.ID || record.Owner.WorkItemID != attempt.WorkItemID ||
		record.Phase != Finished && record.Phase != CleanupRequired || !validTime(at) || at.Before(record.CreatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"Backing check has no settled operation result",
		)
	}
	digest, err := PlanHash(record)
	if err != nil || record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"Backing check result changed its captured input",
		)
	}
	outcome := workexecution.Failed
	if record.Phase == Finished && (record.Outcome == Passed || attempt.Direction == workexecution.Recovery) {
		outcome = workexecution.Succeeded
	}
	if record.Phase == Finished && attempt.StartedAt == nil && attempt.Direction == workexecution.Forward &&
		task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	result := ExecutionResult{Owner: record.Owner, AttemptID: attempt.ID, WorkOutcome: outcome,
		ProbeOutcome: record.Outcome, Failure: record.Failure, CleanupComplete: record.Phase == Finished, RecordedAt: at}
	if record.Observation != nil {
		result.Stages = slices.Clone(record.Observation.Stages)
	}
	value, err := recordcodec.Encode("backing_execution_result", result)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	hash := sha256.Sum256(value)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "backing-check-result"),
		SHA256: hex.EncodeToString(hash[:]),
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: Key(record.Owner.TaskID), ModRevision: snapshot.Revision},
			{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision}, {Key: ResultKey(attempt.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: value}},
	}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: record.Phase == Finished}, nil
}
