package connectorchecks

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutionResult is immutable completion evidence. It deliberately contains no
// Connector credentials, ciphertext, command output or cleanup object contents.
type ExecutionResult struct {
	Owner           Owner                              `json:"owner"`
	AttemptID       string                             `json:"attempt_id"`
	Outcome         Outcome                            `json:"outcome"`
	WorkOutcome     workexecution.Outcome              `json:"work_outcome"`
	Failure         s3connector.ConnectionCheckFailure `json:"failure"`
	Stages          []s3connector.ConnectionCheckStage `json:"stages"`
	CleanupComplete bool                               `json:"cleanup_complete"`
	RecordedAt      time.Time                          `json:"recorded_at"`
}

func ResultKey(attemptID string) string { return "/v1/private/connector-check-results/" + attemptID }

// PrepareResult must join workqueue.PrepareCompletion in one transaction. Only
// the operation's durable state proves effects settled; a nil executor error or
// a disconnected process does not. Cleanup success preserves the original work
// failure through the work model's separate recovery outcome.
func PrepareResult(snapshot keyvalue.Versioned[Record], task workqueue.TaskSnapshot, attempt workexecution.Attempt, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || record.Validate() != nil || attempt.Validate() != nil ||
		task.Revision <= 0 || task.Task.Validate() != nil || task.Task.ID != attempt.TaskID ||
		record.Owner.TaskID != attempt.TaskID || record.Owner.WorkItemID != attempt.WorkItemID ||
		attempt.FinishedAt != nil || attempt.Executor.Executor != workexecution.Controller ||
		!validTime(at) || at.Before(record.CreatedAt) ||
		record.Phase != Finished && record.Phase != CleanupRequired {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "connector check has no settled execution result")
	}
	digest, err := PlanHash(record)
	if err != nil || attempt.Input.ID != record.Owner.PlanID || attempt.Input.SHA256 != digest {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "connector check result changed captured input")
	}
	outcome := workexecution.Failed
	if record.Phase == Finished && (record.Outcome == Passed || attempt.Direction == workexecution.Recovery) {
		outcome = workexecution.Succeeded
	}
	if record.Phase == Finished && attempt.StartedAt == nil && attempt.Direction == workexecution.Forward && task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	result := ExecutionResult{Owner: record.Owner, AttemptID: attempt.ID, Outcome: record.Outcome, WorkOutcome: outcome,
		Failure: record.Failure, Stages: slices.Clone(record.Stages), CleanupComplete: record.Phase == Finished,
		RecordedAt: at}
	value, err := recordcodec.Encode("connector_execution_result", result)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	hash := sha256.Sum256(value)
	evidence := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "connector-check-result"), SHA256: hex.EncodeToString(hash[:])}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: Key(record.Owner.TaskID), ModRevision: snapshot.Revision},
			{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision}, {Key: ResultKey(attempt.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: value}},
	}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: record.Phase == Finished}, nil
}
