package edgechecks

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutionResult keeps diagnostic observations separate from helper cleanup.
// It contains no runtime snapshot, tunnel token or request credential material.
type ExecutionResult struct {
	Owner           Owner                 `json:"owner"`
	AttemptID       string                `json:"attempt_id"`
	WorkOutcome     workexecution.Outcome `json:"work_outcome"`
	ProbeOutcome    string                `json:"probe_outcome"`
	Failure         string                `json:"failure,omitempty"`
	PrivateFailure  string                `json:"private_failure,omitempty"`
	PublicFailure   string                `json:"public_failure,omitempty"`
	PrivateStages   []contract.Stage      `json:"private_stages,omitempty"`
	PublicStages    []contract.Stage      `json:"public_stages,omitempty"`
	CleanupComplete bool                  `json:"cleanup_complete"`
	RecordedAt      time.Time             `json:"recorded_at"`
}

func ResultKey(attemptID string) string { return "/v1/private/edge-check-results/" + attemptID }

func PrepareResult(snapshot keyvalue.Versioned[Record], task workqueue.TaskSnapshot,
	attempt workexecution.Attempt, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || record.Validate() != nil || task.Revision <= 0 || task.Task.Validate() != nil ||
		attempt.Validate() != nil || attempt.FinishedAt != nil || attempt.Executor.Executor != workexecution.Controller ||
		record.Owner.TaskID != task.Task.ID || attempt.TaskID != task.Task.ID || record.Owner.WorkItemID != attempt.WorkItemID ||
		record.Phase != Finished &&
			record.Phase != CleanupRequired || !recordcodec.IsCanonicalUTC(at) || at.Before(record.CreatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"edge check has no settled operation result",
		)
	}
	digest, err := PlanHash(record)
	if err != nil || record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"edge result differs from captured input",
		)
	}
	outcome := workexecution.Failed
	if record.Phase == Finished &&
		(record.Outcome == "passed" || record.Outcome == "protected" || attempt.Direction == workexecution.Recovery) {
		outcome = workexecution.Succeeded
	}
	if record.Phase == Finished && attempt.StartedAt == nil && attempt.Direction == workexecution.Forward &&
		task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	result := ExecutionResult{Owner: record.Owner, AttemptID: attempt.ID, WorkOutcome: outcome,
		ProbeOutcome: record.Outcome, Failure: record.Failure, PrivateFailure: record.PrivateFailure,
		PublicFailure: record.PublicFailure, CleanupComplete: record.Phase == Finished, RecordedAt: at}
	if record.Private != nil {
		result.PrivateStages = slices.Clone(record.Private.Stages)
	}
	if record.Public != nil {
		result.PublicStages = slices.Clone(record.Public.Stages)
	}
	value, err := recordcodec.Encode("edge_execution_result", result)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	hash := sha256.Sum256(value)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "edge-check-result"),
		SHA256: hex.EncodeToString(hash[:]),
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: Key(record.Owner.TaskID), ModRevision: snapshot.Revision},
			{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision}, {Key: ResultKey(attempt.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: value}},
	}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: record.Phase == Finished}, nil
}
