package dnschecks

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Result uses the finished record itself as immutable proof. DNS checks cannot
// retry their queries: a new check captures new resolver state in a new Task.
func Result(snapshot keyvalue.Versioned[Record], task workqueue.TaskSnapshot,
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || record.Validate() != nil || record.Phase != Finished ||
		task.Revision <= 0 || task.Task.Validate() != nil || attempt.Validate() != nil ||
		record.Owner.TaskID != task.Task.ID || attempt.TaskID != task.Task.ID || record.Owner.WorkItemID != attempt.WorkItemID ||
		attempt.FinishedAt != nil || attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"DNS check result is not settled for this attempt",
		)
	}
	digest, err := PlanHash(record)
	if err != nil || record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"DNS check result changed captured input",
		)
	}
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	hash := sha256.Sum256(value)
	outcome := workexecution.Failed
	if record.Outcome == "passed" {
		outcome = workexecution.Succeeded
	}
	if attempt.StartedAt == nil && task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: RecordKey(task.Task.ID), ModRevision: snapshot.Revision},
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
	}}, workexecution.Completion{Outcome: outcome, EffectsSettled: true,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "dns-check-result"),
			SHA256: hex.EncodeToString(hash[:]),
		}}, nil
}
