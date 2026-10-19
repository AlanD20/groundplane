package imageoperations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// CompleteDigestFetch applies only to digest-only pulls. Tagged pulls must use
// RecordTagOutcome so they cannot bypass the tag-write fence and receipt.
func (journal *Journal) CompleteDigestFetch(ctx context.Context, imageID string, at time.Time) error {
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Input.Fetch == nil ||
		record.Phase != Fetching && !(record.Phase == Uncertain && record.UncertainPhase == Fetching) ||
		imageID != record.Input.Fetch.ConfigDigest {
		return conflict()
	}
	tag, err := record.Input.Fetch.TagReference()
	if err != nil {
		return err
	}
	if tag != "" {
		return conflict()
	}
	record.Phase, record.UncertainPhase = Finished, ""
	record.ImageID, record.Outcome = imageID, workexecution.Succeeded
	record.UpdatedAt, record.FinishedAt = at, &at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}

func ResultKey(attemptID string) string { return "/v1/evidence/image-attempts/" + attemptID }

// Result releases worker capacity independently of unresolved effects. Unknown
// writes produce failed attempt evidence with EffectsSettled=false, retaining
// resource claims and operation guards. Evidence is immutable per attempt.
func Result(
	snapshot keyvalue.Versioned[Record],
	task workqueue.TaskSnapshot,
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || task.Revision <= 0 || record.Validate() != nil ||
		record.Phase != Finished && record.Phase != Uncertain ||
		task.Task.Validate() != nil ||
		attempt.Validate() != nil ||
		attempt.FinishedAt != nil ||
		record.Input.TaskID != task.Task.ID ||
		attempt.TaskID != task.Task.ID ||
		record.Input.WorkItemID != attempt.WorkItemID ||
		record.AttemptID != attempt.ID ||
		record.Input.Placement != (workexecution.Placement{Executor: attempt.Executor.Executor, HostID: attempt.Executor.HostID}) {
		return workqueue.Change{}, workexecution.Completion{}, conflict()
	}
	input, err := record.Input.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if input != task.Task.Intent || input != attempt.Input {
		return workqueue.Change{}, workexecution.Completion{}, conflict()
	}
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	digest := sha256.Sum256(value)
	outcome, settled := record.Outcome, record.Phase == Finished
	if !settled {
		outcome = workexecution.Failed
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: Key(task.Task.ID), ModRevision: snapshot.Revision},
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: ResultKey(attempt.ID)},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: value}}}, workexecution.Completion{Outcome: outcome, EffectsSettled: settled,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, record.Input.CreatedAt, attempt.ID, "image-result"),
			SHA256: hex.EncodeToString(digest[:]),
		}}, nil
}
