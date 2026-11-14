package workqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type revocationResult struct {
	TaskID     string                `json:"task_id"`
	WorkID     string                `json:"work_id"`
	Input      workexecution.Input   `json:"input"`
	Outcome    workexecution.Outcome `json:"outcome"`
	RevokedAt  time.Time             `json:"revoked_at"`
	RecordedAt time.Time             `json:"recorded_at"`
}

func revocationResultKey(workID string) string { return root + "revocation-results/" + workID }

func revocationResultID(item workexecution.Item) string {
	return ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "revocation-result")
}

// SealRevocationResult performs one bounded finalization step for captured work
// revoked before assignment. Revocation closes the forward gate; this receipt
// proves that no Agent attempt acquired it. It never claims that operation-owned
// staging was collected, changes the outcome, or invents an execution result.
// Action-specific aggregate completion and retention still run afterward.
func (repository *Repository) SealRevocationResult(ctx context.Context, taskID string, at time.Time) (bool, error) {
	task, revision, err := repository.ReadTask(ctx, taskID, 0)
	if err != nil || task.Task.FinishedAt != nil {
		return false, err
	}
	if at.IsZero() || at.Location() != time.UTC || at.Before(task.Task.UpdatedAt) ||
		!task.Task.PublicationComplete || task.Task.Progress.Unsettled() != 0 || task.Task.CollectionStartedAt != nil {
		return false, errs.New(errs.KindStateConflict, "Task is not ready to finalize revoked work")
	}
	after, count := "", 0
	for {
		page, err := repository.ListWork(ctx, taskID, after, 100, revision)
		if err != nil {
			return false, err
		}
		count += len(page.Items)
		if count > int(task.Task.ExpectedItems) {
			return false, corrupt()
		}
		for _, item := range page.Items {
			if item.Item.Result != nil {
				continue
			}
			change, err := prepareRevocationResult(task, item, at)
			if err != nil {
				return false, err
			}
			defer keyvalue.ClearMutationValues(change.Mutations)
			result, err := repository.Commit(ctx, change)
			keyvalue.ClearValues(result.FailureReads)
			if err != nil {
				return false, err
			}
			if !result.Succeeded {
				return false, errs.New(
					errs.KindStateConflict,
					"Revocation finalization lost its Task or work ownership",
				)
			}
			return true, nil
		}
		if !page.More {
			if count != int(task.Task.ExpectedItems) {
				return false, corrupt()
			}
			return false, nil
		}
		if len(page.Items) == 0 || page.LastID <= after {
			return false, corrupt()
		}
		after = page.LastID
	}
}

func prepareRevocationResult(task TaskSnapshot, snapshot ItemSnapshot, at time.Time) (Change, error) {
	item := snapshot.Item
	if snapshot.Revision <= 0 || item.Validate() != nil || item.TaskID != task.Task.ID ||
		item.Phase != workexecution.Settled || !item.ForwardRevoked || item.Input == nil || item.Result != nil ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.FirstFailure != nil ||
		item.Cleanup != workexecution.CleanupNone || item.CleanupResult != nil ||
		item.Outcome != workexecution.Cancelled &&
			item.Outcome != workexecution.Superseded || at.Before(item.UpdatedAt) {
		return Change{}, corrupt()
	}
	proof := revocationResult{TaskID: task.Task.ID, WorkID: item.ID, Input: *item.Input,
		Outcome: item.Outcome, RevokedAt: item.UpdatedAt, RecordedAt: at}
	raw, err := recordcodec.Encode("execution_revocation_result", proof)
	if err != nil {
		return Change{}, err
	}
	digest := sha256.Sum256(raw)
	next := workexecution.Clone(item)
	next.Result = &workexecution.Input{ID: revocationResultID(item), SHA256: hex.EncodeToString(digest[:])}
	next.UpdatedAt = at
	encoded, err := EncodeItem(next)
	if err != nil {
		clear(raw)
		return Change{}, err
	}
	change := Change{Conditions: []keyvalue.Condition{
		{Key: ItemKey(item.ID), ModRevision: snapshot.Revision},
		{Key: AttemptIndexPrefix(item.ID), Prefix: true},
		{Key: revocationResultKey(item.ID)},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: revocationResultKey(item.ID), Value: raw},
		{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: encoded},
	}}
	if err := countTransition(&change, task, item, next, at); err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return Change{}, err
	}
	return change, nil
}
