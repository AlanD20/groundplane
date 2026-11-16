package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CollectAbandonedGroupReleaseDraft removes expired inert compilation drafts.
// The same transaction excludes an execution capture and compares any remaining
// pure accepted Task. Promoted captures belong to native Task retention.
func (ledger *ReleaseLedger) CollectAbandonedGroupReleaseDraft(
	ctx context.Context,
	taskID string,
	at time.Time,
) (bool, error) {
	if ledger == nil || ledger.store == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		!recordcodec.IsCanonicalUTC(at) {
		return false, errs.New(errs.KindValidationFailed, "Group draft cleanup requires its exact identity and time")
	}
	key := groupReleaseDraftHeaderKey(taskID)
	taskKey, captureKey := workqueue.TaskKey(taskID), releasegroups.ExecutionCaptureKey(taskID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key, taskKey, captureKey}})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 3 {
		return false, errs.New(errs.KindStateConflict, "Group draft cleanup read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return false, nil
	}
	draft, err := recordcodec.Decode[groupReleaseDraft](value.Value, "group-release-draft")
	if err != nil || value.Key != key || value.ModRevision <= 0 || draft.TaskID != taskID ||
		draft.Intent.Validate() != nil || !recordcodec.IsCanonicalUTC(draft.CreatedAt) || !recordcodec.IsCanonicalUTC(draft.Deadline) ||
		!draft.Deadline.After(draft.CreatedAt) {
		return false, errs.New(errs.KindStateConflict, "Group draft cleanup found invalid ownership")
	}
	if at.Before(draft.Deadline) {
		return false, nil
	}
	if read.Values[2] != nil {
		return false, nil
	}
	taskGuard := keyvalue.Condition{Key: taskKey}
	if current := read.Values[1]; current != nil {
		task, err := workqueue.DecodeTask(current.Value)
		if err != nil {
			return false, err
		}
		// Pure accepted intent can exist before a compilation succeeds. Its
		// expired inert draft is not an acknowledged execution capture.
		if task.ID != taskID || task.AcceptedIntent == nil || task.Intent != (workexecution.Input{}) {
			return false, nil
		}
		taskGuard.ModRevision = current.ModRevision
	}
	result, err := ledger.store.Transact(ctx, []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision},
		taskGuard, {Key: captureKey}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: groupReleaseDraftKey(taskID), Prefix: true}})
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded, err
}
