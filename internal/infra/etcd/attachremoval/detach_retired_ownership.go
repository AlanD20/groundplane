package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// retiredDetach is resource authority, not diagnostic history. Only collection
// of fully settled native work may publish it. It allows a new explicit action
// after the original Retry inputs expire, without inferring safety from a
// missing Task. A new action consumes it in the same write that claims the Attach.
type retiredDetach struct {
	AttachID   string              `json:"attach_id"`
	TaskID     string              `json:"task_id"`
	WorkItemID string              `json:"work_item_id"`
	HostID     string              `json:"host_id"`
	Input      workexecution.Input `json:"input"`
	RetiredAt  time.Time           `json:"retired_at"`
}

func decodeRetiredDetach(raw []byte) (retiredDetach, error) {
	record, err := recordcodec.Decode[retiredDetach](raw, "retired-detach")
	if err != nil || ids.Validate(ids.KindAttach, record.AttachID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || ids.Validate(ids.KindWorkItem, record.WorkItemID) != nil ||
		ids.Validate(ids.KindHost, record.HostID) != nil || ids.Validate(ids.KindPlan, record.Input.ID) != nil ||
		!recordcodec.ValidSHA256(
			record.Input.SHA256,
		) || record.RetiredAt.IsZero() || record.RetiredAt.Location() != time.UTC {
		return retiredDetach{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

// PrepareDetachClaim must join source-revision comparisons and native Task
// publication. Failed Detach ownership cannot be replaced merely because its
// Task is finished, expired or missing.
func PrepareDetachClaim(
	ctx context.Context,
	reader Reader,
	source Input,
	taskID, hostID string,
) (attachrecord.Record, workqueue.Change, error) {
	key := attachrecord.RetiredDetachKey(source.Attach.ID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: source.SnapshotRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return attachrecord.Record{}, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != source.SnapshotRevision || len(read.Values) != 1 {
		return attachrecord.Record{}, workqueue.Change{}, recordcodec.CorruptRecord()
	}
	value := read.Values[0]
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(value)}}}
	if source.Attach.Status != "failed" || source.Attach.Operation != attachrecord.AttachOperationDetach {
		if value != nil {
			return attachrecord.Record{}, workqueue.Change{}, recordcodec.CorruptRecord()
		}
		next, err := attachrecord.BeginAttachDetaching(source.Attach, taskID)
		return next, change, err
	}
	if value == nil {
		return attachrecord.Record{}, workqueue.Change{}, errs.New(
			errs.KindResourceInUse,
			"retry the original Detach Task; its captured execution has not been retired",
		)
	}
	proof, err := decodeRetiredDetach(value.Value)
	if err != nil || proof.AttachID != source.Attach.ID || proof.TaskID != source.Attach.TaskID ||
		proof.HostID != hostID ||
		proof.TaskID == taskID {
		return attachrecord.Record{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"failed Detach has no matching retired execution authority on this host",
		)
	}
	next, err := attachrecord.RetryAttachOperation(source.Attach, taskID)
	if err != nil {
		return attachrecord.Record{}, workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
	return next, change, nil
}

// prepareRetiredDetach runs under the collecting Task and fully settled item
// comparisons. Native settlement, not elapsed time, proves no command remains
// in flight. It leaves a bounded receipt only while the failed binding exists.
func (repository *InputRepository) prepareRetiredDetach(
	ctx context.Context,
	view workqueue.View,
	input DetachInput,
	at time.Time,
) (workqueue.Change, error) {
	attachID := input.Snapshot.Source.Attach.ID
	keys := []string{attachrecord.AttachKey(attachID), attachrecord.RetiredDetachKey(attachID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	current, receipt := read.Values[0], read.Values[1]
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: keys[0], ModRevision: keyvalue.RevisionOf(current)},
		{Key: keys[1], ModRevision: keyvalue.RevisionOf(receipt)},
	}}
	if current == nil {
		if receipt != nil {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		return change, nil
	}
	binding, err := attachrecord.DecodeAttachRecord(current.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if binding.TaskID != view.Task.Task.ID {
		// Another explicit action already consumed the settled receipt. Its
		// current binding and authority do not belong to this history collector.
		return change, nil
	}
	if !attachrecord.AttachImmutableEqual(binding, input.Snapshot.Source.Attach) || binding.Status != "failed" ||
		binding.Operation != attachrecord.AttachOperationDetach || view.Item.Item.Outcome == workexecution.Succeeded {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Detach history cannot release an unsettled or inconsistent binding",
		)
	}
	if receipt != nil {
		proof, err := decodeRetiredDetach(receipt.Value)
		if err != nil || proof.TaskID != view.Task.Task.ID || proof.WorkItemID != view.Item.Item.ID ||
			proof.AttachID != attachID || proof.HostID != view.Item.Item.Placement.HostID || proof.Input != *view.Item.Item.Input {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		return change, nil
	}
	raw, err := recordcodec.Encode("retired-detach", retiredDetach{AttachID: attachID, TaskID: view.Task.Task.ID,
		WorkItemID: view.Item.Item.ID, HostID: view.Item.Item.Placement.HostID, Input: *view.Item.Item.Input, RetiredAt: at})
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[1], Value: raw}}
	return change, nil
}
