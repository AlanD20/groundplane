package imageoperations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// RecordTagOutcome consumes an inspected Docker outcome, never a transport-error
// guess. Unknown retains the write fence; applied/unchanged release it atomically
// with the saved result. An applied tag stays readable after Task retention.
func (journal *Journal) RecordTagOutcome(ctx context.Context, outcome imagefetch.TagAssignmentOutcome, at time.Time) error {
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Tagging && !(record.Phase == Uncertain && record.UncertainPhase == Tagging) {
		return conflict()
	}
	selectionKey, writeKey, err := tagKeys(record.Input)
	if err != nil {
		return err
	}
	read, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{selectionKey, writeKey}, Revision: owned.revision})
	if err != nil {
		return err
	}
	if read == nil {
		return conflict()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != owned.revision || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return conflict()
	}
	selection, err := recordcodec.Decode[TagSelection](read.Values[0].Value, "image_tag_selection")
	if err != nil {
		return err
	}
	write, err := recordcodec.Decode[TagSelection](read.Values[1].Value, "image_tag_write")
	if err != nil {
		return err
	}
	input, err := record.Input.Reference()
	if err != nil {
		return err
	}
	expected := TagSelection{TaskID: journal.taskID, WorkItemID: journal.workID, Input: input, ImageID: record.ImageID}
	if selection != expected || write != expected {
		return conflict()
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: selectionKey, ModRevision: read.Values[0].ModRevision}, {Key: writeKey, ModRevision: read.Values[1].ModRevision}}}
	record.UpdatedAt = at
	switch outcome {
	case imagefetch.TagAssignmentUnknown:
		record.Phase, record.UncertainPhase = Uncertain, Tagging
	case imagefetch.TagAssignmentApplied, imagefetch.TagAssignmentUnchanged:
		record.Phase, record.UncertainPhase, record.FinishedAt = Finished, "", &at
		record.Outcome = workexecution.Failed
		if outcome == imagefetch.TagAssignmentApplied {
			record.Outcome = workexecution.Succeeded
			value, err := recordcodec.Encode("image_tag_applied", struct {
				Selection TagSelection `json:"selection"`
				AppliedAt time.Time    `json:"applied_at"`
			}{expected, at})
			if err != nil {
				return err
			}
			defer clear(value)
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
				Key: selectionKey + "/applied", Value: value})
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: writeKey})
	default:
		return invalid()
	}
	return journal.commit(ctx, owned, record, change)
}

// RecordUnknown never grants another Docker call or releases ownership.
func (journal *Journal) RecordUnknown(ctx context.Context, at time.Time) error {
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Fetching && record.Phase != Tagging && record.Phase != Removing && record.Phase != Uncertain {
		return conflict()
	}
	if record.Phase != Uncertain {
		record.UncertainPhase = record.Phase
	}
	record.Phase, record.UpdatedAt = Uncertain, at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}
