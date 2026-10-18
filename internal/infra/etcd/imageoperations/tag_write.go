package imageoperations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// BeginTagWrite durably reserves the selected tag before Docker is called.
// Neither a newer fetch nor another attempt can replace an unresolved write.
func (journal *Journal) BeginTagWrite(ctx context.Context, at time.Time) error {
	owned, err := journal.read(ctx, true)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Fetching || record.ImageID == "" {
		return conflict()
	}
	selectionKey, writeKey, err := tagKeys(record.Input)
	if err != nil {
		return err
	}
	if selectionKey == "" {
		return invalid()
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
	if read.ReadRevision != owned.revision || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] != nil {
		return conflict()
	}
	selection, err := recordcodec.Decode[TagSelection](read.Values[0].Value, "image_tag_selection")
	if err != nil {
		return err
	}
	input, err := record.Input.Reference()
	if err != nil {
		return err
	}
	expected := TagSelection{TaskID: journal.taskID, WorkItemID: journal.workID, Input: input, ImageID: record.ImageID}
	if selection != expected {
		return conflict()
	}
	value, err := recordcodec.Encode("image_tag_write", expected)
	if err != nil {
		return err
	}
	defer clear(value)
	record.Phase, record.UpdatedAt = Tagging, at
	return journal.commit(ctx, owned, record, workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: selectionKey, ModRevision: read.Values[0].ModRevision}, {Key: writeKey}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: writeKey, Value: value}},
	})
}
