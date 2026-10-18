package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// CascadeSourceChange returns an actual contradictory revision, not an error
// classification. Transport failures and unreadable captures remain errors.
// The returned comparisons must join rejection publication so a concurrent
// consumer publication cannot turn an old observation into a new failure.
func (repository *InputRepository) CascadeSourceChange(ctx context.Context, view workqueue.View) (bool, workqueue.Change, error) {
	capture, err := repository.LoadPreparedCascadeCapture(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer capture.Clear()
	if view.Task.Task.PublicationComplete || view.Item.Revision != 0 || capture.Input.Cascade == nil ||
		capture.Input.Cascade.ZoneID != view.Task.Task.TargetID {
		return false, workqueue.Change{}, checkpointConflict("source comparison requires an unpublished cascade consumer")
	}
	source := capture.Input.Snapshot.Source
	expected := capture.Input.Snapshot.ConsumerConditions()
	expected = append(expected,
		keyvalue.Condition{Key: attachments.AttachKey(source.Attach.ID), ModRevision: source.AttachRevision},
		keyvalue.Condition{Key: attachments.AttachFactsKey(source.Attach.ID), ModRevision: source.FactsRevision})
	for _, pin := range capture.Capture.Sources {
		expected = append(expected, keyvalue.Condition{Key: secrets.RecordKey(pin.SecretID), ModRevision: pin.MetadataRevision})
	}
	for offset := 0; offset < len(expected); offset += 32 {
		batch := expected[offset:min(offset+32, len(expected))]
		keys := make([]string, len(batch))
		for index, condition := range batch {
			keys[index] = condition.Key
		}
		read, err := repository.read(ctx, keys, view.ReadRevision)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		for index, condition := range batch {
			actual := keyvalue.RevisionOf(read.Values[index])
			if actual != condition.ModRevision {
				keyvalue.ClearValues(read.Values)
				guards := append(capture.Conditions,
					keyvalue.Condition{Key: condition.Key, ModRevision: actual},
					keyvalue.Condition{Key: workqueue.ItemKey(view.Item.Item.ID)},
					keyvalue.Condition{Key: WorkCaptureKey(view.Item.Item.ID)})
				return true, workqueue.Change{Conditions: guards}, nil
			}
		}
		keyvalue.ClearValues(read.Values)
	}
	return false, workqueue.Change{}, nil
}
