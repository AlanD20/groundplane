package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Finish checks durable publication before abandoning prepared pins. A timed
// out transaction may have committed; neither its error nor an idempotent replay
// permits releasing the original work's captured Secret values.
func (publication *InputPublication) Finish(ctx context.Context) error {
	if publication == nil || publication.store == nil {
		return checkpointConflict("Attach publication outcome has no captured owner")
	}
	capture := publication.Generation
	keys := []string{workqueue.TaskKey(capture.ParentTaskID), workqueue.ItemKey(capture.WorkItemID),
		blueprintattachinputs.Key(capture.ParentTaskID, capture.AttachID)}
	read, err := publication.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return checkpointConflict("Attach publication outcome is unavailable; captured pins remain retained")
	}
	present := 0
	for index, value := range read.Values {
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				return checkpointConflict("Attach publication outcome contains invalid storage evidence")
			}
			present++
		}
	}
	if present == 0 {
		return publication.Abandon(ctx)
	}
	if present != len(keys) {
		return checkpointConflict("Attach publication is partially visible; captured pins remain retained")
	}
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil {
		return err
	}
	item, err := workqueue.DecodeItem(read.Values[1].Value)
	if err != nil {
		return err
	}
	stored, err := blueprintattachinputs.Decode(read.Values[2].Value)
	if err != nil {
		return err
	}
	defer blueprintattachinputs.Clear(&stored)
	expected, err := blueprintattachinputs.WorkInput(capture)
	if err != nil {
		return err
	}
	actual, err := blueprintattachinputs.WorkInput(stored)
	if err != nil {
		return err
	}
	if !task.PublicationComplete || !stored.MatchesWork(task, item) || expected != actual || item.Input == nil || *item.Input != expected {
		return checkpointConflict("Attach publication differs from the submitted capture; pins remain retained")
	}
	return nil
}
