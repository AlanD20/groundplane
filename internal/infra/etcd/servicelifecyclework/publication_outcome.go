package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReconcilePreparation preserves pins if publication is present or unreadable.
// If absent, Abandon fences the preparation/root so a delayed activation cannot
// commit concurrently with releasing its memberships.
func (publication *Publication) ReconcilePreparation(ctx context.Context) error {
	if publication == nil || publication.pins == nil {
		return nil
	}
	keys := []string{InputKey(publication.Input.WorkItemID), workqueue.TaskKey(publication.Input.TaskID), workqueue.ItemKey(publication.Input.WorkItemID)}
	read, err := publication.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil {
		return conflict("Service lifecycle publication outcome is unavailable; Secret protections were retained")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return conflict("Service lifecycle publication outcome is incomplete; Secret protections were retained")
	}
	present := 0
	for index, value := range read.Values {
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				return conflict("Service lifecycle publication outcome is inconsistent; Secret protections were retained")
			}
			present++
		}
	}
	if present == len(keys) {
		return nil
	}
	if present != 0 {
		return conflict("Service lifecycle publication records are incomplete; Secret protections were retained")
	}
	return publication.Abandon(ctx)
}
