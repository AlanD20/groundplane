package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) prepareMetadataPublication(ctx context.Context, view workqueue.View,
	capture Capture, applied componentrecord.Record,
) (workqueue.Change, error) {
	key := componentrecord.RecordKey(view.Item.Item.TargetID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return workqueue.Change{}, conflict("Component metadata disappeared before completion")
	}
	current, err := componentrecord.DecodeRecord(value.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	expected := componentrecord.CloneRecord(capture.Current)
	expected.Runtime.Healthy = current.Runtime.Healthy
	if !componentrecord.EqualRecord(current, expected) {
		return workqueue.Change{}, conflict("Component metadata changed after execution input was captured")
	}
	return componentplanning.PrepareComponentMetadataPublication(ctx, repository.store,
		view.Task.Task.Scope.ProjectID, keyvalue.Versioned[componentrecord.Record]{Record: current,
			Revision: value.ModRevision, ReadRevision: view.ReadRevision}, applied)
}
