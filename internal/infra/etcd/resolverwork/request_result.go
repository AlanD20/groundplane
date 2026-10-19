package resolverwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// The latest requested target retains its forward outcome independently of
// execution history. Cleanup never changes that outcome into success. An older
// action completing after new intent cannot overwrite the newer request.
func (repository *Repository) prepareRequestResult(
	ctx context.Context,
	view workqueue.View,
	outcome workexecution.Outcome,
) (workqueue.Change, error) {
	item := view.Item.Item
	stored, err := repository.ReadRequest(ctx, item.Placement.HostID, item.TargetID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if stored.Revision == 0 {
		return workqueue.Change{}, invalid("CoreDNS result lost its resource request")
	}
	if stored.Record.WorkID != item.ID {
		return workqueue.Change{}, nil
	}
	if stored.Record.TaskID != item.TaskID {
		return workqueue.Change{}, invalid("CoreDNS result belongs to a different request")
	}
	request := stored.Record
	request.Outcome = outcome
	if err := request.validate(); err != nil {
		return workqueue.Change{}, err
	}
	key := RequestKey(item.Placement.HostID, item.TargetID)
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: stored.Revision}}}
	if stored.Record.Outcome == outcome {
		return change, nil
	}
	value, err := recordcodec.Encode("resolver_request", request)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}
	return change, nil
}
