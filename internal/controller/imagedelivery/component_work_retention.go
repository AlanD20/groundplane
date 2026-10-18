package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Read at the inventory's revision so an image selected by a pending Component
// or its dependent Service work cannot disappear between independent reads.
func (service *Service) retainComponentWorkImages(ctx context.Context, retention *imageRetention,
	value keyvalue.KeyValue, revision int64,
) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "image retention work identity differs from its storage key")
	}
	if item.Operation != workexecution.Component {
		return nil
	}
	queue, err := workqueue.NewRepository(service.store)
	if err != nil {
		return err
	}
	parent, _, err := queue.ReadTask(ctx, item.TaskID, revision)
	if err != nil {
		return err
	}
	// Collection is allowed only after effects settle. Failed Apply work cannot
	// retry forward, and a finished parent cannot publish another dependent item.
	if parent.Task.FinishedAt != nil || parent.Task.CollectionStartedAt != nil {
		return nil
	}
	view, err := queue.ReadWork(ctx, item.TaskID, item.ID, revision)
	if err != nil {
		return err
	}
	if item.Placement.Executor == workexecution.Controller {
		selection, found, err := componentplanning.ReadControllerProxySelection(ctx, service.store,
			parent.Task.Scope.EnvironmentID, parent.Task.ID, item.TargetID, revision)
		if err != nil || !found {
			return err
		}
		return service.retainCapturedProxyImages(ctx, retention, selection.Record.Preflight, parent.Task.ID, revision)
	}
	if item.Input == nil {
		return nil // No executable image has been captured yet.
	}
	inputs, err := componentwork.NewInputRepository(service.store)
	if err != nil {
		return err
	}
	input, capture, _, err := inputs.Load(ctx, view)
	if err != nil {
		return err
	}
	reason := "Component work in Task " + parent.Task.ID
	if item.Phase != workexecution.Settled {
		retention.composeArtifact(input.Candidate, reason)
		retention.composeArtifact(input.GetPredecessor().GetRuntime(), reason)
	}
	if capture.Proxy != nil {
		return service.retainCapturedProxyImages(ctx, retention, *capture.Proxy, parent.Task.ID, revision)
	}
	return nil
}
