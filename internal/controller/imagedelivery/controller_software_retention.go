package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) retainControllerWorkImages(
	ctx context.Context,
	retention *imageRetention,
	value keyvalue.KeyValue,
	revision int64,
) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "Controller image retention work identity is inconsistent")
	}
	if item.Operation != workexecution.ControllerSoftware || item.Phase == workexecution.Settled {
		return nil
	}
	if item.Input == nil {
		return service.retainActivationInputImages(ctx, retention, item, revision)
	}
	inputs, err := controllersoftware.New(service.store)
	if err != nil {
		return err
	}
	capture, err := inputs.Load(ctx, item, revision)
	if err != nil {
		return err
	}
	reason := "Captured Controller activation " + item.TaskID
	retention.retain(capture.Record.Input.Manifest.AgentImage, reason)
	if capture.Record.Input.Agent != nil {
		retention.retain(capture.Record.Input.Agent.Image, reason)
	}
	return nil
}
