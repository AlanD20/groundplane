package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Waiting activation units deliberately have no executable input yet. Their
// complete artifact/predecessor authority is the accepted activation capture;
// absence of a unit input is not missing image protection or permission to
// waive protection for unrelated malformed work.
func (service *Service) retainActivationInputImages(
	ctx context.Context,
	retention *imageRetention,
	item workexecution.Item,
	revision int64,
) error {
	queue, err := workqueue.NewRepository(service.store)
	if err != nil {
		return err
	}
	task, _, err := queue.ReadTask(ctx, item.TaskID, revision)
	if err != nil {
		return err
	}
	if task.Task.Action != workexecution.ActionActivate || task.Task.TargetID != activation.TargetID() {
		return errs.New(errs.KindStateConflict, "unprepared software work has no captured activation owner")
	}
	repository, err := activation.NewRepository(service.store)
	if err != nil {
		return err
	}
	record, err := repository.At(ctx, item.TaskID, revision)
	if err != nil {
		return err
	}
	_, graph, err := record.Record.Graph()
	if err != nil {
		return err
	}
	matched := false
	for _, expected := range graph {
		if expected.ID == item.ID && expected.TaskID == item.TaskID && expected.Operation == item.Operation &&
			expected.TargetID == item.TargetID &&
			expected.Input == nil {
			matched = true
		}
	}
	if !matched {
		return errs.New(errs.KindStateConflict, "unprepared software work differs from its captured activation graph")
	}
	reason := "Captured software activation " + item.TaskID
	retention.software(record.Record.Input.Preparation.Progress.Result, reason)
	retention.retain(record.Record.Input.StagingAgentImage, reason)
	if record.Record.Input.Agent != nil {
		retention.retain(record.Record.Input.Agent.Image, reason)
	}
	return nil
}
