package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func retainRunnerRuntimeImages(retention *imageRetention, read func(string, func(keyvalue.KeyValue) error) error) error {
	if err := read(runnerwork.AllocationPlanKey(""), func(value keyvalue.KeyValue) error {
		plan, err := runnerwork.DecodeAllocationPlan(value.Value)
		if err != nil {
			return err
		}
		if value.Key != runnerwork.AllocationPlanKey(plan.RunnerID) {
			return errs.New(errs.KindInternal, "Runner allocation plan key differs from its owner")
		}
		retention.retain(plan.Container.ImageRef, "Reserved Runner runtime "+plan.RunnerID)
		return nil
	}); err != nil {
		return err
	}
	return read(runnerwork.AppliedKey(""), func(value keyvalue.KeyValue) error {
		applied, err := runnerwork.DecodeApplied(value.Value)
		if err != nil {
			return err
		}
		if value.Key != runnerwork.AppliedKey(applied.Plan.RunnerID) {
			return errs.New(errs.KindInternal, "Runner applied runtime key differs from its owner")
		}
		reason := "Applied Runner runtime " + applied.Plan.RunnerID
		retention.retain(applied.Evidence.Software.Image, reason)
		retention.retain(applied.Evidence.Software.ConfigDigest, reason)
		return nil
	})
}

func (service *Service) retainRunnerWorkImages(ctx context.Context, retention *imageRetention, value keyvalue.KeyValue, revision int64) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindInternal, "Runner image protection work key differs from its owner")
	}
	if item.Operation != workexecution.Runner || item.Phase == workexecution.Settled {
		return nil
	}
	inputs, err := runnerwork.NewRepository(service.store)
	if err != nil {
		return err
	}
	input, err := inputs.Load(ctx, item.ID, revision)
	if err != nil {
		return err
	}
	if err := input.Record.MatchWork(item); err != nil {
		return err
	}
	retention.retain(input.Record.Plan.Container.ImageRef, "Runner work in Task "+item.TaskID)
	return nil
}
