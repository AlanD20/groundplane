package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) retainGroupReleaseImages(ctx context.Context, retention *imageRetention,
	value keyvalue.KeyValue, revision int64,
) error {
	capture, err := recordcodec.Decode[releasegroups.ExecutionCapture](value.Value, "release_group_execution")
	if err != nil {
		return err
	}
	if capture.Validate() != nil || value.Key != releasegroups.ExecutionCaptureKey(capture.Policy.TaskID) {
		return errs.New(errs.KindStateConflict, "Group image capture has inconsistent ownership")
	}
	queue, err := workqueue.NewRepository(service.store)
	if err != nil {
		return err
	}
	task, _, err := queue.ReadTask(ctx, capture.Policy.TaskID, revision)
	if err != nil {
		return err
	}
	if task.Task.FinishedAt != nil {
		// Applied Releases and retained executable inputs independently protect
		// their images; a finished Group cannot prepare any further members.
		return nil
	}
	groups, err := releasegroups.NewExecutionRepository(service.store)
	if err != nil {
		return err
	}
	root, err := groups.Load(ctx, task, revision)
	if err != nil {
		return err
	}
	for index := range capture.Policy.Members {
		member, err := service.releases.ReadCapturedGroupMember(ctx, task, root, index, revision)
		if err != nil {
			return err
		}
		err = retainServiceRolloutImages(retention, member.Rollout, "Captured Group in Task "+task.Task.ID)
		clear(member.Rollout.CandidateArtifact)
		if err != nil {
			return err
		}
	}
	return service.retainGroupPreflightImages(ctx, retention, queue, task, root, revision)
}
