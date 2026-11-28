package gitreleasebridge

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Task(ctx context.Context, input gitdelivery.TaskInput) (gitdelivery.TaskResult, error) {
	image, err := verifiedImage(input.Target, input.RequestID, input.ImageRepository, input.Digest)
	if err != nil {
		return gitdelivery.TaskResult{}, err
	}
	snapshot, revision, err := service.queue.ReadTask(ctx, input.TaskID, 0)
	if err != nil {
		return gitdelivery.TaskResult{}, err
	}
	task := snapshot.Task
	result := gitdelivery.TaskResult{State: taskState(task)}
	switch input.Kind {
	case gitdelivery.TaskFetch:
		if err := service.validateFetch(ctx, task, revision, input.Target, image); err != nil {
			return gitdelivery.TaskResult{}, err
		}
	case gitdelivery.TaskDeploy:
		if task.Action != workexecution.ActionDeploy || task.Scope != targetScope(input.Target) ||
			task.TargetID != input.Target.ServiceID || task.AcceptedIntent == nil ||
			len(task.After) != 1 || task.After[0] != input.FetchTaskID {
			return gitdelivery.TaskResult{}, errs.New(
				errs.KindStateConflict,
				"Native Deploy Task does not belong to the Git deployment",
			)
		}
		if result.State == gitdelivery.TaskSucceeded {
			result.ReleaseID, err = service.completedRelease(ctx, input, snapshot, revision, image)
			if err != nil {
				return gitdelivery.TaskResult{}, err
			}
		}
	default:
		return gitdelivery.TaskResult{}, errs.New(errs.KindValidationFailed, "Git delivery native Task kind is invalid")
	}
	return result, nil
}

func (service *Service) validateFetch(ctx context.Context, task workexecution.Task, revision int64,
	target gitdelivery.Target, image string,
) error {
	if task.Action != workexecution.ActionFetch || task.Scope != targetScope(target) ||
		task.TargetName != image || task.AcceptedIntent == nil || len(task.After) != 0 {
		return errs.New(errs.KindStateConflict, "Native Fetch Task does not match the verified Git image")
	}
	host, err := hostbindings.ReadLocalIdentity(ctx, service.store, revision)
	if err != nil {
		return err
	}
	if host.Revision == 0 || host.Record != task.TargetID {
		return errs.New(errs.KindStateConflict, "Native Fetch Task does not belong to the application host")
	}
	return nil
}

func (service *Service) completedRelease(ctx context.Context, input gitdelivery.TaskInput,
	snapshot workqueue.TaskSnapshot, revision int64, image string,
) (string, error) {
	preparation, _, err := service.ledger.ReadDirectReleaseTaskPreparation(ctx, snapshot, revision)
	if err != nil {
		return "", err
	}
	intent := preparation.Intent
	if intent.OperationKind != release.OperationDeploy || intent.GroupOperationID != "" ||
		intent.OriginatingTaskID != snapshot.Task.ID || intent.ServiceID != input.Target.ServiceID ||
		intent.EnvironmentID != input.Target.EnvironmentID || intent.Workspace.TenantID != input.Target.TenantID ||
		intent.Workspace.ProjectID != input.Target.ProjectID || intent.CandidateWorkload.RequestedReference != image {
		return "", errs.New(errs.KindStateConflict, "Native Release does not match the verified Git deployment")
	}
	if err := service.ledger.VerifySuccessfulRelease(ctx, intent.EnvironmentID, intent.ServiceID, intent, revision); err != nil {
		return "", err
	}
	return intent.ID, nil
}

func taskState(task workexecution.Task) gitdelivery.TaskState {
	if task.FinishedAt != nil {
		if task.Outcome == workexecution.Succeeded && task.PublicationComplete {
			return gitdelivery.TaskSucceeded
		}
		return gitdelivery.TaskFailed
	}
	if task.Progress.Attention > 0 {
		return gitdelivery.TaskAttention
	}
	return gitdelivery.TaskPending
}
