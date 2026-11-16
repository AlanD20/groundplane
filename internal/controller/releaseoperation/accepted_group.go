package releaseoperation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareAcceptedGroupDeploy(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	_ time.Time,
	request domain.GroupDeployInput,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	scope, err := service.ledger.LoadPlanningScope(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	group, err := service.groups.GetAtRevision(ctx, task.Task.TargetID, scope.ReadRevision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	tag := request.Tag
	if tag == "" {
		tag = group.Group.DefaultTag
	}
	if tag == "" {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindReleaseGroupTagRequired,
			"release group deploy requires a tag or group default",
		)
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, group.Group.Order)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidates := make([]releaseCandidateInput, len(planning))
	for index := range planning {
		candidates[index], err = service.deployCandidate(
			ctx,
			scope,
			planning[index],
			tag,
			"",
			domain.OnFailure(group.Group.OnFailure),
			"",
		)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	return service.prepareGroupWork(ctx, scope, group, candidates, task)
}

func (service *Service) prepareAcceptedGroupRollback(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	_ time.Time,
	request domain.GroupRollbackInput,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	selection, err := service.selectGroupRollback(
		ctx,
		task.Task.Scope.EnvironmentID,
		task.Task.TargetID,
		request.Tag,
		request.PreviewRevision,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return service.prepareGroupWork(ctx, selection.scope, selection.group, selection.candidates, task)
}
