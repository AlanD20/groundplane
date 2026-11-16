package etcdconfig

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type applyRequest struct {
	ExpectedRevision string `json:"expected_revision,omitempty"`
}

func (s *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(s.submitter, "etcd-config-apply",
		workexecution.ActionApply, ids.KindComponent, s.prepareAcceptedApply)}
}

func (s *Service) prepareAcceptedApply(ctx context.Context, accepted workqueue.TaskSnapshot, _ int64,
	at time.Time, request applyRequest,
) (taskdependencies.Prepared, error) {
	candidate, revision, _, err := s.document.Current(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if request.ExpectedRevision != "" && revision != request.ExpectedRevision {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"etcd config changed; reload before applying",
		)
	}
	previous, err := s.runtime.AppliedConfiguration(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	for _, document := range []string{candidate, previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(document)); err != nil {
			return taskdependencies.Prepared{}, errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	task := workexecution.CloneTask(accepted.Task)
	input := etcdconfiguration.Input{TaskID: task.ID,
		WorkItemID:       ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "etcd-config-apply"),
		PlanID:           ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "etcd-config-apply"),
		SelectedRevision: revision, Candidate: candidate, Previous: previous, CreatedAt: at}
	captured, item, err := input.Work()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, 1, at
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, etcdconfiguration.Timeout)
	authority, err := s.tasks.PrepareEtcdConfigurationAuthority(ctx, input)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
