package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDNSCheck returns exact resolver authority for atomic promotion of the
// already accepted operator Task. It performs no independent publication.
func (repository *TaskRepository) PrepareDNSCheck(ctx context.Context, source DNSCheckSource,
	input dnschecks.Record, task workexecution.Task,
) (workexecution.Item, workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	if source.readRevision <= 0 || input.Validate() != nil || input.Phase != dnschecks.Ready ||
		task.Validate() != nil || task.Scope != (workexecution.Scope{}) || task.Action != workexecution.ActionCheck ||
		task.Actor != workexecution.OperatorActor || task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		input.Owner.TaskID != task.ID || input.Owner.PlanID != task.Intent.ID || input.Source.ComponentID != task.TargetID ||
		input.CreatedAt.Before(
			task.CreatedAt,
		) || input.CreatedAt.After(task.UpdatedAt) || input.Source != source.configuration {
		return workexecution.Item{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"DNS check publication differs from its captured input",
		)
	}
	digest, err := dnschecks.PlanHash(input)
	if err != nil || digest != task.Intent.SHA256 {
		return workexecution.Item{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"DNS check input or request replay differs from publication",
		)
	}
	request, err := input.Input()
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	expected, err := source.Request(request.Action, request.Queries)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	rebuilt, err := dnschecks.NewRecord(input.Owner, source.configuration, expected, input.CreatedAt)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	if rebuilt.ConfigurationSHA256 != input.ConfigurationSHA256 {
		return workexecution.Item{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"DNS check runtime differs from its repository capture",
		)
	}
	conditions, mutations, err := dnschecks.PreparePublication(input)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	deadline := input.CreatedAt.Add(90 * time.Second)
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.DNSCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{
			{HostID: source.configuration.HostID, ScopeID: task.TargetID},
		}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	conditions = append(conditions, source.conditions...)
	return item, workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
