package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (service *ConnectionCheckService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"dns-check",
			workexecution.ActionCheck,
			ids.KindComponent,
			service.prepareAcceptedCheck,
		),
	}
}

func (service *ConnectionCheckService) prepareAcceptedCheck(ctx context.Context, task workqueue.TaskSnapshot,
	_ int64, at time.Time, input apiTypes.DNSCheckRequest,
) (taskdependencies.Prepared, error) {
	queries, _, err := dnsCheckQueries(input.Queries)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	selectedRevision := input.Revision
	if selectedRevision == 0 {
		current, err := service.planner.components.GetComponent(ctx, task.Task.TargetID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		selectedRevision = current.Revision
	}
	source, request, err := service.planner.Capture(ctx, task.Task.TargetID, selectedRevision, queries)
	if err != nil {
		if input.Revision == 0 {
			current, readErr := service.planner.components.GetComponent(ctx, task.Task.TargetID)
			if readErr == nil && current.Revision != selectedRevision {
				return taskdependencies.Prepared{Pending: &taskdependencies.Pending{Reason: workexecution.WaitConflict,
					Detail: "CoreDNS configuration changed while its check inputs were being captured; GP will capture the current configuration again."}}, nil
			}
		}
		return taskdependencies.Prepared{}, err
	}
	owner := dnschecks.Owner{TaskID: task.Task.ID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, task.Task.CreatedAt, task.Task.ID, "dns-check"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, task.Task.ID, "dns-check"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.Task.CreatedAt, task.Task.ID, "dns-check")}
	record, err := dnschecks.NewRecord(owner, source.Configuration(), request, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	hash, err := dnschecks.PlanHash(record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared := workexecution.CloneTask(task.Task)
	prepared.Intent, prepared.ExpectedItems, prepared.UpdatedAt = workexecution.Input{
		ID:     owner.PlanID,
		SHA256: hash,
	}, 1, at
	item, authority, err := service.tasks.PrepareDNSCheck(ctx, source, record, prepared)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	item.Deadline = acceptedaction.ExecutionDeadline(prepared, at, 90*time.Second)
	return taskdependencies.Prepared{Task: prepared, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
