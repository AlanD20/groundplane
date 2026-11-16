package backingchecks

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"backing-check",
			workexecution.ActionCheck,
			ids.KindAttach,
			service.prepareAcceptedCheck,
		),
	}
}

func (service *Service) prepareAcceptedCheck(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	input apiTypes.BackingCheckRequest,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	environmentID := accepted.Task.Scope.EnvironmentID
	selectedRevision := input.Revision
	if selectedRevision == 0 {
		current, err := service.attaches.GetAttach(ctx, input.AttachID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		selectedRevision = current.Revision
	}
	source, err := service.attaches.LoadBackingCheckSource(
		ctx,
		environmentID,
		input.AttachID,
		selectedRevision,
		input.GrantID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer source.Clear()
	adapter, registered := adapters.Get(source.AdapterKey())
	if !registered || adapter.Custom() {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindValidationFailed,
			"access checks support built-in PostgreSQL, MySQL and Valkey Attaches",
		)
	}
	images, err := service.images.ResolveWorkloadImages(
		ctx,
		source.AgentID(),
		[]*agentpb.WorkloadImageSelector{source.ImageSelector()},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	observed, err := service.observations.ObserveServices(
		ctx,
		source.AgentID(),
		[]*agentpb.ServiceObservationTarget{source.ObservationTarget()},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := source.BindHostSelection(images, observed); err != nil {
		return taskdependencies.Prepared{}, err
	}
	checkOwner := checkrecord.Owner{
		TaskID:        accepted.Task.ID,
		WorkItemID:    ids.DeriveAt(ids.KindWorkItem, accepted.Task.CreatedAt, accepted.Task.ID, "backing-check"),
		PlanID:        ids.DeriveAt(ids.KindPlan, accepted.Task.CreatedAt, accepted.Task.ID, "backing-check"),
		OperationID:   ids.DeriveAt(ids.KindOperation, accepted.Task.CreatedAt, accepted.Task.ID, "backing-check"),
		EnvironmentID: environmentID,
	}
	now := at
	facts := source.FactSource()
	defer facts.Clear()
	var check checkrecord.Record
	err = service.facts.ResolveStoredAccessCredentials(
		ctx,
		facts.Consumer,
		facts.Owner,
		facts.Grant,
		facts.Facts.Record,
		adapter,
		func(private backingaccess.Request) error {
			configuration, err := source.Configuration(private)
			if err != nil {
				return err
			}
			check, err = SealInput(ctx, service.protector, checkOwner, configuration, private, now)
			return err
		},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer check.Clear()
	hash, err := checkrecord.PlanHash(check)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task := workexecution.CloneTask(accepted.Task)
	task.Intent, task.ExpectedItems, task.UpdatedAt = workexecution.Input{ID: checkOwner.PlanID, SHA256: hash}, 1, at
	item, authority, err := service.attaches.PrepareBackingCheck(ctx, source, check, task)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	item.TargetName = facts.Consumer.Record.Name
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, time.Minute)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
