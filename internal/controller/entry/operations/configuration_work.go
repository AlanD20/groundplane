package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

type entryConfigurationPublication struct {
	Accepted              workqueue.TaskSnapshot
	At                    time.Time
	Project               keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment           keyvalue.Versioned[hierarchy.EnvironmentRecord]
	Claim                 blueprints.EnvironmentBlueprintStageClaim
	Runtime               taskplanning.EntryMutationRuntime
	Candidate             environmentprojection.EnvironmentComposeProjection
	ReadRevision          int64
	Files                 []taskmaterialization.Record
	Marker                idempotency.IdempotencyMarker
	Action                workexecution.Action
	TargetID              string
	TargetName            string
	Removal               *entrywork.Removal
	ConfigurationRevision int64
}

func (service *entryDesiredMutationService) prepareConfiguration(
	ctx context.Context,
	request entryConfigurationPublication,
) (taskdependencies.Prepared, error) {
	placement := workexecution.Placement{Executor: workexecution.Controller}
	if request.Removal == nil || len(request.Files) != 0 {
		var err error
		placement, err = service.work.ResolveResourceHost(ctx)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	task, item, err := taskplanning.NewEntryConfigurationWork(workexecution.Scope{
		TenantID: request.Project.Record.TenantID, ProjectID: request.Project.Record.ID,
		EnvironmentID: request.Environment.Record.ID,
	}, placement, request.Claim, request.Action, request.TargetID, request.TargetName, request.Accepted, request.At)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if request.Removal != nil && len(request.Files) == 0 {
		capture, err := service.work.PrepareEntryConfigurationWork(ctx, etcd.EntryWorkPublication{
			Project: request.Project, Environment: request.Environment, Claim: request.Claim,
			Projection: request.Candidate, EpochRevision: request.Runtime.EpochRevision,
			Task: task, Item: item, Accepted: request.Accepted, Capture: entrywork.Capture{Removal: request.Removal}, Marker: request.Marker,
			DefinitionConfigurationRevision: request.ConfigurationRevision,
		})
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		return entryCapturePrepared(capture), nil
	}
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, item.ID, "entry-operation")
	configurationID := ids.DeriveAt(ids.KindConfig, task.CreatedAt, item.ID, "configuration")
	prepared, err := service.work.PrepareMutationWorkConfiguration(ctx, task, item, request.Claim,
		request.ReadRevision, configurationID, operationID, request.Files)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	keepCapture := false
	defer func() {
		if !keepCapture {
			finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = service.work.AbandonEntryWorkPreparation(finalCtx, task, item, prepared.Secrets.OperationID())
		}
	}()
	var secrets *tasksecretpins.Binding
	if !prepared.Secrets.IsZero() {
		binding, err := prepared.Secrets.Binding()
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		secrets = &binding
	}
	input, capture, err := request.Runtime.PrepareConfigurationWork(taskplanning.EntryConfigurationRequest{
		Task: task, Item: item, OperationID: operationID, Candidate: request.Candidate,
		Files: request.Files, Configuration: prepared.Capture, Secrets: secrets, Removal: request.Removal,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	publication, err := service.work.PrepareEntryConfigurationWork(ctx, etcd.EntryWorkPublication{
		Project: request.Project, Environment: request.Environment, Claim: request.Claim,
		Projection: request.Candidate, EpochRevision: request.Runtime.EpochRevision,
		Task: task, Item: item, Accepted: request.Accepted, InputID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "captured-input"),
		Input: input, Capture: capture, Secrets: prepared.Secrets, Marker: request.Marker,
		PreparationConditions: prepared.Conditions,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	keepCapture = true
	return entryCapturePrepared(publication), nil
}

func entryCapturePrepared(capture etcd.EntryWorkCapture) taskdependencies.Prepared {
	return taskdependencies.Prepared{
		Task:      capture.Task,
		Graph:     []workexecution.Item{capture.Item},
		Authority: capture.Change,
		Decision:  capture.Decision,
		Finalize:  capture.Finish,
	}
}
