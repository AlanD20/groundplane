package operations

import (
	"context"

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

func (service *entryDesiredMutationService) publishConfiguration(ctx context.Context, request entryConfigurationPublication) (etcd.IdempotencyTransactionResult, error) {
	placement := workexecution.Placement{Executor: workexecution.Controller}
	if request.Removal == nil || len(request.Files) != 0 {
		var err error
		placement, err = service.work.ResolveResourceHost(ctx)
		if err != nil {
			return etcd.IdempotencyTransactionResult{}, err
		}
	}
	task, item, err := taskplanning.NewEntryConfigurationWork(workexecution.Scope{
		TenantID: request.Project.Record.TenantID, ProjectID: request.Project.Record.ID,
		EnvironmentID: request.Environment.Record.ID,
	}, placement, request.Claim, request.Action, request.TargetID, request.TargetName)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	if request.Removal != nil && len(request.Files) == 0 {
		return service.work.PublishEntryConfigurationWork(ctx, etcd.EntryWorkPublication{
			Project: request.Project, Environment: request.Environment, Claim: request.Claim,
			Projection: request.Candidate, EpochRevision: request.Runtime.EpochRevision,
			Task: task, Item: item, Capture: entrywork.Capture{Removal: request.Removal}, Marker: request.Marker,
			DefinitionConfigurationRevision: request.ConfigurationRevision,
		})
	}
	operationID := ids.DeriveAt(ids.KindOperation, item.CreatedAt, item.ID, "entry-operation")
	configurationID := ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "configuration")
	prepared, err := service.work.PrepareMutationWorkConfiguration(ctx, task, item, request.Claim,
		request.ReadRevision, configurationID, operationID, request.Files)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	var secrets *tasksecretpins.Binding
	if !prepared.Secrets.IsZero() {
		binding, err := prepared.Secrets.Binding()
		if err != nil {
			return etcd.IdempotencyTransactionResult{}, err
		}
		secrets = &binding
	}
	input, capture, err := request.Runtime.PrepareConfigurationWork(taskplanning.EntryConfigurationRequest{
		Task: task, Item: item, OperationID: operationID, Candidate: request.Candidate,
		Files: request.Files, Configuration: prepared.Capture, Secrets: secrets, Removal: request.Removal,
	})
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	return service.work.PublishEntryConfigurationWork(ctx, etcd.EntryWorkPublication{
		Project: request.Project, Environment: request.Environment, Claim: request.Claim,
		Projection: request.Candidate, EpochRevision: request.Runtime.EpochRevision,
		Task: task, Item: item, InputID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "captured-input"),
		Input: input, Capture: capture, Secrets: prepared.Secrets, Marker: request.Marker,
		PreparationConditions: prepared.Conditions,
	})
}
