package services

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *serviceLifecycleService) AcceptedPreparations() []taskdependencies.Registration {
	var registrations []taskdependencies.Registration
	for _, action := range []workexecution.Action{workexecution.ActionStart, workexecution.ActionStop, workexecution.ActionDestroy} {
		registrations = append(registrations, acceptedaction.Registration(service.accepted,
			"service-"+string(action), action, ids.KindService, service.prepareAcceptedLifecycle))
	}
	return registrations
}

func (service *serviceLifecycleService) prepareAcceptedLifecycle(ctx context.Context,
	accepted workqueue.TaskSnapshot, _ int64, at time.Time, _ apiTypes.TaskRunAfterRequest,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	current, err := service.repository.GetService(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Service ownership changed after this Task was accepted",
		)
	}
	var tenant *keyvalue.Versioned[hierarchyrecord.TenantRecord]
	if project.Record.Kind == hierarchyrecord.ProjectKindTenant {
		loaded, err := service.repository.GetTenant(ctx, project.Record.TenantID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		tenant = &loaded
	} else if project.Record.Kind != hierarchyrecord.ProjectKindBacking {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Service Project kind is invalid")
	}
	projection, hasProjection, err := service.repository.GetEnvironmentAppliedComposeProjection(
		ctx,
		environment.Record.ID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	runtime, err := service.repository.GetServiceRemovalRuntime(
		ctx,
		environment.Record.ID,
		task.TargetID,
		projection.ReadRevision,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	nativeBacking := project.Record.Kind == hierarchyrecord.ProjectKindBacking && runtime.Revision == 0 && hasProjection
	if nativeBacking {
		if _, _, err := projectionrecord.SelectBackingRuntime(projection.Record, task.TargetID); err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	capture := servicelifecyclework.Input{TaskID: task.ID,
		WorkItemID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "service-lifecycle-work"),
		PlanID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "service-lifecycle-plan"),
		Action:     task.Action, ServiceID: task.TargetID, ServiceRevision: current.Revision,
		Scope: task.Scope, CreatedAt: at}
	timeout := serviceLifecycleControlTimeoutSeconds
	var envelope *taskconfiguration.BackingHookEncryptedInputs
	defer func() {
		if envelope != nil {
			clear(envelope.Ciphertext)
		}
	}()
	if runtime.Revision > 0 || nativeBacking {
		input, sealed, err := service.prepareAppliedServiceLifecycle(ctx, task.Action, current, tenant,
			project, environment, projection, nativeBacking, capture.PlanID,
			ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "service-lifecycle-hook"))
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		envelope, capture.Runtime = sealed, &input
		placement, err := service.repository.ResolveLifecycleHost(ctx)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		capture.HostID = placement.HostID
		timeout = serviceLifecycleAgentTimeoutSeconds
		if hook := serviceLifecycleHookDefinition(task.Action, input.HookConfiguration); hook != nil {
			timeout = max(timeout, int64(hook.TimeoutSeconds)+serviceLifecycleControlTimeoutSeconds)
		}
		if sealed != nil {
			capture.HookInputs = &servicelifecyclework.HookInputs{OperationID: sealed.OperationID,
				CiphertextSHA256: sealed.CiphertextSHA256, SecretSources: sealed.SecretSources}
		}
	}
	publication, err := service.repository.PrepareServiceLifecycle(ctx, tenant, project, environment, current,
		capture, envelope, acceptedaction.ExecutionDeadline(task, at, time.Duration(timeout)*time.Second), accepted)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, err = publication.Input.Reference()
	if err != nil {
		return taskdependencies.Prepared{Finalize: publication.Finish, Authority: publication.Change}, err
	}
	task.ExpectedItems, task.UpdatedAt = 1, at
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{publication.Item},
		Authority: publication.Change, Finalize: publication.Finish}, nil
}
