package services

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerlifecycle "github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceLifecycleHookInputs interface {
	SealBackingHookTaskInputs(
		context.Context,
		string,
		string,
		backinghook.Configuration,
	) (*etcd.BackingHookEncryptedInputs, error)
}

func (service *serviceLifecycleService) prepareAppliedServiceLifecycle(
	ctx context.Context,
	taskType etcd.TaskType,
	current etcd.Versioned[etcd.ServiceRecord],
	tenant *etcd.Versioned[etcd.TenantRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	environment etcd.Versioned[etcd.EnvironmentRecord],
	projection etcd.Versioned[etcd.EnvironmentComposeProjection],
	task etcd.TaskRecord,
) (etcd.TaskRecord, etcd.ServiceLifecycleRenderInput, *etcd.BackingHookEncryptedInputs, error) {
	releaseAuthority, err := controllerlifecycle.CaptureRelease(
		ctx, service.repository, projection, environment.Record.ID, current.Record.Desired.ID,
	)
	if err != nil {
		return etcd.TaskRecord{}, etcd.ServiceLifecycleRenderInput{}, nil, err
	}
	input := etcd.ServiceLifecycleRenderInput{
		PlanID: task.PlanID, ServiceID: current.Record.Desired.ID,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir:       environment.Record.VolumeDir,
		ArtifactID:                releaseAuthority.Current.ArtifactID,
		Projection:                projection.Record,
		AppliedProjectionRevision: projection.Revision,
		Release:                   releaseAuthority,
	}
	if tenant != nil {
		input.TenantID, input.TenantSlug = tenant.Record.ID, tenant.Record.Slug
	}
	var sealed *etcd.BackingHookEncryptedInputs
	definition := serviceLifecycleHookDefinition(taskType, current.Record.Desired.Hooks)
	if definition != nil {
		if current.Record.Desired.Adapter != "custom" {
			return etcd.TaskRecord{}, etcd.ServiceLifecycleRenderInput{}, nil, errs.New(
				errs.KindStateConflict,
				"Service lifecycle hooks require a Custom backing Service",
			)
		}
		input.AdapterKey = current.Record.Desired.Adapter
		input.HookConfiguration = backinghook.CloneConfiguration(current.Record.Desired.Hooks)
		sealed, err = service.hookInputs.SealBackingHookTaskInputs(
			ctx, task.OperationID, project.Record.ID, *input.HookConfiguration,
		)
		if err != nil {
			return etcd.TaskRecord{}, etcd.ServiceLifecycleRenderInput{}, nil, err
		}
		if sealed != nil {
			task, err = etcd.BindBackingHookTaskInputs(task, project.Record.ID, *sealed)
			if err != nil {
				clear(sealed.Ciphertext)
				return etcd.TaskRecord{}, etcd.ServiceLifecycleRenderInput{}, nil, err
			}
		}
		minimumTimeout := int64(definition.TimeoutSeconds) + serviceLifecycleControlTimeoutSeconds
		if task.TimeoutSeconds < minimumTimeout {
			task.TimeoutSeconds = minimumTimeout
		}
	}
	task.Executor = etcd.TaskExecutorAgent
	if task.TimeoutSeconds < serviceLifecycleAgentTimeoutSeconds {
		task.TimeoutSeconds = serviceLifecycleAgentTimeoutSeconds
	}
	stepIDs := []string{ids.New(ids.KindStep)}
	if input.Release.RetainedPrior != nil {
		stepIDs = append(stepIDs, ids.New(ids.KindStep))
	}
	if input.HookConfiguration != nil {
		hookStepID := ids.New(ids.KindStep)
		if taskType == etcd.TaskStart {
			stepIDs = append(stepIDs, hookStepID)
		} else {
			stepIDs = append([]string{hookStepID}, stepIDs...)
		}
	}
	prepared, err := service.plans.PrepareServiceLifecycleHookTask(ctx, task, input, sealed, stepIDs)
	if err != nil {
		if sealed != nil {
			clear(sealed.Ciphertext)
		}
		return etcd.TaskRecord{}, etcd.ServiceLifecycleRenderInput{}, nil, err
	}
	return prepared, input, sealed, nil
}

func serviceLifecycleHookDefinition(
	taskType etcd.TaskType,
	configuration *backinghook.Configuration,
) *backinghook.Definition {
	if configuration == nil {
		return nil
	}
	if taskType == etcd.TaskStart {
		return configuration.AfterStart
	}
	if taskType == etcd.TaskStop || taskType == etcd.TaskDestroy {
		return configuration.BeforeStop
	}
	return nil
}
