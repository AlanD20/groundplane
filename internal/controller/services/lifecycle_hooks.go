package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	controllerlifecycle "github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceLifecycleHookInputs interface {
	SealBackingHookTaskInputs(
		context.Context,
		string,
		string,
		backinghook.Configuration,
	) (*taskconfiguration.BackingHookEncryptedInputs, error)
}

func (service *serviceLifecycleService) prepareAppliedServiceLifecycle(
	ctx context.Context,
	action workexecution.Action,
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	nativeBacking bool,
	planID, operationID string,
) (releaserender.ServiceLifecycleRenderInput, *taskconfiguration.BackingHookEncryptedInputs, error) {
	input := releaserender.ServiceLifecycleRenderInput{
		PlanID: planID, ServiceID: current.Record.Desired.ID,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir,
	}
	if nativeBacking {
		artifact, _, err := projectionrecord.SelectBackingRuntime(projection.Record, current.Record.Desired.ID)
		if err != nil {
			return releaserender.ServiceLifecycleRenderInput{}, nil, err
		}
		native := projectionrecord.CloneEnvironmentComposeProjection(projection.Record)
		input.NativeBacking, input.ArtifactID = &native, artifact.ArtifactId
		input.NativeBackingRevision = projection.Revision
	} else {
		releaseAuthority, err := controllerlifecycle.CaptureRelease(
			ctx, service.repository, projection, environment.Record.ID, current.Record.Desired.ID,
		)
		if err != nil {
			return releaserender.ServiceLifecycleRenderInput{}, nil, err
		}
		runtime, err := service.repository.GetServiceRemovalRuntime(
			ctx, environment.Record.ID, current.Record.Desired.ID, projection.ReadRevision,
		)
		if err != nil {
			return releaserender.ServiceLifecycleRenderInput{}, nil, err
		}
		if runtime.ReadRevision != projection.ReadRevision || runtime.Revision <= 0 || runtime.Revision > projection.ReadRevision {
			return releaserender.ServiceLifecycleRenderInput{}, nil, errs.New(
				errs.KindStateConflict, "Service lifecycle runtime snapshot changed",
			)
		}
		input.ArtifactID, input.Release = releaseAuthority.Current.ArtifactID, &releaseAuthority
		input.AcknowledgedRuntime, input.AcknowledgedRuntimeRevision = &runtime.Record, runtime.Revision
	}
	if tenant != nil {
		input.TenantID, input.TenantSlug = tenant.Record.ID, tenant.Record.Slug
	}
	var sealed *taskconfiguration.BackingHookEncryptedInputs
	var err error
	definition := serviceLifecycleHookDefinition(action, current.Record.Desired.Hooks)
	if definition != nil {
		if current.Record.Desired.Adapter != "custom" {
			return releaserender.ServiceLifecycleRenderInput{}, nil, errs.New(
				errs.KindStateConflict,
				"Service lifecycle hooks require a Custom backing Service",
			)
		}
		input.AdapterKey = current.Record.Desired.Adapter
		input.HookConfiguration = backinghook.CloneConfiguration(current.Record.Desired.Hooks)
		sealed, err = service.hookInputs.SealBackingHookTaskInputs(
			ctx, operationID, project.Record.ID, *input.HookConfiguration,
		)
		if err != nil {
			return releaserender.ServiceLifecycleRenderInput{}, nil, err
		}
	}
	return input, sealed, nil
}

func serviceLifecycleHookDefinition(
	action workexecution.Action,
	configuration *backinghook.Configuration,
) *backinghook.Definition {
	if configuration == nil {
		return nil
	}
	if action == workexecution.ActionStart {
		return configuration.AfterStart
	}
	if action == workexecution.ActionStop || action == workexecution.ActionDestroy {
		return configuration.BeforeStop
	}
	return nil
}
