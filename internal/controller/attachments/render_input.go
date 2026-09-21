package attachments

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachRuntimeCapture interface {
	CaptureEntryMutationRuntime(
		context.Context,
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	) (taskplanning.EntryMutationRuntime, error)
}

func newAttachMutationTask(
	project hierarchyrecord.ProjectRecord,
	environment hierarchyrecord.EnvironmentRecord,
	taskID string,
	attachID string,
	environmentID string,
	taskType etcd.TaskType,
	renderGeneration uint64,
	stepCount int,
	idempotencyKey string,
	createdAt time.Time,
) (etcd.TaskRecord, string, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindAttach, attachID) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || renderGeneration == 0 ||
		renderGeneration > math.MaxInt32 || stepCount <= 0 {
		return etcd.TaskRecord{}, "", errs.New(errs.KindValidationFailed, "Attach Task input is invalid")
	}
	owner, err := etcd.EnvironmentTaskOwner(project, environment)
	if err != nil || environment.ID != environmentID {
		return etcd.TaskRecord{}, "", errs.New(errs.KindValidationFailed, "attach task owner is invalid")
	}
	steps := make([]etcd.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: int32(renderGeneration), Type: taskType, Target: attachID,
		Params: map[string]string{etcd.TaskMutationEnvironmentParam: environmentID}, Steps: steps,
		TimeoutSeconds: attachMutationTimeout, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, ids.New(ids.KindConfig), nil
}

func attachTaskStepCount(
	adapter adapters.Adapter,
	authentication core.BackingAuthentication,
	grantCount int,
	ownsCredential bool,
	hookBundle bool,
) int {
	if adapter.Custom() {
		if hookBundle {
			return 2
		}
		return 1
	}
	if !ownsCredential || authentication == core.BackingAuthenticationNone {
		return 1
	}
	return grantCount + 2
}

func buildAttachTaskRenderInput(
	scope etcd.AttachCreateScope,
	runtime taskplanning.EntryMutationRuntime,
	record attachrecord.Record,
	attaches []etcdstore.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	artifactID string,
) (etcd.AttachTaskRenderInput, error) {
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 || scope.BackingEnvironment.Revision <= 0 ||
		scope.BackingService.Revision <= 0 {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render scope records must be versioned",
		)
	}
	if runtime.EpochRevision <= 0 ||
		runtime.Projection.EnvironmentID != scope.ComposeProjection.Record.EnvironmentID ||
		runtime.Projection.RevisionID != scope.ComposeProjection.Record.RevisionID ||
		runtime.Projection.RenderGeneration != scope.ComposeProjection.Record.RenderGeneration {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"Attach captured runtime does not match the desired projection",
		)
	}
	if ids.Validate(ids.KindPlan, task.PlanID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil ||
		task.Target != record.ID || task.RenderGeneration <= 0 ||
		uint64(task.RenderGeneration) != scope.ComposeProjection.Record.RenderGeneration {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task identity is invalid",
		)
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID ||
		scope.DesiredHead.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.RevisionID != scope.DesiredHead.Record.RevisionID ||
		scope.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		scope.BackingEnvironment.Record.ProjectID != scope.BackingProject.Record.ID ||
		scope.BackingService.Record.EnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingProjectID != scope.BackingProject.Record.ID ||
		record.BackingEnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		record.BackingNetworkID != scope.BackingService.Record.BackingNetworkID {
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindScopeUnauthorized,
			"Attach render hierarchy is invalid",
		)
	}

	excludedAttachID := ""
	unionRecords := attaches
	switch task.Type {
	case etcd.TaskAttach:
		if record.Status != core.AttachPending || record.Operation != attachrecord.AttachOperationProvision ||
			record.TaskID != task.ID {
			return etcd.AttachTaskRenderInput{}, errs.New(
				errs.KindValidationFailed,
				"Attach create render requires its pending provision record",
			)
		}
		unionRecords = append(
			append([]etcdstore.Versioned[attachrecord.Record](nil), attaches...),
			etcdstore.Versioned[attachrecord.Record]{
				Record: record,
			},
		)
	case etcd.TaskDetach:
		if record.Status != core.AttachDetaching || record.Operation != attachrecord.AttachOperationDetach ||
			record.TaskID != task.ID {
			return etcd.AttachTaskRenderInput{}, errs.New(
				errs.KindValidationFailed,
				"Attach detach render requires its detaching record",
			)
		}
		excludedAttachID = record.ID
	default:
		return etcd.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task type is invalid",
		)
	}

	joins, err := taskplanning.ResolveAttachNetworkJoins(
		record.EnvironmentID,
		scope.ComposeProjection.Record,
		unionRecords,
		excludedAttachID,
	)
	if err != nil {
		return etcd.AttachTaskRenderInput{}, err
	}
	return etcd.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: record.ID,
		AttachName: record.Name,
		TenantID:   scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir:      scope.Environment.Record.VolumeDir,
		BackingServiceID:         record.BackingServiceID,
		BackingProjectID:         record.BackingProjectID,
		AdapterKey:               scope.BackingService.Record.Desired.Adapter,
		Authentication:           scope.BackingService.Record.Desired.Authentication,
		HookConfiguration:        backinghook.CloneConfiguration(scope.BackingService.Record.Desired.Hooks),
		DesiredRevisionID:        scope.DesiredHead.Record.RevisionID,
		ArtifactID:               artifactID,
		RenderGeneration:         scope.ComposeProjection.Record.RenderGeneration,
		EnvironmentEpochRevision: runtime.EpochRevision,
		RuntimeProjection:        runtime.Projection,
		RunningServiceIDs:        slices.Sorted(slices.Values(runtime.RunningServiceIDs)),
		Services:                 attachTaskServiceSnapshots(scope.ComposeProjection.Record.DesiredServices),
		Networks:                 attachTaskOwnedNetworkSnapshots(scope.ComposeProjection.Record.DesiredZones),
		Volumes:                  slices.Clone(scope.ComposeProjection.Record.Volumes),
		VolumeMounts:             slices.Clone(scope.ComposeProjection.Record.VolumeMounts),
		NetworkJoins:             joins,
		ConsumerServiceIDs:       []string{record.ServiceID},
		GrantAttachIDs:           append([]string(nil), record.GrantAttachIDs...),
		ServiceDependencyPlans:   scope.ComposeProjection.Record.ServiceDependencyPlans.Clone(),
	}, nil
}

func attachTaskServiceSnapshots(
	values []servicerecord.EnvironmentServiceProjection,
) []etcd.AttachTaskServiceSnapshot {
	snapshots := make([]etcd.AttachTaskServiceSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskServiceSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}

func attachTaskOwnedNetworkSnapshots(
	values []projectionrecord.EnvironmentZoneProjection,
) []etcd.AttachTaskOwnedNetworkSnapshot {
	snapshots := make([]etcd.AttachTaskOwnedNetworkSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskOwnedNetworkSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}
