package attachments

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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

// AuthoredDetachRenderScope carries only the exact predecessor hierarchy and
// applied projection. The new desired head is intentionally not a render
// source for omission cleanup.
type AuthoredDetachRenderScope struct {
	Tenant             etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project            etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment        etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	AppliedProjection  etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	BackingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService     etcdstore.Versioned[servicerecord.ServiceRecord]
}

func newAttachMutationTask(
	project hierarchyrecord.ProjectRecord,
	environment hierarchyrecord.EnvironmentRecord,
	taskID string,
	attachID string,
	environmentID string,
	taskType taskjournal.TaskType,
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
	owner, err := taskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil || environment.ID != environmentID {
		return etcd.TaskRecord{}, "", errs.New(errs.KindValidationFailed, "attach task owner is invalid")
	}
	steps := make([]taskjournal.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: int32(renderGeneration), Type: taskType, Target: attachID,
		Params: map[string]string{taskjournal.TaskMutationEnvironmentParam: environmentID}, Steps: steps,
		TimeoutSeconds: attachMutationTimeout, Status: taskjournal.TaskStatusPending,
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

// AuthoredDetachStepCount returns the ordinary native Detach step shape for a
// captured source. The caller still seals the complete plan and hash.
func AuthoredDetachStepCount(
	adapter adapters.Adapter,
	authentication core.BackingAuthentication,
	record attachrecord.Record,
	detachHook bool,
) int {
	return attachTaskStepCount(adapter, authentication, len(record.GrantAttachIDs), record.OwnsCredential(), detachHook)
}

func buildAttachTaskRenderInput(
	scope etcd.AttachCreateScope,
	runtime taskplanning.EntryMutationRuntime,
	record attachrecord.Record,
	attaches []etcdstore.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	artifactID string,
) (attachrender.AttachTaskRenderInput, error) {
	if scope.DesiredHead.Revision <= 0 || scope.DesiredHead.Record.EnvironmentID != record.EnvironmentID ||
		scope.ComposeProjection.Record.RevisionID != scope.DesiredHead.Record.RevisionID {
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindScopeUnauthorized,
			"Attach render desired head is invalid",
		)
	}
	return buildAttachTaskRenderInputAtRevision(
		scope.Tenant, scope.Project, scope.Environment, scope.ComposeProjection,
		scope.BackingProject, scope.BackingEnvironment, scope.BackingService,
		runtime, record, attaches, task, artifactID, scope.DesiredHead.Record.RevisionID,
	)
}

// BuildAuthoredDetachTaskRenderInput derives cleanup solely from the exact
// applied predecessor. It cannot substitute the newer authored desired head.
func BuildAuthoredDetachTaskRenderInput(
	scope AuthoredDetachRenderScope,
	runtime taskplanning.EntryMutationRuntime,
	record attachrecord.Record,
	attaches []etcdstore.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	artifactID string,
) (attachrender.AttachTaskRenderInput, error) {
	return buildAttachTaskRenderInputAtRevision(
		scope.Tenant, scope.Project, scope.Environment, scope.AppliedProjection,
		scope.BackingProject, scope.BackingEnvironment, scope.BackingService,
		runtime, record, attaches, task, artifactID, scope.AppliedProjection.Record.RevisionID,
	)
}

func buildAttachTaskRenderInputAtRevision(
	tenant etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	backingProject etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	backingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	backingService etcdstore.Versioned[servicerecord.ServiceRecord],
	runtime taskplanning.EntryMutationRuntime,
	record attachrecord.Record,
	attaches []etcdstore.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	artifactID string,
	desiredRevisionID string,
) (attachrender.AttachTaskRenderInput, error) {
	if tenant.Revision <= 0 || project.Revision <= 0 || environment.Revision <= 0 ||
		projection.Revision <= 0 || backingProject.Revision <= 0 || backingEnvironment.Revision <= 0 ||
		backingService.Revision <= 0 || desiredRevisionID != projection.Record.RevisionID {
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render scope records must be versioned",
		)
	}
	if runtime.EpochRevision <= 0 ||
		runtime.Projection.EnvironmentID != projection.Record.EnvironmentID ||
		runtime.Projection.RevisionID != projection.Record.RevisionID ||
		runtime.Projection.RenderGeneration != projection.Record.RenderGeneration {
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"Attach captured runtime does not match the desired projection",
		)
	}
	if ids.Validate(ids.KindPlan, task.PlanID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil ||
		task.Target != record.ID || task.RenderGeneration <= 0 ||
		uint64(task.RenderGeneration) != projection.Record.RenderGeneration {
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task identity is invalid",
		)
	}
	if tenant.Record.ID != project.Record.TenantID || project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProjectID != project.Record.ID || record.EnvironmentID != environment.Record.ID ||
		projection.Record.EnvironmentID != record.EnvironmentID ||
		backingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		backingEnvironment.Record.ProjectID != backingProject.Record.ID ||
		backingService.Record.EnvironmentID != backingEnvironment.Record.ID ||
		record.BackingProjectID != backingProject.Record.ID ||
		record.BackingEnvironmentID != backingEnvironment.Record.ID ||
		record.BackingServiceID != backingService.Record.Desired.ID ||
		record.BackingNetworkID != backingService.Record.BackingNetworkID {
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindScopeUnauthorized,
			"Attach render hierarchy is invalid",
		)
	}

	excludedAttachID := ""
	unionRecords := attaches
	switch task.Type {
	case taskjournal.TaskAttach:
		if record.Status != core.AttachPending || record.Operation != attachrecord.AttachOperationProvision ||
			record.TaskID != task.ID {
			return attachrender.AttachTaskRenderInput{}, errs.New(
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
	case taskjournal.TaskDetach:
		if record.Status != core.AttachDetaching || record.Operation != attachrecord.AttachOperationDetach ||
			record.TaskID != task.ID {
			return attachrender.AttachTaskRenderInput{}, errs.New(
				errs.KindValidationFailed,
				"Attach detach render requires its detaching record",
			)
		}
		excludedAttachID = record.ID
	default:
		return attachrender.AttachTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Attach render Task type is invalid",
		)
	}

	joins, err := taskplanning.ResolveAttachNetworkJoins(
		record.EnvironmentID,
		projection.Record,
		unionRecords,
		excludedAttachID,
	)
	if err != nil {
		return attachrender.AttachTaskRenderInput{}, err
	}
	return attachrender.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: record.ID,
		AttachName: record.Name,
		TenantID:   tenant.Record.ID, TenantSlug: tenant.Record.Slug,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: record.EnvironmentID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir:      environment.Record.VolumeDir,
		BackingServiceID:         record.BackingServiceID,
		BackingProjectID:         record.BackingProjectID,
		AdapterKey:               backingService.Record.Desired.Adapter,
		Authentication:           backingService.Record.Desired.Authentication,
		HookConfiguration:        backinghook.CloneConfiguration(backingService.Record.Desired.Hooks),
		DesiredRevisionID:        desiredRevisionID,
		ArtifactID:               artifactID,
		RenderGeneration:         projection.Record.RenderGeneration,
		EnvironmentEpochRevision: runtime.EpochRevision,
		RuntimeProjection:        runtime.Projection,
		RunningServiceIDs:        slices.Sorted(slices.Values(runtime.RunningServiceIDs)),
		Services:                 attachTaskServiceSnapshots(projection.Record.DesiredServices),
		Networks:                 attachTaskOwnedNetworkSnapshots(projection.Record.DesiredZones),
		Volumes:                  slices.Clone(projection.Record.Volumes),
		VolumeMounts:             slices.Clone(projection.Record.VolumeMounts),
		NetworkJoins:             joins,
		ConsumerServiceIDs:       []string{record.ServiceID},
		GrantAttachIDs:           append([]string(nil), record.GrantAttachIDs...),
		ServiceDependencyPlans:   projection.Record.ServiceDependencyPlans.Clone(),
	}, nil
}

func attachTaskServiceSnapshots(
	values []servicerecord.EnvironmentServiceProjection,
) []attachrender.AttachTaskServiceSnapshot {
	snapshots := make([]attachrender.AttachTaskServiceSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = attachrender.AttachTaskServiceSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}

func attachTaskOwnedNetworkSnapshots(
	values []projectionrecord.EnvironmentZoneProjection,
) []attachrender.AttachTaskOwnedNetworkSnapshot {
	snapshots := make([]attachrender.AttachTaskOwnedNetworkSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = attachrender.AttachTaskOwnedNetworkSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}
