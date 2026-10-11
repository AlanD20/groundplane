package blueprint

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Prepare constructs one private Service child from the parent's exact sealed
// runtime projection. Publication remains the coordinator's responsibility;
// it rechecks the desired unit and execution epoch atomically.
func (service *Service) Prepare(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) (blueprintcoordinator.PreparedChild, error) {
	if service == nil || ctx == nil || parent.Executor != taskjournal.TaskExecutorBlueprint ||
		validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil ||
		parent.Status != taskjournal.TaskStatusRunning || unit.Removal &&
		(unit.Target.Kind != ids.KindComponent && unit.Target.Kind != ids.KindEnvEntry &&
			unit.Target.Kind != ids.KindService && unit.Target.Kind != ids.KindNetwork &&
			unit.Target.Kind != ids.KindAttach) {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint child request is invalid",
		)
	}
	if parent.StartedAt == nil || parent.TimeoutSeconds <= 0 ||
		parent.TimeoutSeconds > int64(24*time.Hour/time.Second) {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent execution deadline is unavailable",
		)
	}
	elapsed := service.now().UTC().Sub(*parent.StartedAt)
	timeoutSeconds := parent.TimeoutSeconds - int64(elapsed/time.Second)
	if elapsed%time.Second > 0 {
		timeoutSeconds--
	}
	if elapsed < 0 || timeoutSeconds <= 0 {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent has no execution time remaining",
		)
	}
	if unit.Target.Kind == ids.KindNetwork {
		if unit.Removal {
			return service.prepareAuthoredZoneRemovalChild(ctx, parent, unit, timeoutSeconds)
		}
		return service.prepareAuthoredNetworkChild(ctx, parent, unit, timeoutSeconds)
	}
	if unit.Target.Kind == ids.KindVolume {
		return service.prepareAuthoredVolumeChild(ctx, parent, unit, timeoutSeconds)
	}
	if unit.Target.Kind == ids.KindAttach {
		if unit.Removal {
			return service.prepareAuthoredAttachRemovalChild(ctx, parent, unit, timeoutSeconds)
		}
		return service.prepareAuthoredAttachChild(ctx, parent, unit, timeoutSeconds)
	}
	if unit.Target.Kind == ids.KindComponent {
		return service.prepareAuthoredComponentChild(ctx, parent, unit, snapshot, timeoutSeconds)
	}
	if unit.Target.Kind == ids.KindEnvEntry && unit.Removal {
		return service.prepareAuthoredEntryRemovalChild(ctx, parent, unit, timeoutSeconds)
	}
	if unit.Target.Kind == ids.KindService && unit.Removal {
		return service.prepareAuthoredServiceRemovalChild(ctx, parent, unit, timeoutSeconds)
	}
	if unit.Target.Kind != ids.KindService {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint unit has no child preparer",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	projectInput, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, input.desired, input.identities)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	serviceName := ""
	for _, identity := range input.identities.Services {
		if identity.ID == unit.Target.ID {
			serviceName = identity.Name
			break
		}
	}
	selectedService, found := projectInput.Services[serviceName]
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Service child authored definition is unavailable",
		)
	}
	upstreams, err := zonedns.Upstreams(selectedService, projectInput.Networks)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	dnsActivation, err := service.repository.ReadZoneDNSActivation(ctx, upstreams)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	// Runtime inputs and the Backup policy are settled atomically before a
	// Service child can select this immutable projection.
	projection, found, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, parent.Owner.EnvironmentID, parent.ID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || projection.Record.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.Record.RevisionID != parent.ID ||
		projection.Record.RenderGeneration != input.desired.RenderGeneration {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child runtime projection is unavailable",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact, artifact,
	); err != nil || ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child artifact is invalid",
		)
	}
	createdAt := service.now().UTC()
	allScripts, _, err := service.listBlueprintScripts(ctx, parent.Owner.EnvironmentID, service.repository)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	scripts := make([]scriptrecord.Record, 0)
	for _, versioned := range allScripts {
		if versioned.Record.ScriptSetGeneration != parent.ID {
			return blueprintcoordinator.PreparedChild{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Script set has been superseded",
			)
		}
		if versioned.Record.ServiceID == unit.Target.ID {
			scripts = append(scripts, versioned.Record)
		}
	}
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: parent.Owner.EnvironmentID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam:      parent.ID,
			taskjournal.TaskBlueprintParentParam:            parent.ID,
			taskjournal.TaskMaterializationEnvironmentParam: parent.Owner.EnvironmentID,
			taskcontract.EnvironmentBlueprintArtifactParam:  artifact.GetArtifactId(),
			taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
		},
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	allocate := func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, createdAt, child.PlanID, name) }
	child, prefixSteps, err := service.prepareAuthoredServiceFiles(
		ctx,
		child,
		projection.Record,
		input,
		artifact,
		allocate,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	prepared, err := service.blueprintReleases.PrepareServiceUnit(
		ctx,
		blueprintrelease.PrepareServiceUnitInput{
			ServiceID: unit.Target.ID, Projection: projection.Record,
			Scripts: scripts, PrefixSteps: prefixSteps,
			Task: child, CreatedAt: createdAt,
			AllocateNamed: allocate,
		},
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	prerequisites, err := service.loadAuthoredPrerequisiteAuthority(
		ctx, parent.Owner.EnvironmentID, parent.ID,
	)
	if err == nil && !sameBlueprintRequirements(
		prerequisites.Record.Requirements, projection.Record.BlueprintRequirements,
	) {
		err = errs.New(errs.KindStateConflict, "Blueprint Service prerequisite authority changed")
	}
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, errors.Join(err, prepared.Abandon(ctx))
	}
	requirementGate, err := prepareRequirementGateWithEdges(
		prepared.Task,
		projection.Record.BlueprintRequirements,
		prerequisites.Record.InheritedTaskEdges,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, errors.Join(err, prepared.Abandon(ctx))
	}
	publication, err := prepared.Publication.WithRequirementGate(
		requirementGate, prepared.Task, projection.Record,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, errors.Join(err, prepared.Abandon(ctx))
	}
	publication, err = publication.WithZoneDNSActivation(dnsActivation)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, errors.Join(err, prepared.Abandon(ctx))
	}
	return blueprintcoordinator.PreparedChild{
		Task: prepared.Task, ReleasePublication: publication,
	}, nil
}

func (service *Service) prepareAuthoredAttachChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok || service.attachFacts == nil || ids.Validate(ids.KindAttach, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Attach child authority is not configured",
		)
	}
	versioned, found, err := reader.GetBlueprintAttachInputGeneration(ctx, parent.ID, unit.Target.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach input generation is unavailable",
		)
	}
	defer attachinputs.Clear(&versioned.Record)
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: versioned.Record.OperationID,
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: parent.Owner.EnvironmentID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			attachinputs.TaskAttachIDParam:             unit.Target.ID,
		},
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	transferred, err := attachinputs.TransferToChild(versioned.Record, child.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if transferred.ResolvedInputs != nil {
		child.Configuration = &taskconfiguration.TaskConfiguration{
			BackingHookInputs: &taskconfiguration.TaskBackingHookInputSet{
				ProjectID:        transferred.BackingProjectID,
				CiphertextSHA256: transferred.ResolvedInputs.CiphertextSHA256,
				SecretSources: append(
					[]tasksecretpinrecord.Record(nil), transferred.SecretSources...,
				),
			},
		}
		if transferred.SecretPins != nil {
			pins := *transferred.SecretPins
			child.Configuration.SecretPins = &pins
		}
	}
	prepared, plan, err := taskplanning.PrepareBlueprintAttachUnit(
		ctx, service.volumeRoot, service.attachFacts, transferred, child,
	)
	if plan != nil {
		for _, step := range plan.GetSteps() {
			taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
		}
	}
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

type authoredParentVolumeIntentReader interface {
	BlueprintParentProtectedIntentSHA256(context.Context, string) (string, error)
	BlueprintVolumeNeedsCreate(context.Context, string, string, blueprintunits.Unit) (bool, error)
}

func (service *Service) prepareAuthoredVolumeChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if service.plans == nil || ids.Validate(ids.KindVolume, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Volume child request is invalid",
		)
	}
	authority, ok := service.repository.(authoredParentVolumeIntentReader)
	if !ok {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Volume child intent authority is not configured",
		)
	}
	intentSHA256, err := authority.BlueprintParentProtectedIntentSHA256(ctx, parent.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	create, err := authority.BlueprintVolumeNeedsCreate(
		ctx, parent.Owner.EnvironmentID, parent.ID, unit,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	mode := taskjournal.TaskBlueprintVolumeModeVerify
	if create {
		mode = taskjournal.TaskBlueprintVolumeModeCreate
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: unit.Target.ID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			taskjournal.TaskBlueprintVolumeUnitParam:   unit.Target.ID,
			taskjournal.TaskBlueprintVolumeModeParam:   mode,
			taskjournal.TaskBlueprintVolumeIntentParam: intentSHA256,
		},
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, _, err := service.plans.PrepareBlueprintVolumeUnit(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

func (service *Service) prepareAuthoredNetworkChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if service.plans == nil || ids.Validate(ids.KindNetwork, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Network child request is invalid",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: unit.Target.ID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			taskjournal.TaskBlueprintNetworkUnitParam:  unit.Target.ID,
		},
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, _, err := service.plans.PrepareBlueprintNetworkUnit(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

var _ blueprintcoordinator.ChildPreparer = (*Service)(nil)
