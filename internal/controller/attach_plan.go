package controller

import (
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type AttachPlanGrantIdentity struct {
	AttachID string
	Database string
}

type AttachPlanIdentity struct {
	Authentication core.BackingAuthentication
	Database       string
	Role           string
	Password       []byte
	Grants         []AttachPlanGrantIdentity
}

func (identity *AttachPlanIdentity) Clear() {
	if identity == nil {
		return
	}
	clear(identity.Password)
	identity.Password = nil
	identity.Grants = nil
}

type AttachPlanIdentityConsumer func(AttachPlanIdentity) error

type attachPlanRecordReader interface {
	GetAttach(context.Context, string) (etcd.Versioned[etcd.AttachRecord], error)
	GetAttachTaskRenderInput(context.Context, string) (etcd.Versioned[etcd.AttachTaskRenderInput], error)
	GetBlueprintAttachTaskIntent(
		context.Context,
		string,
	) (etcd.Versioned[etcd.BlueprintAttachTaskIntent], bool, error)
}

// BuildAttachProvisionSteps compiles one already-resolved credential owner's
// typed adapter procedure. Blueprint and direct Attach planning share this
// exact builder so their Agent payloads cannot drift.
func BuildAttachProvisionSteps(
	task etcd.TaskRecord,
	record etcd.AttachRecord,
	adapterKey string,
	identity AttachPlanIdentity,
) ([]*agentpb.ExecutionStep, error) {
	if task.Type != etcd.TaskAttach {
		return nil, errs.New(errs.KindInternal, "Attach provision procedure requires an Attach Task shape")
	}
	return attachProcedureSteps(task, record, adapterKey, identity)
}

type attachPlanServiceReader interface {
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
}

type attachPlanIdentityResolver interface {
	ResolveTaskIdentity(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		string,
		AttachPlanIdentityConsumer,
	) error
}

func NewTaskPlanResolverWithAttachments(
	volumeRoot string,
	blueprints blueprintPlanStateReader,
	attaches attachPlanRecordReader,
	services attachPlanServiceReader,
	identities attachPlanIdentityResolver,
	componentCatalog []EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	resolver, err := NewTaskPlanResolverWithBlueprints(volumeRoot, blueprints, componentCatalog)
	if err != nil {
		return nil, err
	}
	if attaches == nil || services == nil || identities == nil {
		return nil, errs.New(errs.KindInternal, "Attach plan dependencies are required")
	}
	resolver.attaches = attaches
	resolver.services = services
	resolver.attachIdentities = identities
	return resolver, nil
}

func (resolver *TaskPlanResolver) resolveAttachPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.attaches == nil || resolver.services == nil || resolver.attachIdentities == nil ||
		task.Executor != etcd.TaskExecutorAgent || ids.Validate(ids.KindAttach, task.Target) != nil ||
		len(task.Params) != 1 || len(task.Materializations) != 0 || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Attach Task shape is invalid")
	}
	current, err := resolver.attaches.GetAttach(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	if current.Record.TaskID != task.ID {
		return nil, errs.New(errs.KindInternal, "durable Attach Task no longer owns its target")
	}
	renderInput, err := resolver.attaches.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil {
		return nil, err
	}
	if renderInput.Record.PlanID != task.PlanID || renderInput.Record.AttachID != current.Record.ID ||
		renderInput.Record.EnvironmentID != current.Record.EnvironmentID ||
		renderInput.Record.RenderGeneration != uint64(task.RenderGeneration) ||
		renderInput.Record.BackingProjectID != current.Record.BackingProjectID ||
		!slices.Equal(renderInput.Record.ConsumerServiceIDs, []string{current.Record.ServiceID}) ||
		!slices.Equal(renderInput.Record.GrantAttachIDs, current.Record.GrantAttachIDs) ||
		task.Params[etcd.TaskMutationEnvironmentParam] != current.Record.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "durable Attach Task render input does not match its Task")
	}
	operation, err := attachPlanOperation(task, current.Record)
	if err != nil {
		return nil, err
	}
	backing, err := resolver.services.GetService(ctx, current.Record.BackingServiceID)
	if err != nil {
		return nil, err
	}
	if backing.Record.EnvironmentID != current.Record.BackingEnvironmentID ||
		backing.Record.Desired.ID != current.Record.BackingServiceID ||
		backing.Record.BackingNetworkID != current.Record.BackingNetworkID ||
		backing.Record.Runtime.ServiceID != current.Record.BackingServiceID ||
		backing.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return nil, errs.New(errs.KindStateConflict, "Attach backing Service is not runnable")
	}
	if renderInput.Record.BackingServiceID != backing.Record.Desired.ID ||
		renderInput.Record.AdapterKey != backing.Record.Desired.Adapter ||
		renderInput.Record.Authentication != backing.Record.Desired.Authentication {
		return nil, errs.New(errs.KindStateConflict, "Attach backing Service changed after Task publication")
	}
	adapter, found := adapters.Get(renderInput.Record.AdapterKey)
	if !found {
		return nil, errs.New(errs.KindValidationFailed, "Attach backing Service adapter is not registered")
	}
	authentication, authErr := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), renderInput.Record.Authentication,
	)
	if authErr != nil || authentication != renderInput.Record.Authentication {
		return nil, errs.New(errs.KindStateConflict, "Attach backing Service authentication mode is invalid")
	}
	if err := resolver.validateAttachGrantTargets(ctx, current.Record); err != nil {
		return nil, err
	}
	pinned, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx,
		current.Record.EnvironmentID,
		renderInput.Record.DesiredRevisionID,
	)
	if err != nil {
		return nil, err
	}
	projection := pinned.Record
	if !found || projection.EnvironmentID != current.Record.EnvironmentID ||
		projection.RevisionID != renderInput.Record.DesiredRevisionID ||
		projection.RenderGeneration != renderInput.Record.RenderGeneration ||
		!slices.Equal(attachPlanServiceSnapshots(projection.DesiredServices), renderInput.Record.Services) ||
		!slices.Equal(attachPlanOwnedNetworkSnapshots(projection.DesiredZones), renderInput.Record.Networks) ||
		!slices.Equal(projection.Volumes, renderInput.Record.Volumes) ||
		!projection.ServiceDependencyPlans.Equal(renderInput.Record.ServiceDependencyPlans) {
		return nil, errs.New(errs.KindInternal, "durable Attach Task projection does not match its render input")
	}
	runtime := renderInput.Record.RuntimeProjection
	if runtime.EnvironmentID != projection.EnvironmentID || runtime.RevisionID != projection.RevisionID ||
		runtime.RenderGeneration != projection.RenderGeneration ||
		!slices.Equal(attachPlanServiceSnapshots(runtime.DesiredServices), renderInput.Record.Services) {
		return nil, errs.New(errs.KindInternal, "durable Attach Task runtime capture does not match its projection")
	}
	baseline := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(runtime.ComposeArtifact, baseline); err != nil {
		return nil, errs.New(errs.KindInternal, "durable Attach Task runtime artifact is corrupt")
	}
	artifact, err := MutateAttachNetworkArtifact(
		ctx, baseline, runtime, renderInput.Record.NetworkJoins,
		renderInput.Record.ArtifactID,
	)
	if err != nil {
		return nil, err
	}
	applyRuntime := slices.Contains(renderInput.Record.RunningServiceIDs, current.Record.ServiceID)
	if adapter.Manual() || !current.Record.OwnsCredential() {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "manual Attach Task step count is invalid")
		}
		return BuildPlan(PlanBuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
			TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
			Steps: []*agentpb.ExecutionStep{
				attachComposeStep(task, 0, artifact, attachRuntimeServiceIDs(current.Record.ServiceID, applyRuntime)),
			},
		})
	}
	if authentication == core.BackingAuthenticationNone {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "no-auth Attach Task step count is invalid")
		}
		if err := resolver.attachIdentities.ResolveTaskIdentity(
			ctx,
			current,
			task.ID,
			func(identity AttachPlanIdentity) error {
				if identity.Authentication != authentication {
					return errs.New(errs.KindStateConflict, "Attach encrypted authentication mode changed")
				}
				procedureTask := task
				procedureTask.Steps = nil
				_, buildErr := attachProcedureSteps(
					procedureTask, current.Record, renderInput.Record.AdapterKey, identity,
				)
				return buildErr
			},
		); err != nil {
			return nil, err
		}
		return BuildPlan(PlanBuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
			TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
			Steps: []*agentpb.ExecutionStep{
				attachComposeStep(task, 0, artifact, attachRuntimeServiceIDs(current.Record.ServiceID, applyRuntime)),
			},
		})
	}
	var plan *agentpb.ExecutionPlan
	err = resolver.attachIdentities.ResolveTaskIdentity(ctx, current, task.ID, func(identity AttachPlanIdentity) error {
		if identity.Authentication != authentication {
			return errs.New(errs.KindStateConflict, "Attach encrypted authentication mode changed")
		}
		steps, buildErr := attachNetworkProcedureSteps(
			task, current.Record, renderInput.Record.AdapterKey, artifact, identity, applyRuntime,
		)
		if buildErr != nil {
			return buildErr
		}
		defer clearAdapterProcedurePasswords(steps)
		plan, buildErr = BuildPlan(PlanBuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
			TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
		})
		return buildErr
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func attachPlanServiceSnapshots(values []etcd.EnvironmentServiceProjection) []etcd.AttachTaskServiceSnapshot {
	snapshots := make([]etcd.AttachTaskServiceSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskServiceSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}

func attachPlanOwnedNetworkSnapshots(values []etcd.EnvironmentZoneProjection) []etcd.AttachTaskOwnedNetworkSnapshot {
	snapshots := make([]etcd.AttachTaskOwnedNetworkSnapshot, len(values))
	for index, value := range values {
		snapshots[index] = etcd.AttachTaskOwnedNetworkSnapshot{ID: value.Desired.ID, Name: value.Desired.Name}
	}
	return snapshots
}

func attachNetworkProcedureSteps(
	task etcd.TaskRecord,
	record etcd.AttachRecord,
	adapterKey string,
	artifact *agentpb.ComposeArtifact,
	identity AttachPlanIdentity,
	applyRuntime bool,
) ([]*agentpb.ExecutionStep, error) {
	if len(task.Steps) != len(identity.Grants)+2 {
		return nil, errs.New(errs.KindInternal, "durable Attach Task step count is invalid")
	}
	procedureTask := task
	if task.Type == etcd.TaskAttach {
		procedureTask.Steps = append([]etcd.TaskStepRecord(nil), task.Steps[:len(task.Steps)-1]...)
		procedures, err := attachProcedureSteps(procedureTask, record, adapterKey, identity)
		if err != nil {
			return nil, err
		}
		return append(procedures, attachComposeStep(
			task, len(task.Steps)-1, artifact, attachRuntimeServiceIDs(record.ServiceID, applyRuntime),
		)), nil
	}
	grantCount := len(identity.Grants)
	procedureTask.Steps = append([]etcd.TaskStepRecord(nil), task.Steps[:grantCount]...)
	procedureTask.Steps = append(procedureTask.Steps, task.Steps[len(task.Steps)-1])
	procedures, err := attachProcedureSteps(procedureTask, record, adapterKey, identity)
	if err != nil {
		return nil, err
	}
	steps := append([]*agentpb.ExecutionStep(nil), procedures[:grantCount]...)
	steps = append(steps, attachComposeStep(
		task, grantCount, artifact, attachRuntimeServiceIDs(record.ServiceID, applyRuntime),
	))
	return append(steps, procedures[grantCount]), nil
}

func attachRuntimeServiceIDs(serviceID string, running bool) []string {
	if running {
		return []string{serviceID}
	}
	return nil
}

func attachComposeStep(
	task etcd.TaskRecord,
	stepIndex int,
	artifact *agentpb.ComposeArtifact,
	serviceIDs []string,
) *agentpb.ExecutionStep {
	apply := &agentpb.ComposeApply{ArtifactId: artifact.ArtifactId}
	apply.ServiceIds = append([]string(nil), serviceIDs...)
	apply.NoDependencies = true
	return &agentpb.ExecutionStep{
		StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: apply},
	}
}

func attachPlanOperation(task etcd.TaskRecord, record etcd.AttachRecord) (agentpb.PlanOperation, error) {
	switch task.Type {
	case etcd.TaskAttach:
		if record.Operation != etcd.AttachOperationProvision ||
			(record.Status != core.AttachPending && record.Status != core.AttachProvisioning) {
			return 0, errs.New(errs.KindStateConflict, "Attach is not provisionable")
		}
		return agentpb.PlanOperation_PLAN_OPERATION_ATTACH, nil
	case etcd.TaskDetach:
		if record.Operation != etcd.AttachOperationDetach || record.Status != core.AttachDetaching {
			return 0, errs.New(errs.KindStateConflict, "Attach is not detaching")
		}
		return agentpb.PlanOperation_PLAN_OPERATION_DETACH, nil
	default:
		return 0, errs.New(errs.KindInternal, "durable Attach Task type is invalid")
	}
}

func (resolver *TaskPlanResolver) validateAttachGrantTargets(ctx context.Context, record etcd.AttachRecord) error {
	for _, grantID := range record.GrantAttachIDs {
		grant, err := resolver.attaches.GetAttach(ctx, grantID)
		if err != nil {
			return err
		}
		if grant.Record.Status != core.AttachReady || grant.Record.BackingServiceID != record.BackingServiceID {
			return errs.New(errs.KindStateConflict, "Attach grant target is not ready on the same backing Service")
		}
	}
	return nil
}

func attachProcedureSteps(
	task etcd.TaskRecord,
	record etcd.AttachRecord,
	adapterKey string,
	identity AttachPlanIdentity,
) ([]*agentpb.ExecutionStep, error) {
	adapter, found := adapters.Get(adapterKey)
	if !found {
		return nil, errs.New(errs.KindValidationFailed, "Attach task adapter is not registered")
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), identity.Authentication,
	)
	if err != nil || authentication != identity.Authentication {
		return nil, errs.New(errs.KindInternal, "Attach task authentication mode is invalid")
	}
	if authentication == core.BackingAuthenticationNone {
		if identity.Role != "" || len(identity.Password) != 0 || len(identity.Grants) != 0 ||
			len(record.GrantAttachIDs) != 0 || len(task.Steps) != 0 {
			return nil, errs.New(errs.KindInternal, "no-auth Attach task identity or step count is invalid")
		}
		return nil, nil
	}
	if identity.Role == "" || len(identity.Password) == 0 || len(identity.Grants) != len(record.GrantAttachIDs) ||
		len(task.Steps) != len(record.GrantAttachIDs)+1 {
		return nil, errs.New(errs.KindInternal, "Attach task identity or step count is invalid")
	}
	for index, grant := range identity.Grants {
		if grant.AttachID != record.GrantAttachIDs[index] || grant.Database == "" {
			return nil, errs.New(errs.KindInternal, "Attach task grant identity is invalid")
		}
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	wireAuthentication, err := encodeBackingAuthentication(authentication)
	if err != nil {
		return nil, err
	}
	appendProcedure := func(index int, phase agentpb.AdapterProcedurePhase, database, grantOn string, password []byte) {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[index].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: &agentpb.AdapterProcedure{
				AdapterKey: adapterKey, Phase: phase, AttachId: record.ID,
				BackingServiceId: record.BackingServiceID, Role: identity.Role,
				Password: append([]byte(nil), password...), Database: database, GrantOn: grantOn,
				Authentication: wireAuthentication,
			}},
		})
	}
	if task.Type == etcd.TaskAttach {
		appendProcedure(0, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
			identity.Database, "", identity.Password)
		for index, grant := range identity.Grants {
			appendProcedure(index+1, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT,
				"", grant.Database, nil)
		}
		return steps, nil
	}
	for index := len(identity.Grants) - 1; index >= 0; index-- {
		grant := identity.Grants[index]
		stepIndex := len(identity.Grants) - 1 - index
		appendProcedure(stepIndex, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE,
			"", grant.Database, nil)
	}
	detachPassword := []byte(nil)
	if authentication == core.BackingAuthenticationPassword {
		detachPassword = identity.Password
	}
	appendProcedure(len(identity.Grants), agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH,
		identity.Database, "", detachPassword)
	return steps, nil
}

func clearAdapterProcedurePasswords(steps []*agentpb.ExecutionStep) {
	for _, step := range steps {
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}
