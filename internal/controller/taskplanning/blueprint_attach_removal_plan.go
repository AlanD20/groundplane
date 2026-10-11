package taskplanning

import (
	"context"
	"encoding/hex"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type authoredAttachRemovalIntentReader interface {
	GetAuthoredAttachRemovalIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[environmentchanges.AttachRemovalIntent], bool, error)
}

type storedAttachRemovalIdentityResolver interface {
	ResolveStoredTaskIdentity(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		attachrecord.EncryptedFacts,
		string,
		AttachPlanIdentityConsumer,
	) error
	ResolveDraftHookInput(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		*attachrecord.EncryptedFacts,
		etcd.TaskRecord,
		*taskconfiguration.BackingHookEncryptedInputs,
		backinghook.Context,
		BackingHookInputConsumer,
	) error
}

// PrepareBlueprintAttachRemovalTask seals one ordinary Detach plan from the
// captured predecessor recipe. It performs no mutable topology reads.
func PrepareBlueprintAttachRemovalTask(
	ctx context.Context,
	volumeRoot string,
	inputs storedAttachRemovalIdentityResolver,
	intent environmentchanges.AttachRemovalIntent,
	task etcd.TaskRecord,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 0 || task.PlanHash != "" {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach removal is already prepared",
		)
	}
	adapter, found := adapters.Get(intent.RenderInput.AdapterKey)
	if !found {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed, "Attach backing adapter is not registered")
	}
	detachHook := intent.Detaching.HookBundle && intent.Detaching.OwnsCredential() &&
		intent.RenderInput.HookConfiguration != nil &&
		intent.RenderInput.HookConfiguration.Detach != nil
	stepCount := attachRemovalStepCount(adapter, intent.RenderInput.Authentication, intent.Detaching, detachHook)
	prepared := task
	prepared.Steps = make([]taskjournal.TaskStepRecord, stepCount)
	for index := range prepared.Steps {
		prepared.Steps[index] = taskjournal.TaskStepRecord{
			Kind: taskjournal.TaskStepOperation,
			ID:   ids.New(ids.KindStep),
		}
	}
	plan, err := buildBlueprintAttachRemovalPlan(ctx, volumeRoot, inputs, intent, prepared, hookInputs)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	prepared.Steps = taskjournal.CaptureStepDescriptions(prepared.Steps, plan)
	return prepared, plan, nil
}

func (resolver *TaskPlanResolver) resolveBlueprintAttachRemovalPlan(
	ctx context.Context, task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.attaches.(authoredAttachRemovalIntentReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "authored Attach removal authority is not configured")
	}
	inputs, ok := resolver.attachIdentities.(storedAttachRemovalIdentityResolver)
	if !ok || inputs == nil {
		return nil, errs.New(errs.KindInternal, "authored Attach removal identity resolver is not configured")
	}
	stored, found, err := reader.GetAuthoredAttachRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "authored Attach removal authority is unavailable")
	}
	defer environmentchanges.ClearAttachRemovalIntent(&stored.Record)
	return buildBlueprintAttachRemovalPlan(ctx, resolver.volumeRoot, inputs, stored.Record, task, nil)
}

func buildBlueprintAttachRemovalPlan(
	ctx context.Context,
	volumeRoot string,
	inputs storedAttachRemovalIdentityResolver,
	intent environmentchanges.AttachRemovalIntent,
	task etcd.TaskRecord,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
) (*agentpb.ExecutionPlan, error) {
	if err := validateBlueprintAttachRemovalTask(task, intent); err != nil {
		return nil, err
	}
	adapter, found := adapters.Get(intent.RenderInput.AdapterKey)
	if !found {
		return nil, errs.New(errs.KindValidationFailed, "Attach backing adapter is not registered")
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), intent.RenderInput.Authentication,
	)
	if err != nil || authentication != intent.RenderInput.Authentication {
		return nil, errs.New(errs.KindStateConflict, "Attach backing authentication changed")
	}
	if err := validateAttachRemovalReferences(intent); err != nil {
		return nil, err
	}
	runtime := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		intent.RenderInput.RuntimeProjection.ComposeArtifact, runtime,
	) != nil {
		return nil, errs.New(errs.KindInternal, "authored Attach runtime artifact is corrupt")
	}
	artifact, err := MutateAttachNetworkArtifact(
		ctx, runtime, intent.RenderInput.RuntimeProjection,
		intent.RenderInput.NetworkJoins, intent.RenderInput.ArtifactID,
	)
	if err != nil {
		return nil, err
	}
	current := etcdstore.Versioned[attachrecord.Record]{
		Record: intent.Detaching, Revision: intent.SourceRevision, ReadRevision: intent.SourceRevision,
	}
	applyRuntime := slices.Contains(intent.RenderInput.RunningServiceIDs, intent.Source.ServiceID)
	if adapter.Custom() {
		return buildAuthoredCustomDetachPlan(
			ctx, volumeRoot, inputs, task, current, intent, hookInputs, artifact, applyRuntime,
		)
	}
	if !intent.Detaching.OwnsCredential() {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "dependent authored Detach step count changed")
		}
		return buildAttachNetworkOnlyPlan(volumeRoot, task, artifact, intent.Source.ServiceID, applyRuntime)
	}
	if intent.Facts == nil {
		return nil, errs.New(errs.KindStateConflict, "authored Detach credential facts are unavailable")
	}
	if authentication == core.BackingAuthenticationNone {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "no-auth authored Detach step count changed")
		}
		err := inputs.ResolveStoredTaskIdentity(
			ctx,
			current,
			*intent.Facts,
			task.ID,
			func(identity AttachPlanIdentity) error {
				if identity.Authentication != authentication {
					return errs.New(errs.KindStateConflict, "Attach encrypted authentication changed")
				}
				procedureTask := task
				procedureTask.Steps = nil
				_, buildErr := attachProcedureSteps(
					procedureTask,
					intent.Detaching,
					intent.RenderInput.AdapterKey,
					identity,
				)
				return buildErr
			},
		)
		if err != nil {
			return nil, err
		}
		return buildAttachNetworkOnlyPlan(volumeRoot, task, artifact, intent.Source.ServiceID, applyRuntime)
	}
	var plan *agentpb.ExecutionPlan
	err = inputs.ResolveStoredTaskIdentity(
		ctx,
		current,
		*intent.Facts,
		task.ID,
		func(identity AttachPlanIdentity) error {
			if identity.Authentication != authentication {
				return errs.New(errs.KindStateConflict, "Attach encrypted authentication changed")
			}
			steps, buildErr := attachNetworkProcedureSteps(
				task, intent.Detaching, intent.RenderInput.AdapterKey, artifact, identity, applyRuntime,
			)
			if buildErr != nil {
				return buildErr
			}
			defer clearAdapterProcedurePasswords(steps)
			plan, buildErr = taskplan.Build(taskplan.BuildInput{
				VolumeRoot: volumeRoot, PlanID: task.PlanID,
				RenderGeneration: uint64(task.RenderGeneration),
				Operation:        agentpb.PlanOperation_PLAN_OPERATION_DETACH,
				TargetID:         task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
			})
			return buildErr
		},
	)
	return plan, err
}

func validateBlueprintAttachRemovalTask(task etcd.TaskRecord, intent environmentchanges.AttachRemovalIntent) error {
	if environmentchanges.ValidateAttachRemovalIntent(intent) != nil || task.ID != intent.TaskID ||
		task.Actor != taskjournal.TaskActorSystem || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskDetach || task.Target != intent.AttachID ||
		task.Owner.EnvironmentID != intent.EnvironmentID || task.PlanID != intent.RenderInput.PlanID ||
		task.Params[taskjournal.TaskBlueprintParentParam] != intent.ParentTaskID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.ParentTaskID ||
		task.Params[attachinputs.TaskAttachIDParam] != intent.AttachID ||
		task.Params[taskjournal.TaskMutationEnvironmentParam] != intent.EnvironmentID || len(task.Params) != 4 ||
		task.RenderGeneration <= 0 || uint64(task.RenderGeneration) != intent.RenderInput.RenderGeneration ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Steps) == 0 ||
		len(task.Materializations) != 0 || task.EntryRuntime != nil ||
		len(task.ComponentActionStepIDs) != 0 || len(task.ManagedComponentTeardownSources) != 0 {
		return errs.New(errs.KindInternal, "durable authored Attach removal Task changed")
	}
	adapter, found := adapters.Get(intent.RenderInput.AdapterKey)
	if !found {
		return errs.New(errs.KindStateConflict, "authored Attach removal adapter is unavailable")
	}
	detachHook := intent.Detaching.HookBundle && intent.Detaching.OwnsCredential() &&
		intent.RenderInput.HookConfiguration != nil &&
		intent.RenderInput.HookConfiguration.Detach != nil
	if len(task.Steps) != attachRemovalStepCount(
		adapter, intent.RenderInput.Authentication, intent.Detaching, detachHook,
	) {
		return errs.New(errs.KindInternal, "authored Attach removal step count changed")
	}
	for _, step := range task.Steps {
		if step.Kind != taskjournal.TaskStepOperation || ids.Validate(ids.KindStep, step.ID) != nil {
			return errs.New(errs.KindInternal, "authored Attach removal steps changed")
		}
	}
	return nil
}

func attachRemovalStepCount(
	adapter adapters.Adapter, authentication core.BackingAuthentication,
	record attachrecord.Record, detachHook bool,
) int {
	if adapter.Custom() {
		if record.OwnsCredential() && detachHook {
			return 2
		}
		return 1
	}
	if !record.OwnsCredential() || authentication == core.BackingAuthenticationNone {
		return 1
	}
	return len(record.GrantAttachIDs) + 2
}

func validateAttachRemovalReferences(intent environmentchanges.AttachRemovalIntent) error {
	byID := make(map[string]attachrecord.Record, len(intent.Topology))
	for _, source := range intent.Topology {
		byID[source.Record.ID] = source.Record
	}
	for _, grantID := range intent.Source.GrantAttachIDs {
		grant, found := byID[grantID]
		if !found || grant.Status != core.AttachReady || !grant.OwnsCredential() ||
			grant.BackingServiceID != intent.Source.BackingServiceID {
			return errs.New(errs.KindStateConflict, "authored Detach grant authority changed")
		}
	}
	if !intent.Source.OwnsCredential() {
		owner, found := byID[intent.Source.CredentialAttachID]
		if !found || owner.Status != core.AttachReady || !owner.OwnsCredential() ||
			owner.BackingServiceID != intent.Source.BackingServiceID {
			return errs.New(errs.KindStateConflict, "authored Detach credential owner changed")
		}
	}
	return nil
}

func buildAttachNetworkOnlyPlan(
	volumeRoot string, task etcd.TaskRecord, artifact *agentpb.ComposeArtifact,
	serviceID string, applyRuntime bool,
) (*agentpb.ExecutionPlan, error) {
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DETACH, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			attachComposeStep(task, 0, artifact, attachRuntimeServiceIDs(serviceID, applyRuntime)),
		},
	})
}

func buildAuthoredCustomDetachPlan(
	ctx context.Context,
	volumeRoot string,
	inputs storedAttachRemovalIdentityResolver,
	task etcd.TaskRecord,
	current etcdstore.Versioned[attachrecord.Record],
	intent environmentchanges.AttachRemovalIntent,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	artifact *agentpb.ComposeArtifact,
	applyRuntime bool,
) (*agentpb.ExecutionPlan, error) {
	definition := (*backinghook.Definition)(nil)
	if intent.RenderInput.HookConfiguration != nil {
		definition = intent.RenderInput.HookConfiguration.Detach
	}
	if !intent.Detaching.OwnsCredential() || definition == nil {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "network-only authored Custom Detach changed")
		}
		return buildAttachNetworkOnlyPlan(volumeRoot, task, artifact, intent.Source.ServiceID, applyRuntime)
	}
	if intent.Facts == nil || !intent.Detaching.HookBundle || len(task.Steps) != 2 {
		return nil, errs.New(errs.KindStateConflict, "authored Custom Detach authority is incomplete")
	}
	hookContext := backinghook.Context{
		Event: backinghook.Detach, BackingServiceID: intent.Detaching.BackingServiceID,
		AttachID: intent.Detaching.ID, TenantID: intent.RenderInput.TenantID,
		ProjectID: intent.RenderInput.ProjectID, EnvironmentID: intent.EnvironmentID,
		ServiceID: intent.Detaching.ServiceID,
	}
	var plan *agentpb.ExecutionPlan
	err := inputs.ResolveDraftHookInput(
		ctx, current, intent.Facts, task, hookInputs, hookContext,
		func(input backinghook.Input) error {
			hookStep, buildErr := backingHookStep(task.Steps[0], *definition, input, nil)
			if buildErr != nil {
				return buildErr
			}
			defer taskplan.ClearBackingHookProcedure(hookStep.GetBackingHookProcedure())
			plan, buildErr = taskplan.Build(taskplan.BuildInput{
				VolumeRoot: volumeRoot, PlanID: task.PlanID,
				RenderGeneration: uint64(task.RenderGeneration),
				Operation:        agentpb.PlanOperation_PLAN_OPERATION_DETACH, TargetID: task.Target,
				Artifacts: []*agentpb.ComposeArtifact{artifact},
				Steps: []*agentpb.ExecutionStep{
					hookStep,
					attachComposeStep(
						task,
						1,
						artifact,
						attachRuntimeServiceIDs(intent.Source.ServiceID, applyRuntime),
					),
				},
			})
			return buildErr
		},
	)
	return plan, err
}
