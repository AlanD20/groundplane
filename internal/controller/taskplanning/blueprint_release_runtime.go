package taskplanning

import (
	"context"
	"encoding/hex"
	"maps"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintReleaseRuntime owns a validated native rollout and its exact Task
// identity. It is not publishable execution authority: the configuration owner
// must bind the actual predecessor files before the final plan is sealed.
type BlueprintReleaseRuntime struct {
	plan     *agentpb.ExecutionPlan
	identity blueprintReleaseRuntimeIdentity
	params   map[string]string
	steps    []taskjournal.TaskStepRecord
}

type blueprintReleaseRuntimeIdentity struct {
	taskID, operationID, planID, target string
	owner                               taskjournal.TaskOwner
	actor                               taskjournal.TaskActor
	executor                            taskjournal.TaskExecutor
	taskType                            taskjournal.TaskType
	generation                          int32
	timeoutSeconds                      int64
}

func blueprintRuntimeIdentity(task etcd.TaskRecord) blueprintReleaseRuntimeIdentity {
	return blueprintReleaseRuntimeIdentity{
		taskID: task.ID, operationID: task.OperationID, planID: task.PlanID,
		target: task.Target, owner: task.Owner, actor: task.Actor,
		executor: task.Executor, taskType: task.Type,
		generation: task.RenderGeneration, timeoutSeconds: task.TimeoutSeconds,
	}
}

// PrepareBlueprintReleaseRuntime freezes rollout, hooks and native restoration
// without reading the Environment's mutable configuration head. The existing
// shared planner owns both early capture and ordinary Blueprint Deploy.
func (resolver *TaskPlanResolver) PrepareBlueprintReleaseRuntime(
	ctx context.Context,
	task etcd.TaskRecord,
	input BlueprintReleasePlanInput,
) (etcd.TaskRecord, BlueprintReleaseRuntime, error) {
	task, plan, err := resolver.prepareBlueprintReleaseRuntime(ctx, task, input)
	if err != nil {
		return etcd.TaskRecord{}, BlueprintReleaseRuntime{}, err
	}
	return task, BlueprintReleaseRuntime{
		plan: plan, identity: blueprintRuntimeIdentity(task),
		params: maps.Clone(task.Params), steps: taskjournal.CloneTaskSteps(task.Steps),
	}, nil
}

// SealBlueprintReleaseRuntime attaches exact file recovery at final publication
// time. It does not re-render the rollout, resolve images or read Service state.
func (resolver *TaskPlanResolver) SealBlueprintReleaseRuntime(
	ctx context.Context,
	task etcd.TaskRecord,
	runtime BlueprintReleaseRuntime,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	identity := blueprintRuntimeIdentity(task)
	// A capture fixes the maximum budget. Delayed child publication may shorten
	// it to the visible parent's remaining deadline, never extend native work.
	identity.timeoutSeconds = runtime.identity.timeoutSeconds
	if resolver == nil || ctx == nil || runtime.plan == nil ||
		identity != runtime.identity || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > runtime.identity.timeoutSeconds || !maps.Equal(task.Params, runtime.params) {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed,
			"Blueprint rollout capture does not match its Task")
	}
	draft := proto.CloneOf(runtime.plan)
	procedure := draft.GetCandidateReleaseProcedure()
	if procedure == nil || len(procedure.GetMembers()) == 0 || procedure.GetConfigurationRestoration() != nil {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed,
			"Blueprint rollout capture already contains file recovery or has no Release")
	}
	configuration, fileSteps, err := resolver.configurationRecoverySteps(
		ctx, task, procedure.Members[0].CandidateArtifactId,
	)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	procedure.ConfigurationRestoration = configuration
	plan, err := taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: draft.PlanId,
		RenderGeneration: draft.RenderGeneration, Operation: draft.Operation,
		TargetID: draft.TargetId, Artifacts: draft.Artifacts,
		ScriptRunnerSnapshots:     draft.ScriptRunnerSnapshots,
		ScriptRunnerProjections:   draft.ScriptRunnerProjections,
		ScriptBodyArtifacts:       draft.ScriptBodyArtifacts,
		Steps:                     append(draft.Steps, fileSteps...),
		CandidateReleaseProcedure: procedure,
		ManagedComponentProcedure: draft.ManagedComponentProcedure,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	task.ComponentActionStepIDs, err = executionplan.ComponentActionStepIDs(plan)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.Steps = taskjournal.CloneTaskSteps(runtime.steps)
	for _, step := range fileSteps {
		task.Steps = append(task.Steps, taskjournal.TaskStepRecord{
			ID: step.StepId, Kind: taskjournal.TaskStepOperation,
		})
	}
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps, plan)
	return task, plan, nil
}

func (resolver *TaskPlanResolver) PrepareBlueprintReleaseTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input BlueprintReleasePlanInput,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	task, runtime, err := resolver.PrepareBlueprintReleaseRuntime(ctx, task, input)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	return resolver.SealBlueprintReleaseRuntime(ctx, task, runtime)
}
