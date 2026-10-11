package taskplanning

import (
	"maps"

	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// Capture freezes the private preparation without exposing its mutable plan.
// Only Service children can be retained this way; aggregate/Component work has
// its separate captured authority and cannot become a Service rollout.
func (runtime BlueprintReleaseRuntime) Capture() (releaserender.BlueprintServiceRolloutCapture, error) {
	if runtime.plan == nil || runtime.identity.actor != taskjournal.TaskActorSystem ||
		runtime.identity.executor != taskjournal.TaskExecutorAgent || runtime.identity.taskType != taskjournal.TaskUpdate {
		return releaserender.BlueprintServiceRolloutCapture{}, errs.New(errs.KindValidationFailed,
			"Blueprint rollout preparation is not a private Agent Service child")
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(runtime.plan)
	if err != nil {
		return releaserender.BlueprintServiceRolloutCapture{}, errs.Wrap(errs.KindInternal, err)
	}
	capture := releaserender.BlueprintServiceRolloutCapture{
		TaskID: runtime.identity.taskID, OperationID: runtime.identity.operationID,
		Owner: runtime.identity.owner, TimeoutSeconds: runtime.identity.timeoutSeconds,
		Params: maps.Clone(runtime.params), Steps: taskjournal.CloneTaskSteps(runtime.steps), NativePlan: value,
	}
	if _, err := releaserender.OpenBlueprintServiceRolloutCapture(capture); err != nil {
		clear(value)
		return releaserender.BlueprintServiceRolloutCapture{}, err
	}
	return capture, nil
}

// RestoreBlueprintReleaseRuntime opens immutable prepared data. It does not
// rebuild native work, resolve images or establish a published Task/receipt.
func RestoreBlueprintReleaseRuntime(
	capture releaserender.BlueprintServiceRolloutCapture,
) (BlueprintReleaseRuntime, error) {
	plan, err := releaserender.OpenBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return BlueprintReleaseRuntime{}, err
	}
	return BlueprintReleaseRuntime{
		plan: plan,
		identity: blueprintReleaseRuntimeIdentity{
			taskID: capture.TaskID, operationID: capture.OperationID,
			planID: plan.PlanId, target: plan.TargetId, owner: capture.Owner,
			actor: taskjournal.TaskActorSystem, executor: taskjournal.TaskExecutorAgent,
			taskType: taskjournal.TaskUpdate, generation: int32(plan.RenderGeneration),
			timeoutSeconds: capture.TimeoutSeconds,
		},
		params: maps.Clone(capture.Params), steps: taskjournal.CloneTaskSteps(capture.Steps),
	}, nil
}
