package releaserender

import (
	"bytes"
	"math"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintServiceRolloutCapture is prepared input, not a Task publication or
// acknowledged runtime. It freezes one native Service rollout before final
// configuration recovery is bound. Its storage owner must separately retain
// source/image/Secret references and compare the parent and Service authority.
type BlueprintServiceRolloutCapture struct {
	TaskID         string                       `json:"task_id"`
	OperationID    string                       `json:"operation_id"`
	Owner          taskjournal.TaskOwner        `json:"owner"`
	TimeoutSeconds int64                        `json:"timeout_seconds"`
	Params         map[string]string            `json:"params"`
	Steps          []taskjournal.TaskStepRecord `json:"steps"`
	NativePlan     []byte                       `json:"native_plan"`
}

// OpenBlueprintServiceRolloutCapture returns an owned, validated native plan.
// Publication still needs exact source comparisons and final file recovery.
func OpenBlueprintServiceRolloutCapture(capture BlueprintServiceRolloutCapture) (*agentpb.ExecutionPlan, error) {
	parentID := capture.Params[taskjournal.TaskBlueprintParentParam]
	if ids.Validate(ids.KindTask, capture.TaskID) != nil ||
		ids.Validate(ids.KindOperation, capture.OperationID) != nil ||
		taskjournal.ValidateOwner(capture.Owner) != nil ||
		ids.Validate(ids.KindEnvironment, capture.Owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, parentID) != nil || parentID == capture.TaskID ||
		capture.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		capture.Params[taskjournal.TaskMaterializationEnvironmentParam] != capture.Owner.EnvironmentID ||
		ids.Validate(
			ids.KindDeployment,
			string(ids.KindDeployment)+"_"+capture.Params[TaskReleasePublicationParam],
		) != nil ||
		capture.TimeoutSeconds < 1 || capture.TimeoutSeconds > 24*60*60 ||
		len(capture.NativePlan) == 0 || len(capture.NativePlan) > keyvalue.MaximumBytes {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture identity or size is invalid")
	}
	message := &agentpb.ExecutionPlan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(capture.NativePlan, message); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture is unreadable")
	}
	plan, err := executionplan.Validate(message)
	if err != nil {
		return nil, err
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if !bytes.Equal(canonical, capture.NativePlan) ||
		plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		plan.TargetId != capture.Owner.EnvironmentID || plan.RenderGeneration > math.MaxInt32 ||
		plan.GetCandidateReleaseProcedure() == nil ||
		len(plan.GetCandidateReleaseProcedure().GetMembers()) != 1 ||
		plan.GetCandidateReleaseProcedure().GetConfigurationRestoration() != nil ||
		plan.GetManagedComponentProcedure() != nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint rollout capture contains another scope or final file recovery",
		)
	}
	if err := taskjournal.ValidateTaskSteps(capture.Steps); err != nil {
		return nil, err
	}
	if len(capture.Steps) != len(plan.Steps) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture step metadata is incomplete")
	}
	for index, step := range plan.Steps {
		record := capture.Steps[index]
		if record.ID != step.StepId {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture step order changed")
		}
		run := step.GetRunScript()
		if run == nil && record.Kind != taskjournal.TaskStepOperation ||
			run != nil && (record.Kind != taskjournal.TaskStepScript || record.ScriptID != run.ScriptId) {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture Script identity changed")
		}
		member := capture.Params[ReleaseHookStepMemberParam(step.StepId)]
		execution := capture.Params[ReleaseHookStepExecutionParam(step.StepId)]
		if run == nil && (member != "" || execution != "") ||
			run != nil && (member != "1" || execution != run.ScriptExecutionId) {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint rollout capture hook binding changed")
		}
	}
	return plan, nil
}

func EncodeBlueprintServiceRolloutCapture(capture BlueprintServiceRolloutCapture) ([]byte, error) {
	if _, err := OpenBlueprintServiceRolloutCapture(capture); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("blueprint-service-rollout", capture)
	if err != nil {
		return nil, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, errs.New(
			errs.KindReleasePlanTooLarge,
			"Blueprint rollout capture exceeds the storage request budget",
		)
	}
	return value, nil
}

func DecodeBlueprintServiceRolloutCapture(value []byte) (BlueprintServiceRolloutCapture, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes {
		return BlueprintServiceRolloutCapture{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint rollout capture size is invalid",
		)
	}
	capture, err := recordcodec.Decode[BlueprintServiceRolloutCapture](value, "blueprint-service-rollout")
	if err != nil {
		return BlueprintServiceRolloutCapture{}, err
	}
	if _, err := OpenBlueprintServiceRolloutCapture(capture); err != nil {
		return BlueprintServiceRolloutCapture{}, err
	}
	return capture, nil
}
