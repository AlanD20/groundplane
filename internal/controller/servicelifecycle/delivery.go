package servicelifecycle

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BuildWorkInput delivers only captured runtime and hook command authority.
// Decrypted hook values use their separately authorized delivery, never this
// assignment or the Agent's persisted assignment journal.
func BuildWorkInput(input servicelifecyclework.Input) (*agentpb.ServiceLifecycleWorkInput, error) {
	procedure, err := BuildProcedure(input)
	if err != nil {
		return nil, err
	}
	if input.Runtime == nil {
		return nil, errs.New(errs.KindStateConflict, "undeployed Service lifecycle has no Agent work")
	}
	reference, err := input.Reference()
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle capture digest is invalid")
	}
	result := &agentpb.ServiceLifecycleWorkInput{
		CapturedSource: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest},
		EnvironmentId:  input.Scope.EnvironmentID, ServiceId: input.ServiceID,
	}
	switch procedure.Action {
	case workexecution.ActionStart:
		result.Action = agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START
	case workexecution.ActionStop:
		result.Action = agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_STOP
		result.StopGraceSeconds = 30
	case workexecution.ActionDestroy:
		result.Action = agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY
	default:
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle action is invalid")
	}
	for _, member := range procedure.Members {
		result.Members = append(result.Members, &agentpb.ServiceLifecycleMember{Artifact: member.Artifact, Ownership: member.Source})
	}
	if procedure.Hook != nil {
		hook := procedure.Hook
		event := agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP
		if hook.Event == backinghook.AfterStart {
			event = agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START
		}
		result.Hook = &agentpb.ServiceLifecycleHook{StageId: hook.StageID, Event: event,
			Command: append([]string(nil), hook.Definition.Command...), TimeoutSeconds: hook.Definition.TimeoutSeconds}
	}
	if err := executionplan.ValidateServiceLifecycleWorkInput(result); err != nil {
		return nil, err
	}
	return result, nil
}
