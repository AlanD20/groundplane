package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GroupReleaseMemberCapture struct {
	Preparation DirectReleasePreparation            `json:"preparation"`
	Rollout     releaserender.ServiceRolloutCapture `json:"rollout"`
}

type GroupReleasePreflightCapture struct {
	Sources   scriptsourcequeries.ScriptExecutionSources `json:"sources"`
	Execution scriptexecutions.ScriptExecutionRecord     `json:"execution"`
}

// GroupReleaseCapture is an admission bundle, not an independently executable
// plan. The small Group root retains only immutable member input references.
type GroupReleaseCapture struct {
	Root      releasegroups.ExecutionCapture
	Members   []GroupReleaseMemberCapture
	Preflight []GroupReleasePreflightCapture
}

func (capture *GroupReleaseCapture) Clear() {
	if capture == nil {
		return
	}
	for index := range capture.Members {
		clear(capture.Members[index].Rollout.CandidateArtifact)
	}
	for index := range capture.Preflight {
		capture.Preflight[index].Execution.Clear()
	}
}

func (capture GroupReleaseCapture) Validate() (workexecution.Task, error) {
	task, err := capture.Root.Task()
	if err != nil {
		return workexecution.Task{}, err
	}
	if len(capture.Members) != len(capture.Root.Members) || len(capture.Preflight) != len(capture.Root.Preflight) {
		return workexecution.Task{}, errs.New(errs.KindValidationFailed, "Group admission has incomplete captured members or preflight")
	}
	byID := make(map[string]workexecution.Item, len(capture.Root.Graph))
	for _, item := range capture.Root.Graph {
		byID[item.ID] = item
	}
	for index, member := range capture.Members {
		binding := capture.Root.Policy.Members[index]
		value, reference, err := EncodeDirectReleasePreparation(member.Preparation)
		clear(value)
		if err != nil {
			return workexecution.Task{}, err
		}
		if reference != capture.Root.Members[index].Forward || member.Preparation.GroupID != task.TargetID ||
			member.Preparation.Intent.GroupOperationID != capture.Root.Policy.Manifest.OperationID ||
			member.Preparation.Intent.GroupMemberOrdinal != binding.Member.Ordinal ||
			member.Preparation.Intent.ID != binding.Member.ReleaseID || member.Preparation.Source.ReadRevision != capture.Root.SourceReadRevision {
			return workexecution.Task{}, errs.New(errs.KindStateConflict, "Group admission member differs from its accepted root reference")
		}
		if err := member.Preparation.Matches(task, byID[binding.ForwardID], member.Rollout); err != nil {
			return workexecution.Task{}, err
		}
		phases, err := releaserender.OrderedReleaseHooks(member.Preparation.Intent.OperationKind, binding.Member.ServiceID, member.Rollout.Render.Hooks)
		if err != nil {
			return workexecution.Task{}, err
		}
		hasBefore := len(phases.Before) != 0
		phases.Clear()
		if hasBefore {
			return workexecution.Task{}, errs.New(errs.KindStateConflict, "Group member repeats a hook owned by shared preflight")
		}
	}
	for index, preflight := range capture.Preflight {
		hook := capture.Root.Preflight[index]
		execution := preflight.Execution
		if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.Input != hook.Input ||
			execution.CurrentTaskID != task.ID || execution.WorkItemID != hook.WorkItemID ||
			execution.ScriptID != hook.ScriptID || execution.ServiceID != hook.ServiceID ||
			execution.EnvironmentID != task.Scope.EnvironmentID || execution.ReleaseID != capture.Root.Policy.Members[hook.Ordinal-1].Member.ReleaseID ||
			execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			execution.StartAuthorized || execution.AttemptID != "" || execution.Executor != nil || execution.SourceMembershipCount != 0 ||
			preflight.Sources.Script.Record.Desired.ID != hook.ScriptID || preflight.Sources.Service.Record.Desired.ID != hook.ServiceID ||
			preflight.Sources.Environment.Record.ID != task.Scope.EnvironmentID {
			return workexecution.Task{}, errs.New(errs.KindStateConflict, "Group preflight differs from its captured Script sources")
		}
	}
	return task, nil
}
