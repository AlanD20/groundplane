package networkwork

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AppliedRemoteNetwork is a durable receipt for the exact Host that observed
// an isolated bridge. Neither the local applied key nor a different Host's
// receipt can prove destination readiness.
type AppliedRemoteNetwork struct {
	AppliedNetwork
	Executor   workexecution.Identity `json:"executor"`
	TaskID     string                 `json:"task_id"`
	WorkItemID string                 `json:"work_item_id"`
	AttemptID  string                 `json:"attempt_id"`
	Evidence   workexecution.Input    `json:"evidence"`
}

func RemoteAppliedKey(hostID, networkID string) string {
	return "/v1/remote-network-applied/" + hostID + "/" + networkID
}

// appliedNetworkHost classifies an already selected Host. It never selects a
// placement or falls back from an unenrolled Host to the local installation.
func (repository *InputRepository) appliedNetworkHost(ctx context.Context, executor workexecution.Identity,
	revision int64,
) (bool, []keyvalue.Condition, error) {
	if executor.Validate() != nil || executor.Executor != workexecution.Agent || revision <= 0 {
		return false, nil, conflict("Network observation requires a Host-bound Agent attempt")
	}
	local, err := hostbindings.ReadLocalIdentity(ctx, repository.store, revision)
	if err != nil {
		return false, nil, err
	}
	guards := []keyvalue.Condition{{Key: hostbindings.LocalIdentityKey(), ModRevision: local.Revision}}
	if local.Record == executor.HostID {
		return false, guards, nil
	}
	binding, err := hostbindings.Read(ctx, repository.store, executor.AgentID, revision)
	if err != nil {
		return false, nil, err
	}
	if binding.Binding.HostID != executor.HostID {
		return false, nil, conflict("Network observation does not match the Agent's Host")
	}
	hostKey := remotehosts.Key(executor.HostID)
	read, err := repository.read(ctx, []string{hostKey}, revision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return false, nil, conflict("Network observation has no active remote Host enrollment")
	}
	host, err := remotehosts.DecodeHost(executor.HostID, value.Value)
	if err != nil {
		return false, nil, err
	}
	if host.Agent == nil || host.Agent.AgentID != executor.AgentID {
		return false, nil, conflict("Network observation is outside the active remote Agent enrollment")
	}
	guards = append(guards, binding.Conditions...)
	return true, append(guards, keyvalue.Condition{Key: hostKey, ModRevision: value.ModRevision}), nil
}

func (repository *InputRepository) prepareRemoteApplied(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, absent, preserve bool, observedAt time.Time, evidence workexecution.Input,
	guards []keyvalue.Condition,
) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if task.Action != workexecution.ActionDeploy || ids.Validate(ids.KindService, task.TargetID) != nil ||
		task.Scope.Validate() != nil || task.Scope.TenantID == "" || task.Scope.EnvironmentID != input.EnvironmentId ||
		task.ID != item.TaskID || item.Operation != workexecution.Network || item.TargetID != input.NetworkId ||
		!view.ActiveAttempt || !view.CanSettleResult() || workexecution.ValidateDelivery(item, attempt) != nil ||
		input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE || preserve ||
		absent && attempt.Direction != workexecution.Recovery {
		return workqueue.Change{}, conflict("remote Network observation requires captured direct Deploy ENSURE work")
	}
	expected := AppliedRemoteNetwork{
		AppliedNetwork: AppliedNetwork{NetworkID: input.NetworkId, EnvironmentID: input.EnvironmentId,
			HostID: attempt.Executor.HostID, DockerName: input.DockerName, Input: attempt.Input,
			IntentSHA256: hex.EncodeToString(input.IntentSha256), ObservedAt: observedAt},
		Executor: attempt.Executor, TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID,
		AttemptID: attempt.ID, Evidence: evidence,
	}
	if err := expected.validate(); err != nil {
		return workqueue.Change{}, err
	}
	key := RemoteAppliedKey(expected.HostID, expected.NetworkID)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value != nil {
		saved, err := decodeRemoteApplied(value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if saved.HostID != expected.HostID || saved.EnvironmentID != expected.EnvironmentID ||
			saved.NetworkID != expected.NetworkID || saved.DockerName != expected.DockerName ||
			saved.Executor.Generation > expected.Executor.Generation ||
			saved.Executor.Generation == expected.Executor.Generation && saved.Executor != expected.Executor ||
			saved.ObservedAt.After(observedAt) {
			return workqueue.Change{}, conflict("remote Network completion would replace differently owned storage")
		}
	}
	change := workqueue.Change{
		Conditions: append(guards, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)}),
	}
	if absent {
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
		return change, nil
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		return workqueue.Change{}, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: canonical}}
	return change, nil
}

func (record AppliedRemoteNetwork) validate() error {
	if err := validateApplied(record.AppliedNetwork); err != nil {
		return err
	}
	if record.Executor.Validate() != nil || record.Executor.Executor != workexecution.Agent ||
		record.Executor.HostID != record.HostID || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(
			ids.KindWorkItem,
			record.WorkItemID,
		) != nil || ids.Validate(ids.KindAttempt, record.AttemptID) != nil ||
		record.Evidence.Validate() != nil {
		return conflict("remote Network receipt lacks its authenticated Agent observation")
	}
	return nil
}

func decodeRemoteApplied(raw []byte) (AppliedRemoteNetwork, error) {
	if len(raw) == 0 || len(raw) > 4096 {
		return AppliedRemoteNetwork{}, conflict("remote applied Network state is invalid")
	}
	record, err := jcs.Decode[AppliedRemoteNetwork](raw)
	if err != nil {
		return AppliedRemoteNetwork{}, err
	}
	return record, record.validate()
}

// ReadRemoteApplied selects only this destination and the exact currently
// registered Agent generation at the caller's fixed storage revision.
func (repository *InputRepository) ReadRemoteApplied(ctx context.Context, executor workexecution.Identity,
	environmentID, networkID string, revision int64,
) (keyvalue.Versioned[AppliedRemoteNetwork], []keyvalue.Condition, error) {
	var result keyvalue.Versioned[AppliedRemoteNetwork]
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindNetwork, networkID) != nil {
		return result, nil, conflict("remote Network observation scope is invalid")
	}
	remote, guards, err := repository.appliedNetworkHost(ctx, executor, revision)
	if err != nil {
		return result, nil, err
	}
	if !remote {
		return result, nil, conflict("destination Network observation requires an enrolled remote Host")
	}
	key, executorKey := RemoteAppliedKey(executor.HostID, networkID), workqueue.ExecutorKey(executor)
	read, err := repository.read(ctx, []string{key, executorKey}, revision)
	if err != nil {
		return result, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	current := read.Values[1]
	if current == nil {
		return result, nil, conflict("destination Network observation has no registered executor")
	}
	registered, err := workqueue.DecodeExecutor(current.Value)
	if err != nil {
		return result, nil, err
	}
	if registered.Identity != executor {
		return result, nil, conflict("destination Network executor generation changed")
	}
	value := read.Values[0]
	guards = append(guards, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)},
		keyvalue.Condition{Key: executorKey, ModRevision: current.ModRevision})
	result.ReadRevision = revision
	if value == nil {
		return result, guards, nil
	}
	record, err := decodeRemoteApplied(value.Value)
	if err != nil {
		return result, nil, err
	}
	if record.Executor != executor || record.EnvironmentID != environmentID || record.NetworkID != networkID {
		return result, nil, conflict("destination Network observation differs from its Host, Environment, or Agent")
	}
	result.Record, result.Revision = record, value.ModRevision
	return result, guards, nil
}
