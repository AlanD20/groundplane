package networkwork

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AppliedNetwork survives Task/report retention. Desired declarations do not
// prove storage exists; only completion of the captured host operation does.
type AppliedNetwork struct {
	NetworkID     string              `json:"network_id"`
	EnvironmentID string              `json:"environment_id"`
	HostID        string              `json:"host_id"`
	DockerName    string              `json:"docker_name"`
	IntentSHA256  string              `json:"intent_sha256"`
	Input         workexecution.Input `json:"input"`
	ObservedAt    time.Time           `json:"observed_at"`
}

func AppliedKey(networkID string) string { return "/v1/network-applied/" + networkID }

func (repository *InputRepository) PrepareNetworkWorkCompletion(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, at time.Time,
) (workqueue.Change, error) {
	proof, err := repository.PrepareCapturedCompletion(ctx, view, input, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := repository.prepareApplied(ctx, view, input)
	if err != nil {
		keyvalue.ZeroMutationBytes(proof.Mutations)
		return workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, change.Conditions...)
	proof.Mutations = append(proof.Mutations, change.Mutations...)
	if view.Work.Task.Task.Action == workexecution.ActionRemove {
		finalization, err := repository.prepareDirectRemovalFinalization(ctx, view, input)
		if err != nil {
			keyvalue.ZeroMutationBytes(proof.Mutations)
			return workqueue.Change{}, err
		}
		proof.Conditions = append(proof.Conditions, finalization.Conditions...)
		proof.Mutations = append(proof.Mutations, finalization.Mutations...)
	}
	if view.Work.Task.Task.Action == workexecution.ActionApply {
		ledger, err := blueprintunits.NewRepository(repository.store)
		if err != nil {
			keyvalue.ZeroMutationBytes(proof.Mutations)
			return workqueue.Change{}, err
		}
		receipt, err := ledger.PrepareNetworkCompletionReceipt(ctx, view, input.Action == agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE)
		if err != nil {
			keyvalue.ZeroMutationBytes(proof.Mutations)
			return workqueue.Change{}, err
		}
		proof.Conditions = append(proof.Conditions, receipt.Conditions...)
		proof.Mutations = append(proof.Mutations, receipt.Mutations...)
	}
	return proof, nil
}

func (repository *InputRepository) prepareApplied(ctx context.Context, view workqueue.ResumeView, input *agentpb.NetworkWorkInput) (workqueue.Change, error) {
	return repository.prepareObservedApplied(ctx, view, input,
		input.Action == agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE, false, view.Checkpoint.Record.RecordedAt)
}

func (repository *InputRepository) prepareObservedApplied(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, absent, preserve bool, observedAt time.Time,
) (workqueue.Change, error) {
	key := AppliedKey(input.NetworkId)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	var revision int64
	if value := read.Values[0]; value != nil {
		saved, err := decodeApplied(value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if saved.NetworkID != input.NetworkId || saved.EnvironmentID != input.EnvironmentId ||
			saved.HostID != view.Attempt.Attempt.Executor.HostID || saved.DockerName != input.DockerName ||
			saved.ObservedAt.After(observedAt) {
			return workqueue.Change{}, conflict("Network completion would replace differently owned storage")
		}
		revision = value.ModRevision
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: revision}}}
	if preserve {
		if absent {
			return workqueue.Change{}, conflict("absent Network cannot preserve a present configuration")
		}
		// A directly created Zone can have been materialized by a Service
		// deployment without a standalone Network receipt. Inspection proves
		// presence and ownership, not its prior configuration. Keep that
		// configuration unrecorded instead of inventing it from removal input.
		return change, nil
	}
	if absent {
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
		return change, nil
	}
	state := AppliedNetwork{NetworkID: input.NetworkId, EnvironmentID: input.EnvironmentId,
		HostID: view.Attempt.Attempt.Executor.HostID, DockerName: input.DockerName,
		IntentSHA256: hex.EncodeToString(input.IntentSha256), Input: view.Attempt.Attempt.Input,
		ObservedAt: observedAt}
	raw, err := json.Marshal(state)
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

func decodeApplied(raw []byte) (AppliedNetwork, error) {
	if len(raw) == 0 || len(raw) > 4096 {
		return AppliedNetwork{}, conflict("applied Network state is invalid")
	}
	state, err := jcs.Decode[AppliedNetwork](raw)
	if err != nil || ids.Validate(ids.KindNetwork, state.NetworkID) != nil ||
		ids.Validate(ids.KindEnvironment, state.EnvironmentID) != nil || ids.Validate(ids.KindHost, state.HostID) != nil ||
		state.Input.Validate() != nil || state.ObservedAt.IsZero() || len(state.IntentSHA256) != 64 {
		return AppliedNetwork{}, conflict("applied Network state is invalid")
	}
	name, nameErr := networkname.New(state.NetworkID)
	if nameErr != nil || name != state.DockerName {
		return AppliedNetwork{}, conflict("applied Network name is not id-derived")
	}
	if _, err := hex.DecodeString(state.IntentSHA256); err != nil {
		return AppliedNetwork{}, conflict("applied Network intent digest is invalid")
	}
	return state, nil
}

func (repository *InputRepository) ReadApplied(ctx context.Context, networkID string, revision int64) (*AppliedNetwork, error) {
	if ids.Validate(ids.KindNetwork, networkID) != nil || revision < 0 {
		return nil, conflict("applied Network read identity is invalid")
	}
	read, err := repository.read(ctx, []string{AppliedKey(networkID)}, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil
	}
	state, err := decodeApplied(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if state.NetworkID != networkID {
		return nil, conflict("applied Network identity differs from its key")
	}
	return &state, nil
}
