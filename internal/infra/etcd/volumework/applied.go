package volumework

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AppliedVolume survives Task/report retention. Desired declarations do not
// prove storage exists; only completion of the captured host operation does.
type AppliedVolume struct {
	VolumeID      string              `json:"volume_id"`
	EnvironmentID string              `json:"environment_id"`
	HostID        string              `json:"host_id"`
	ComposeKey    string              `json:"compose_key"`
	IntentSHA256  string              `json:"intent_sha256"`
	Input         workexecution.Input `json:"input"`
	ObservedAt    time.Time           `json:"observed_at"`
}

func AppliedKey(volumeID string) string { return "/v1/volume-applied/" + volumeID }

func (repository *InputRepository) PrepareVolumeWorkCompletion(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.VolumeWorkInput, at time.Time,
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
	if view.Work.Task.Task.Action == workexecution.ActionApply {
		ledger, err := blueprintunits.NewRepository(repository.store)
		if err != nil {
			keyvalue.ZeroMutationBytes(proof.Mutations)
			return workqueue.Change{}, err
		}
		receipt, err := ledger.PrepareVolumeCompletionReceipt(ctx, view)
		if err != nil {
			keyvalue.ZeroMutationBytes(proof.Mutations)
			return workqueue.Change{}, err
		}
		proof.Conditions = append(proof.Conditions, receipt.Conditions...)
		proof.Mutations = append(proof.Mutations, receipt.Mutations...)
	}
	return proof, nil
}

func (repository *InputRepository) prepareApplied(ctx context.Context, view workqueue.ResumeView, input *agentpb.VolumeWorkInput) (workqueue.Change, error) {
	key := AppliedKey(input.VolumeId)
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
		if saved.VolumeID != input.VolumeId || saved.EnvironmentID != input.EnvironmentId ||
			saved.HostID != view.Attempt.Attempt.Executor.HostID || saved.ComposeKey != input.ComposeKey ||
			saved.ObservedAt.After(view.Checkpoint.Record.RecordedAt) {
			return workqueue.Change{}, conflict("Volume completion would replace differently owned storage")
		}
		revision = value.ModRevision
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: revision}}}
	if input.Action == agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
		return change, nil
	}
	state := AppliedVolume{VolumeID: input.VolumeId, EnvironmentID: input.EnvironmentId,
		HostID: view.Attempt.Attempt.Executor.HostID, ComposeKey: input.ComposeKey,
		IntentSHA256: hex.EncodeToString(input.IntentSha256), Input: view.Attempt.Attempt.Input,
		ObservedAt: view.Checkpoint.Record.RecordedAt}
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

func decodeApplied(raw []byte) (AppliedVolume, error) {
	if len(raw) == 0 || len(raw) > 4096 {
		return AppliedVolume{}, conflict("applied Volume state is invalid")
	}
	state, err := jcs.Decode[AppliedVolume](raw)
	if err != nil || ids.Validate(ids.KindVolume, state.VolumeID) != nil ||
		ids.Validate(ids.KindEnvironment, state.EnvironmentID) != nil || ids.Validate(ids.KindHost, state.HostID) != nil ||
		volumeidentity.ValidateKey(state.ComposeKey) != nil || state.Input.Validate() != nil || state.ObservedAt.IsZero() || len(state.IntentSHA256) != 64 {
		return AppliedVolume{}, conflict("applied Volume state is invalid")
	}
	if _, err := hex.DecodeString(state.IntentSHA256); err != nil {
		return AppliedVolume{}, conflict("applied Volume intent digest is invalid")
	}
	return state, nil
}

func (repository *InputRepository) ReadApplied(ctx context.Context, volumeID string, revision int64) (*AppliedVolume, error) {
	if ids.Validate(ids.KindVolume, volumeID) != nil || revision < 0 {
		return nil, conflict("applied Volume read identity is invalid")
	}
	read, err := repository.read(ctx, []string{AppliedKey(volumeID)}, revision)
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
	if state.VolumeID != volumeID {
		return nil, conflict("applied Volume identity differs from its key")
	}
	return &state, nil
}
