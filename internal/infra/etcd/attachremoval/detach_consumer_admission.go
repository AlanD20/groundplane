package attachremoval

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"google.golang.org/protobuf/proto"
)

// DetachConsumerAdmission permits the original captured runtime or this work's
// already-acknowledged consumer change. It cannot overwrite a newer Deploy or
// reinterpret a missing runtime as proof that a captured effect succeeded.
func (repository *InputRepository) DetachConsumerAdmission(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	input, conditions, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	if len(input.Consumer) == 0 {
		return workqueue.Change{Conditions: conditions}, nil
	}
	consumer, err := input.ConsumerTransition(ctx)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clearConsumer(consumer)
	key, runtimeKey := ConsumerSettlementKey(
		input.WorkItemID,
	), serviceruntimerecord.Key(
		input.Snapshot.Source.Attach.ServiceID,
	)
	read, err := repository.read(ctx, []string{key, runtimeKey}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	current := read.Values[1]
	if current == nil {
		return workqueue.Change{}, checkpointConflict("Detach consumer's captured runtime is no longer present")
	}
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])},
		keyvalue.Condition{Key: runtimeKey, ModRevision: current.ModRevision})
	if read.Values[0] == nil {
		if current.ModRevision != input.Snapshot.RuntimeRevision {
			return workqueue.Change{}, checkpointConflict("Detach consumer runtime changed after admission")
		}
		return workqueue.Change{Conditions: conditions}, nil
	}
	receipt, err := recordcodec.Decode[consumerSettlement](read.Values[0].Value, "detach-consumer-settlement")
	if err != nil || view.Item.Item.Input == nil || receipt.Source.TaskID != input.TaskID ||
		receipt.Source.WorkItemID != input.WorkItemID || receipt.Source.Input != *view.Item.Item.Input ||
		receipt.Source.Executor.HostID != view.Item.Item.Placement.HostID ||
		receipt.CandidateSHA256 != hex.EncodeToString(consumer.Candidate.YamlSha256) || input.Snapshot.Runtime == nil {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	expected := *input.Snapshot.Runtime
	expected.Source = receipt.Source
	expected.Runtime.CurrentArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(consumer.Candidate)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(expected.Runtime.CurrentArtifact)
	if serviceruntimerecord.Validate(expected) != nil {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	raw, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", expected)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	if !bytes.Equal(raw, current.Value) {
		return workqueue.Change{}, checkpointConflict("Detach cannot replace a newer consumer runtime")
	}
	return workqueue.Change{Conditions: conditions}, nil
}
