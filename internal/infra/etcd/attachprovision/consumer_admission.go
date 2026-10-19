package attachprovision

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// consumerAdmission compares the captured runtime before assignment and each
// new forward grant. A direct connection without a runtime retains that absence
// until settlement. A Retry may observe only this work's acknowledged result,
// never silently select a Deploy that completed while the work was queued.
func (repository *InputRepository) consumerAdmission(ctx context.Context, view workqueue.View,
	capture blueprintattachinputs.Generation,
) (bool, []keyvalue.Condition, error) {
	consumer, err := capture.Consumer()
	if err != nil {
		return false, nil, err
	}
	if consumer == nil && view.Task.Task.Action != workexecution.ActionAttach {
		return true, nil, nil
	}
	keys := []string{serviceruntimerecord.Key(capture.ConsumerServiceID), ConsumerSettlementKey(view.Item.Item.ID)}
	if consumer == nil {
		keys = append(
			keys,
			services.ServiceRuntimeKey(capture.ConsumerServiceID),
			releases.ReleaseProjectionKey(capture.ConsumerServiceID),
		)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return false, nil, checkpointConflict("Attach consumer admission snapshot is incomplete")
	}
	guards := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision) {
			return false, nil, recordcodec.CorruptRecord()
		}
		guards = append(guards, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	if consumer == nil {
		if read.Values[1] != nil {
			return false, nil, checkpointConflict(
				"Attach without a consumer transition has an unexpected runtime settlement",
			)
		}
		unapplied, err := unappliedConsumer(capture, read.Values[2], read.Values[3])
		return read.Values[0] == nil && unapplied, guards, err
	}
	if read.Values[0] == nil {
		return false, guards, nil
	}
	expected := *capture.ConsumerRuntime
	if settled := read.Values[1]; settled != nil {
		source, err := recordcodec.Decode[serviceruntimerecord.Acknowledgement](
			settled.Value,
			"attach-consumer-settlement",
		)
		if err != nil || view.Item.Item.Input == nil || source.TaskID != view.Task.Task.ID ||
			source.WorkItemID != view.Item.Item.ID || source.Input != *view.Item.Item.Input || source.Executor.HostID != view.Item.Item.Placement.HostID {
			return false, nil, checkpointConflict("Attach consumer settlement belongs to another work item")
		}
		expected, err = acknowledgedConsumer(capture, consumer.Candidate, source)
		if err != nil {
			return false, nil, err
		}
		defer clear(expected.Runtime.CurrentArtifact)
	} else if read.Values[0].ModRevision != capture.ConsumerRuntimeRevision {
		return false, guards, nil
	}
	raw, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", expected)
	if err != nil {
		return false, nil, err
	}
	defer clear(raw)
	return bytes.Equal(raw, read.Values[0].Value), guards, nil
}
