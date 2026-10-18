package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

const blueprintHistorySourcePrefix = "/v1/records/environment-blueprint-revisions/"

// Router replacement preparation is durable before its Service children are
// published. Those children still compare the original acknowledged runtime at
// the captured view; a recent-history window alone cannot protect a long Apply.
func blueprintHistoricalReference(key string, value []byte) (*storagemaintenance.Pin, error) {
	if !strings.HasPrefix(key, blueprintHistorySourcePrefix) {
		return nil, nil
	}
	if strings.Contains(key, "/proxy-selections/") {
		record, err := componentplanning.DecodeControllerProxySelection(value)
		if err != nil {
			return nil, err
		}
		return blueprintPreparationPin(record.Preflight.ParentTaskID, record.Preflight.ReadRevision)
	}
	if strings.Contains(key, "/proxy-service-rollouts/") {
		record, err := recordcodec.Decode[BlueprintProxyServiceRollout](value, "blueprint-proxy-service-rollout")
		if err != nil {
			return nil, err
		}
		var floor int64
		for _, predecessor := range record.NativePredecessors {
			if predecessor.FixedReadRevision <= 0 {
				return nil, recordcodec.CorruptRecord()
			}
			if floor == 0 || predecessor.FixedReadRevision < floor {
				floor = predecessor.FixedReadRevision
			}
		}
		if floor != 0 {
			return blueprintPreparationPin(record.Input.ParentTaskID, floor)
		}
	}
	return nil, nil
}

func blueprintPreparationPin(parentID string, revision int64) (*storagemaintenance.Pin, error) {
	if ids.Validate(ids.KindTask, parentID) != nil || revision <= 0 {
		return nil, recordcodec.CorruptRecord()
	}
	return &storagemaintenance.Pin{Kind: "blueprint_proxy_preparation", OwnerID: parentID, Revision: revision}, nil
}

// Sealed execution/recovery reads immutable retained bytes at a fresh view.
// Preparation needs its old view only while the original parent can still
// publish children. Retained metadata must not pin history for ninety days.
func (s *store) historicalReferenceHeld(
	ctx context.Context,
	pin *storagemaintenance.Pin,
	revision int64,
) (bool, error) {
	if pin.Kind != "blueprint_proxy_preparation" {
		return true, nil
	}
	read, err := s.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{workqueue.TaskKey(pin.OwnerID)}, Revision: revision},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil || task.ID != pin.OwnerID {
		return false, recordcodec.CorruptRecord()
	}
	return task.FinishedAt == nil, nil
}
