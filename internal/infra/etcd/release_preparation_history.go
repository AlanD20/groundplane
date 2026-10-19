package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

const releasePreparationHistoryKind = "service_rollout_preparation"

// Preparation can still need fixed-revision resource/hook sources. Once the
// complete executable input is published, the operation reads its immutable
// captures instead; retained diagnostics must not pin MVCC history indefinitely.
func releasePreparationHistoryReference(key string, value []byte) (*storagemaintenance.Pin, error) {
	capture, err := releaserender.DecodeServiceRolloutCapture(value)
	if err != nil {
		return nil, err
	}
	defer clear(capture.CandidateArtifact)
	if key != releaserender.ServiceRolloutKey(capture.Scope.EnvironmentID, capture.TaskID, capture.WorkItemID) {
		return nil, recordcodec.CorruptRecord()
	}
	if capture.Group != nil {
		// The accepted Group root pins every unfinished member through graph
		// publication. A promoted member need not have its item stored yet and
		// must not create a second pin that assumes publication already finished.
		return nil, nil
	}
	return &storagemaintenance.Pin{Kind: releasePreparationHistoryKind,
		OwnerID: capture.WorkItemID, Revision: capture.SourceReadRevision}, nil
}

func (s *store) releasePreparationHistoryHeld(
	ctx context.Context,
	pin *storagemaintenance.Pin,
	revision int64,
) (bool, error) {
	key := workqueue.ItemKey(pin.OwnerID)
	read, err := s.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return false, recordcodec.CorruptRecord()
	}
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil || item.ID != pin.OwnerID || item.Operation != workexecution.Release {
		return false, recordcodec.CorruptRecord()
	}
	return item.Input == nil && item.Phase != workexecution.Settled, nil
}
