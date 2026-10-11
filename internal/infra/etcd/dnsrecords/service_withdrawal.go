package dnsrecords

import (
	"context"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Only an exact native removal intent can withdraw an otherwise serving named
// target. The comparisons travel with CoreDNS publication, so concurrent removal
// cancellation cannot publish a stale waiting selection.
func (reader *Reader) environmentServiceWithdrawing(ctx context.Context,
	record model.EnvironmentRecord, serviceID string, revision int64,
) (bool, []keyvalue.Condition, error) {
	key := deletions.TombstoneKey(string(deletions.DeletionTargetService), serviceID)
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, nil, errs.New(errs.KindInternal, "DNS Service removal read is incomplete")
	}
	conditions := []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}
	if read.Values[0] == nil {
		return false, conditions, nil
	}
	tombstone, err := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil {
		return false, nil, err
	}
	if tombstone.TargetKind != deletions.DeletionTargetService || tombstone.TargetID != serviceID ||
		tombstone.Phase != deletions.DeletionPhaseHostEffects {
		return false, nil, errs.New(errs.KindStateConflict, "DNS Service removal owner changed")
	}
	intentKey := environmentchanges.ServiceRemovalIntentKey(tombstone.TaskID)
	intentRead, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{intentKey}, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if intentRead == nil || intentRead.ReadRevision != revision || len(intentRead.Values) != 1 ||
		intentRead.Values[0] == nil {
		return false, nil, errs.New(errs.KindStateConflict, "DNS Service removal intent is unavailable")
	}
	intent, err := environmentchanges.DecodeServiceRemovalIntent(intentRead.Values[0].Value)
	if err != nil {
		return false, nil, err
	}
	if intent.TaskID != tombstone.TaskID || intent.EnvironmentID != record.EnvironmentID ||
		intent.ServiceID != serviceID || intent.ServiceName != record.ServiceName ||
		intent.ServiceRevision != tombstone.TargetRevision || intent.Status != taskjournal.TaskStatusPending ||
		!intent.CreatedAt.Equal(tombstone.CreatedAt) {
		return false, nil, errs.New(errs.KindStateConflict, "DNS Service removal intent differs from its target")
	}
	conditions = append(conditions, keyvalue.Condition{Key: intentKey, ModRevision: intentRead.Values[0].ModRevision})
	return true, conditions, nil
}
