package etcd

import (
	"context"
	"net/netip"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) zoneRemovalDNSClaimConditions(ctx context.Context,
	task TaskRecord, revision int64, at time.Time,
) ([]keyvalue.Condition, bool, error) {
	_, operationID, err := zoneRemovalTaskIdentity(task)
	if err != nil {
		return nil, false, err
	}
	key := environmentchanges.ZoneRemovalIntentKey(operationID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, false, errs.New(errs.KindStateConflict, "Zone removal DNS gate has no captured intent")
	}
	intent, err := environmentchanges.DecodeZoneRemovalIntent(read.Values[0].Value)
	if err != nil {
		return nil, false, err
	}
	if err := validateZoneRemovalTaskOwner(task, intent); err != nil {
		return nil, false, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return nil, false, errs.New(errs.KindStateConflict, "Zone removal DNS gate is no longer pending")
	}
	if intent.Authored == nil && !at.Before(task.CreatedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) {
		return nil, false, nil
	}
	zone, err := projectedZoneRemovalTarget(intent, revision)
	if err != nil {
		return nil, false, err
	}
	conditions, ready, err := repository.zoneDNSWithdrawalConditions(
		ctx,
		zone.Desired,
		revision,
		resolverWithdrawalBeforeEffects,
	)
	if err != nil || !ready {
		return nil, false, err
	}
	return append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}), true, nil
}

// Check every eligible gateway, even when opt-in is now off: an older applied
// resolver can still own that listener. Its address cannot be released on intent.
func (repository *TaskRepository) zoneDNSWithdrawalConditions(ctx context.Context,
	zone core.Zone, revision int64, purpose resolverAcknowledgementPurpose,
) ([]keyvalue.Condition, bool, error) {
	prefix, err := netip.ParsePrefix(zone.Subnet)
	if err != nil {
		return nil, false, errs.New(errs.KindInternal, "Zone DNS withdrawal subnet is invalid")
	}
	if !zone.Internal || !prefix.Addr().IsPrivate() {
		return nil, true, nil
	}
	gateway, err := zonedns.Gateway(zone.Subnet)
	if err != nil {
		return nil, false, err
	}
	ack, ready, err := readResolverStateAcknowledgement(ctx, repository.store, revision, purpose)
	if err != nil || !ready {
		return nil, false, err
	}
	if ack.state.Applied != nil && ack.state.Applied.Desired.Enabled && slices.Contains(ack.state.Applied.ZoneListeners, gateway) {
		return nil, false, nil
	}
	if ack.pendingTaskID != "" {
		// Finalization need not wait on an unrelated resolver Task after claim.
		// Its exact sealed input must also exclude the retiring gateway.
		conditions, safe, err := repository.pendingResolverExcludesGateway(ctx, ack, gateway, revision)
		if err != nil || !safe {
			return nil, false, err
		}
		ack.conditions = append(ack.conditions, conditions...)
	}
	return ack.conditions, true, nil
}

func (repository *TaskRepository) pendingResolverExcludesGateway(_ context.Context, ack resolverAcknowledgement, gateway string, _ int64) ([]keyvalue.Condition, bool, error) {
	for _, captured := range []*resolverwork.CapturedState{ack.state.Active, ack.state.Pending} {
		if captured == nil {
			continue
		}
		if captured.Input.Enabled && slices.Contains(captured.Capture.ZoneListeners, gateway) {
			return nil, false, nil
		}
		// A recovery may restore its previous runtime. Its exact captured predecessor
		// must be the applied state already proven to exclude this retiring gateway.
		if captured.Input.PreviousEnabled && captured.Capture.AppliedRevision != ack.state.AppliedRevision {
			return nil, false, nil
		}
	}
	return ack.state.Conditions, true, nil
}
