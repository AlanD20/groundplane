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
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
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
	if ack.observation.Enabled && slices.Contains(ack.input.ZoneListeners, gateway) {
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

func (repository *TaskRepository) pendingResolverExcludesGateway(ctx context.Context,
	ack resolverAcknowledgement, gateway string, revision int64,
) ([]keyvalue.Condition, bool, error) {
	taskID := ack.pendingTaskID
	taskKey := taskjournal.TaskStorageKey(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{taskKey}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, false, errs.New(errs.KindStateConflict, "active CoreDNS Task is unavailable")
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return nil, false, err
	}
	inputKey := platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)
	inputRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{inputKey}, Revision: revision},
	)
	if err != nil {
		return nil, false, err
	}
	if inputRead == nil || inputRead.ReadRevision != revision || len(inputRead.Values) != 1 ||
		inputRead.Values[0] == nil {
		return nil, false, errs.New(errs.KindStateConflict, "active CoreDNS Task input is unavailable")
	}
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(inputRead.Values[0].Value)
	if err != nil {
		return nil, false, err
	}
	if task.ID != taskID || task.Target != ack.observation.ComponentID || input.ComponentID != task.Target ||
		input.PlanID != task.PlanID || input.ExecutionPlanSHA256 != task.PlanHash ||
		input.PredecessorTaskID != ack.observation.TaskID ||
		input.ExpectedPreviousArtifactSHA256 != ack.observation.CorefileSHA256 ||
		!ack.observation.Enabled && input.RollbackComposeArtifact != nil {
		return nil, false, errs.New(errs.KindStateConflict, "active CoreDNS Task input differs from its owner")
	}
	ownerConditions, err := resolverInputTaskOwnerConditions(ctx, repository.store, task, input, revision)
	if err != nil {
		return nil, false, err
	}
	if !input.DisableService && slices.Contains(input.ZoneListeners, gateway) {
		return nil, false, nil
	}
	conditions := []keyvalue.Condition{{Key: taskKey, ModRevision: read.Values[0].ModRevision},
		{Key: inputKey, ModRevision: inputRead.Values[0].ModRevision}}
	return append(conditions, ownerConditions...), true, nil
}
