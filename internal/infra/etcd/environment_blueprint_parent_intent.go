package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintVolumeNeedsCreate selects the Volume's execution mode from the
// acknowledged unit ledger. PublishBlueprintChild repeats this decision and
// fences the same ledger epoch atomically with the hidden Task claim.
func (repository *EnvironmentBlueprintRepository) BlueprintVolumeNeedsCreate(
	ctx context.Context,
	environmentID string,
	parentID string,
	unit blueprintunits.Unit,
) (bool, error) {
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return false, err
	}
	snapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		return false, err
	}
	if snapshot.HeadTaskID != parentID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != parentID ||
		!desiredBlueprintUnitMatches(snapshot.Desired.Record.Units, unit) {
		return false, errs.New(errs.KindStateConflict, "Blueprint Volume desired authority changed")
	}
	selected, err := blueprintunits.Select(snapshot)
	if err != nil {
		return false, err
	}
	ready := false
	for _, target := range selected.Ready {
		if target.Kind == unit.Target.Kind && target.ID == unit.Target.ID {
			ready = true
			break
		}
	}
	if !ready {
		return false, errs.New(errs.KindStateConflict, "Blueprint Volume unit is not ready")
	}
	return blueprintVolumeNeedsCreate(snapshot, unit)
}

// BlueprintParentProtectedIntentSHA256 exposes only the durable digest needed
// to bind a private Volume directory ensure to its visible Apply. Publication
// compares the same marker again in the child-claim transaction.
func (repository *EnvironmentBlueprintRepository) BlueprintParentProtectedIntentSHA256(
	ctx context.Context,
	parentID string,
) (string, error) {
	if repository == nil || repository.HierarchyRepository == nil ||
		ids.Validate(ids.KindTask, parentID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Blueprint parent identity is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(parentID)},
	})
	if err != nil {
		return "", err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return "", errs.New(errs.KindStateConflict, "Blueprint parent is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	parent, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || parent.ID != parentID || parent.Status != taskjournal.TaskStatusRunning ||
		validateBlueprintParentClaimTask(parent) != nil || parent.idempotencyMarker == nil {
		return "", errs.New(errs.KindStateConflict, "Blueprint parent intent authority changed")
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(*parent.idempotencyMarker)
	if err != nil {
		return "", err
	}
	markerRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{markerKey}, Revision: read.ReadRevision,
	})
	if err != nil {
		return "", err
	}
	if markerRead == nil || len(markerRead.Values) != 1 || markerRead.Values[0] == nil {
		return "", errs.New(errs.KindStateConflict, "Blueprint parent intent is unavailable")
	}
	defer keyvalue.ClearValues(markerRead.Values)
	marker, err := idempotency.DecodeIdempotencyMarker(markerRead.Values[0].Value, *parent.idempotencyMarker)
	if err != nil || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending || marker.TaskID != parentID {
		return "", errs.New(errs.KindStateConflict, "Blueprint parent intent authority changed")
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	digest, err := blueprints.ProtectedBlueprintIntentDigest(marker.Intent)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}
