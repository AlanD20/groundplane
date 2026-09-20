package etcd

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateTaskLifecycleCompanions(task TaskRecord, activeValue *etcdstore.KeyValue, markerValue *etcdstore.KeyValue) error {
	activeTaskID, err := idempotencyrecord.DecodeTaskReference(activeValue.Value)
	if err != nil || activeTaskID != task.ID {
		return errs.New(errs.KindInternal, "active-operation record does not match its Task")
	}
	marker, err := idempotencyrecord.DecodeIdempotencyMarker(markerValue.Value, *task.idempotencyMarker)
	if err != nil {
		return err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID {
		return errs.New(errs.KindInternal, "task idempotency marker is not pending for its Task")
	}
	return nil
}

func hydrateTerminalTaskMarker(
	prepared idempotencyrecord.IdempotencyMarker,
	persisted []byte,
) (idempotencyrecord.IdempotencyMarker, error) {
	existing, err := idempotencyrecord.DecodeIdempotencyMarker(persisted, prepared.Locator)
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, err
	}
	prepared.Intent = existing.Intent
	prepared.Response = existing.Response
	prepared.CreatedAt = existing.CreatedAt
	prepared.ReplayTarget = idempotencyrecord.CloneIdempotencyReplayTarget(existing.ReplayTarget)
	return prepared, nil
}
