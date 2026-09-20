package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateTaskLifecycleCompanions(task TaskRecord, activeValue *etcdstore.KeyValue, markerValue *etcdstore.KeyValue) error {
	activeTaskID, err := decodeTaskReference(activeValue.Value)
	if err != nil || activeTaskID != task.ID {
		return errs.New(errs.KindInternal, "active-operation record does not match its Task")
	}
	marker, err := decodeIdempotencyMarker(markerValue.Value, *task.idempotencyMarker)
	if err != nil {
		return err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID {
		return errs.New(errs.KindInternal, "task idempotency marker is not pending for its Task")
	}
	return nil
}

func hydrateTerminalTaskMarker(
	prepared IdempotencyMarker,
	persisted []byte,
) (IdempotencyMarker, error) {
	existing, err := decodeIdempotencyMarker(persisted, prepared.Locator)
	if err != nil {
		return IdempotencyMarker{}, err
	}
	prepared.Intent = existing.Intent
	prepared.Response = existing.Response
	prepared.CreatedAt = existing.CreatedAt
	prepared.ReplayTarget = cloneIdempotencyReplayTarget(existing.ReplayTarget)
	return prepared, nil
}
