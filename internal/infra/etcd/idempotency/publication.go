package idempotency

import (
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreparePublication keeps a request receipt and its reverse lookup inseparable.
// Native work publishers and direct mutations use the same index encoding and
// absent-key comparisons. The caller joins these writes to resource acceptance.
func PreparePublication(marker IdempotencyMarker) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	markerKey, err := IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return nil, nil, err
	}
	value, err := EncodeIdempotencyMarker(marker)
	if err != nil {
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{{Key: markerKey}}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: markerKey, Value: value}}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ClearMutationValues(mutations)
		}
	}()
	if marker.ReplayTarget != nil {
		key, err := IdempotencyReplayTargetKey(
			*marker.ReplayTarget,
			marker.Locator.Method,
			marker.Locator.Route,
			marker.Locator.Key,
		)
		if err != nil {
			return nil, nil, err
		}
		reference, err := EncodeReplayTargetReference(markerKey)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: key})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: reference})
	}
	if !marker.RetainUntil.IsZero() {
		key, err := IdempotencyRetentionKey(markerKey, marker.RetainUntil)
		if err != nil {
			return nil, nil, err
		}
		reference, err := json.Marshal(RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			return nil, nil, errs.Wrap(errs.KindInternal, err)
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: reference})
	}
	keep = true
	return conditions, mutations, nil
}
