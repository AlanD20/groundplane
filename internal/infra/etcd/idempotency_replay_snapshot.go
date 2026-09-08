package etcd

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *IdempotencyRepository) Read(
	ctx context.Context,
	locator IdempotencyLocator,
) (*IdempotencyEvidence, error) {
	evidence, _, err := repository.ReadWithRevision(ctx, locator)
	return evidence, err
}

// ReadWithRevision returns current marker evidence and the exact MVCC snapshot
// used to observe it. Callers can resolve related replay indexes at that same
// revision without misclassifying an atomic publication race as corruption.
func (repository *IdempotencyRepository) ReadWithRevision(
	ctx context.Context,
	locator IdempotencyLocator,
) (*IdempotencyEvidence, int64, error) {
	if ctx == nil {
		return nil, 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	key, err := idempotencyMarkerKey(locator)
	if err != nil {
		return nil, 0, err
	}
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return nil, 0, errs.New(errs.KindInternal, "idempotency read result is missing")
	}
	if result.Entry == nil {
		return nil, result.ReadRevision, nil
	}
	evidence, err := repository.readEvidenceAtRevision(ctx, locator, result.ReadRevision)
	return evidence, result.ReadRevision, err
}

// ReadAtRevision returns marker evidence from one fixed MVCC snapshot. The
// revision is supplied by a durable reverse-index read so a replay cannot
// combine a marker with a later task transition.
func (repository *IdempotencyRepository) ReadAtRevision(
	ctx context.Context,
	locator IdempotencyLocator,
	revision int64,
) (*IdempotencyEvidence, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "idempotency revision is invalid")
	}
	return repository.readEvidenceAtRevision(ctx, locator, revision)
}

func (repository *IdempotencyRepository) readEvidenceAtRevision(
	ctx context.Context,
	locator IdempotencyLocator,
	revision int64,
) (*IdempotencyEvidence, error) {
	if err := validateIdempotencyLocator(locator); err != nil {
		return nil, err
	}
	if revision <= 0 {
		return nil, corruptIdempotencyMarker()
	}
	key, err := idempotencyMarkerKey(locator)
	if err != nil {
		return nil, err
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
		return nil, corruptIdempotencyMarker()
	}
	if result.Values[0] == nil {
		return nil, nil
	}
	defer clear(result.Values[0].Value)
	marker, err := decodeIdempotencyMarker(result.Values[0].Value, locator)
	if err != nil {
		return nil, err
	}
	if result.Values[0].ModRevision <= 0 {
		return nil, corruptIdempotencyMarker()
	}
	return &IdempotencyEvidence{marker: marker, modRevision: result.Values[0].ModRevision}, nil
}

func (repository *IdempotencyRepository) ResolveReplayLocator(
	ctx context.Context,
	target IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (IdempotencyLocator, bool, error) {
	locator, _, found, err := repository.ResolveReplayLocatorAtRevision(ctx, target, method, route, key)
	return locator, found, err
}

// ResolveReplayLocatorAtRevision resolves the reverse index and its marker at
// the index's read revision. No target-primary fallback is permitted.
func (repository *IdempotencyRepository) ResolveReplayLocatorAtRevision(
	ctx context.Context,
	target IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (IdempotencyLocator, int64, bool, error) {
	if ctx == nil {
		return IdempotencyLocator{}, 0, false, errs.New(errs.KindInternal, "idempotency context is required")
	}
	targetKey, err := idempotencyReplayTargetKey(target, method, route, key)
	if err != nil {
		return IdempotencyLocator{}, 0, false, err
	}
	result, err := repository.store.Get(ctx, targetKey)
	if err != nil {
		return IdempotencyLocator{}, 0, false, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return IdempotencyLocator{}, 0, false, errs.New(
			errs.KindInternal,
			"idempotency replay lookup result is missing",
		)
	}
	if result.Entry == nil {
		return IdempotencyLocator{}, 0, false, nil
	}
	defer clear(result.Entry.Value)
	if result.Entry.Key != targetKey || result.Entry.ModRevision <= 0 {
		return IdempotencyLocator{}, 0, false, corruptIdempotencyMarker()
	}
	var reference replayTargetReferenceJSON
	decoder := json.NewDecoder(bytes.NewReader(result.Entry.Value))
	decoder.DisallowUnknownFields()
	if rejectDuplicateJSONFields(result.Entry.Value) != nil || decoder.Decode(&reference) != nil ||
		requireJSONEOF(decoder) != nil || reference.Schema != 1 {
		return IdempotencyLocator{}, 0, false, corruptIdempotencyMarker()
	}
	locator, err := parseIdempotencyMarkerKey(reference.MarkerKey)
	if err != nil || locator.Method != method || locator.Route != route || locator.Key != key {
		return IdempotencyLocator{}, 0, false, corruptIdempotencyMarker()
	}
	markers, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{reference.MarkerKey}, Revision: result.ReadRevision,
	})
	if err != nil {
		return IdempotencyLocator{}, 0, false, err
	}
	if markers == nil || markers.ReadRevision != result.ReadRevision || len(markers.Values) != 1 ||
		markers.Values[0] == nil {
		return IdempotencyLocator{}, 0, false, corruptIdempotencyMarker()
	}
	defer clear(markers.Values[0].Value)
	marker, err := decodeIdempotencyMarker(markers.Values[0].Value, locator)
	if err != nil {
		return IdempotencyLocator{}, 0, false, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.ReplayTarget == nil || *marker.ReplayTarget != target ||
		decodeReplayTargetReference(result.Entry.Value, reference.MarkerKey) != nil {
		return IdempotencyLocator{}, 0, false, corruptIdempotencyMarker()
	}
	return locator, result.ReadRevision, true, nil
}

// ResolveReplayLocatorAtSnapshot resolves both reverse index and marker from
// one caller-selected MVCC snapshot.
func (repository *IdempotencyRepository) ResolveReplayLocatorAtSnapshot(
	ctx context.Context,
	target IdempotencyReplayTarget,
	method string,
	route string,
	key string,
	revision int64,
) (IdempotencyLocator, bool, error) {
	if ctx == nil {
		return IdempotencyLocator{}, false, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if revision <= 0 {
		return IdempotencyLocator{}, false, errs.New(errs.KindValidationFailed, "idempotency revision is invalid")
	}
	targetKey, err := idempotencyReplayTargetKey(target, method, route, key)
	if err != nil {
		return IdempotencyLocator{}, false, err
	}
	targets, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{targetKey}, Revision: revision})
	if err != nil {
		return IdempotencyLocator{}, false, err
	}
	if targets == nil || targets.ReadRevision != revision || len(targets.Values) != 1 {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	if targets.Values[0] == nil {
		return IdempotencyLocator{}, false, nil
	}
	defer clear(targets.Values[0].Value)
	if targets.Values[0].Key != targetKey || targets.Values[0].ModRevision <= 0 {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	var reference replayTargetReferenceJSON
	decoder := json.NewDecoder(bytes.NewReader(targets.Values[0].Value))
	decoder.DisallowUnknownFields()
	if rejectDuplicateJSONFields(targets.Values[0].Value) != nil || decoder.Decode(&reference) != nil ||
		requireJSONEOF(decoder) != nil || reference.Schema != 1 {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	locator, err := parseIdempotencyMarkerKey(reference.MarkerKey)
	if err != nil || locator.Method != method || locator.Route != route || locator.Key != key {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	markers, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{reference.MarkerKey}, Revision: revision,
	})
	if err != nil {
		return IdempotencyLocator{}, false, err
	}
	if markers == nil || markers.ReadRevision != revision || len(markers.Values) != 1 || markers.Values[0] == nil {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	defer clear(markers.Values[0].Value)
	marker, err := decodeIdempotencyMarker(markers.Values[0].Value, locator)
	if err != nil {
		return IdempotencyLocator{}, false, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.ReplayTarget == nil || *marker.ReplayTarget != target ||
		decodeReplayTargetReference(targets.Values[0].Value, reference.MarkerKey) != nil {
		return IdempotencyLocator{}, false, corruptIdempotencyMarker()
	}
	return locator, true, nil
}

// Revision is the marker's durable MVCC revision.
func (evidence *IdempotencyEvidence) Revision() int64 {
	if evidence == nil {
		return 0
	}
	return evidence.modRevision
}
