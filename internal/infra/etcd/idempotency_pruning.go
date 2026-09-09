package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PruneExpired performs one bounded daily collection. It reads retention
// entries and their marker counterparts at one fixed MVCC revision, then
// retries only a known CAS conflict from a fresh snapshot.
func (repository *IdempotencyRepository) PruneExpired(ctx context.Context, now time.Time) (int, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "idempotency repository is not initialized")
	}
	if !validMarkerTime(now) {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune time must be UTC")
	}
	for attempt := 0; attempt < maximumPruneCASAttempts; attempt++ {
		candidates, err := repository.collectExpired(ctx, now)
		if err != nil {
			return 0, err
		}
		if len(candidates) == 0 {
			return 0, nil
		}
		count := len(candidates)
		_, err = repository.pruneExpired(ctx, now, candidates)
		clearPruneCandidates(candidates)
		if err == nil {
			return count, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict {
			return 0, err
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return 0, contextErr
		}
	}
	return 0, errs.New(errs.KindStateConflict, "idempotency prune batch kept changing")
}

func (repository *IdempotencyRepository) collectExpired(
	ctx context.Context,
	now time.Time,
) ([]idempotencyPruneCandidate, error) {
	page, err := repository.store.Range(ctx, RangeRequest{
		Prefix: idempotencyRetentionPrefix,
		Limit:  maximumPruneMarkers,
	})
	if err != nil {
		return nil, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > maximumPruneMarkers {
		return nil, corruptIdempotencyMarker()
	}
	defer clearKeyValueSlice(page.Values)
	markerKeys := make([]string, 0, len(page.Values))
	retentionEntries := make([]KeyValue, 0, len(page.Values))
	for _, entry := range page.Values {
		if entry.ModRevision <= 0 {
			return nil, corruptIdempotencyMarker()
		}
		markerKey, retainUntil, err := parseIdempotencyRetentionKey(entry.Key)
		if err != nil {
			return nil, err
		}
		if retainUntil.After(now) {
			break
		}
		if err := decodeRetentionReference(entry.Value, markerKey); err != nil {
			return nil, err
		}
		markerKeys = append(markerKeys, markerKey)
		retentionEntries = append(retentionEntries, entry)
	}
	if len(markerKeys) == 0 {
		return nil, nil
	}
	markers, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: markerKeys, Revision: page.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if markers == nil || markers.ReadRevision != page.ReadRevision || len(markers.Values) != len(markerKeys) {
		return nil, corruptIdempotencyMarker()
	}
	defer clearKeyValues(markers.Values)
	candidates := make([]idempotencyPruneCandidate, 0, len(markerKeys))
	for index, markerEntry := range markers.Values {
		if markerEntry == nil || markerEntry.Key != markerKeys[index] || markerEntry.ModRevision <= 0 {
			clearPruneCandidates(candidates)
			return nil, corruptIdempotencyMarker()
		}
		locator, err := parseIdempotencyMarkerKey(markerEntry.Key)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, err
		}
		marker, err := decodeIdempotencyMarker(markerEntry.Value, locator)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, err
		}
		if err := validateIdempotencyRetentionKey(
			retentionEntries[index].Key,
			markerEntry.Key,
			marker.RetainUntil,
		); err != nil {
			clear(marker.Intent.Ciphertext)
			clear(marker.Response.Body)
			clearPruneCandidates(candidates)
			return nil, err
		}
		candidates = append(candidates, idempotencyPruneCandidate{
			Marker:               IdempotencyEvidence{marker: marker, modRevision: markerEntry.ModRevision},
			RetentionKey:         retentionEntries[index].Key,
			RetentionValue:       append([]byte(nil), retentionEntries[index].Value...),
			RetentionModRevision: retentionEntries[index].ModRevision,
		})
	}
	targetKeys := make([]string, 0, len(candidates))
	targetIndexes := make([]int, 0, len(candidates))
	for index := range candidates {
		marker := candidates[index].Marker.marker
		if marker.ReplayTarget == nil {
			continue
		}
		targetKey, err := idempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, corruptIdempotencyMarker()
		}
		targetKeys = append(targetKeys, targetKey)
		targetIndexes = append(targetIndexes, index)
	}
	if len(targetKeys) != 0 {
		targets, err := repository.store.GetMany(ctx, GetManyRequest{Keys: targetKeys, Revision: page.ReadRevision})
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, err
		}
		if targets == nil || targets.ReadRevision != page.ReadRevision || len(targets.Values) != len(targetKeys) {
			clearPruneCandidates(candidates)
			return nil, corruptIdempotencyMarker()
		}
		defer clearKeyValues(targets.Values)
		for index, targetEntry := range targets.Values {
			candidateIndex := targetIndexes[index]
			markerKey, keyErr := idempotencyMarkerKey(candidates[candidateIndex].Marker.marker.Locator)
			if keyErr != nil || targetEntry == nil || targetEntry.Key != targetKeys[index] ||
				targetEntry.ModRevision <= 0 ||
				decodeReplayTargetReference(targetEntry.Value, markerKey) != nil {
				clearPruneCandidates(candidates)
				return nil, corruptIdempotencyMarker()
			}
			candidates[candidateIndex].ReplayTargetKey = targetEntry.Key
			candidates[candidateIndex].ReplayTargetValue = append([]byte(nil), targetEntry.Value...)
			candidates[candidateIndex].ReplayTargetModRevision = targetEntry.ModRevision
		}
	}
	return candidates, nil
}

func clearKeyValueSlice(values []KeyValue) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}

func clearPruneCandidates(values []idempotencyPruneCandidate) {
	for index := range values {
		clear(values[index].Marker.marker.Intent.Ciphertext)
		clear(values[index].Marker.marker.Response.Body)
		clear(values[index].RetentionValue)
		values[index].RetentionValue = nil
		clear(values[index].ReplayTargetValue)
		values[index].ReplayTargetValue = nil
	}
}

func (repository *IdempotencyRepository) pruneExpired(
	ctx context.Context,
	now time.Time,
	candidates []idempotencyPruneCandidate,
) (int64, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "idempotency repository is not initialized")
	}
	if !validMarkerTime(now) {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune time must be UTC")
	}
	if len(candidates) == 0 || len(candidates) > maximumPruneMarkers {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune batch must contain 1 through 16 markers")
	}
	conditions := make([]Condition, 0, len(candidates)*2)
	mutations := make([]Mutation, 0, len(candidates)*2)
	seenMarkers := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.Marker.modRevision <= 0 || candidate.RetentionModRevision <= 0 ||
			validateIdempotencyMarker(candidate.Marker.marker) != nil ||
			candidate.Marker.marker.RetainUntil.After(now) {
			return 0, corruptIdempotencyMarker()
		}
		markerKey, err := idempotencyMarkerKey(candidate.Marker.marker.Locator)
		if err != nil {
			return 0, corruptIdempotencyMarker()
		}
		if _, duplicate := seenMarkers[markerKey]; duplicate {
			return 0, corruptIdempotencyMarker()
		}
		seenMarkers[markerKey] = struct{}{}
		if err := validateIdempotencyRetentionKey(
			candidate.RetentionKey,
			markerKey,
			candidate.Marker.marker.RetainUntil,
		); err != nil {
			return 0, err
		}
		if err := decodeRetentionReference(candidate.RetentionValue, markerKey); err != nil {
			return 0, err
		}
		conditions = append(conditions,
			Condition{Key: markerKey, ModRevision: candidate.Marker.modRevision},
			Condition{Key: candidate.RetentionKey, ModRevision: candidate.RetentionModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: markerKey},
			Mutation{Type: MutationDelete, Key: candidate.RetentionKey},
		)
		if candidate.Marker.marker.ReplayTarget != nil {
			targetKey, targetErr := idempotencyReplayTargetKey(
				*candidate.Marker.marker.ReplayTarget,
				candidate.Marker.marker.Locator.Method,
				candidate.Marker.marker.Locator.Route,
				candidate.Marker.marker.Locator.Key,
			)
			if targetErr != nil || candidate.ReplayTargetKey != targetKey ||
				candidate.ReplayTargetModRevision <= 0 ||
				decodeReplayTargetReference(candidate.ReplayTargetValue, markerKey) != nil {
				return 0, corruptIdempotencyMarker()
			}
			conditions = append(conditions, Condition{
				Key: candidate.ReplayTargetKey, ModRevision: candidate.ReplayTargetModRevision,
			})
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: candidate.ReplayTargetKey})
		} else if candidate.ReplayTargetKey != "" || candidate.ReplayTargetModRevision != 0 ||
			len(candidate.ReplayTargetValue) != 0 {
			return 0, corruptIdempotencyMarker()
		}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return result.Revision, errs.New(errs.KindStateConflict, "idempotency prune batch changed")
	}
	return result.Revision, nil
}
