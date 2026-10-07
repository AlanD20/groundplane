package idempotency

import (
	"context"
	"time"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	idempotencyPruneCursorKey = "/v1/runtime/idempotency-prune-cursor"
	maximumPruneMarkers       = 16
	maximumPruneCASAttempts   = 3
)

type retentionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// PruneGuardInput identifies the terminal marker whose capability-specific
// authority must be checked at the collection snapshot.
type PruneGuardInput struct {
	MarkerKind IdempotencyMarkerKind
	TaskID     string
	Revision   int64
}

// PruneGuardResult either retains the marker or adds capability-specific
// compare fences to its collection transaction.
type PruneGuardResult struct {
	Conditions []etcdstore.Condition
	Retain     bool
}

// PruneGuard supplies the cross-capability authority that marker retention
// cannot derive from the idempotency records themselves.
type PruneGuard func(context.Context, PruneGuardInput) (PruneGuardResult, error)

// RetentionRepository owns bounded collection of terminal idempotency evidence.
type RetentionRepository struct {
	store retentionStore
	guard PruneGuard
}

type pruneScan struct {
	After          string
	CursorRevision int64
	ReadRevision   int64
	More           bool
}

type pruneCursor struct {
	After string `json:"after"`
}

type pruneCandidate struct {
	Marker                  IdempotencyMarker
	MarkerModRevision       int64
	RetentionKey            string
	RetentionValue          []byte
	RetentionModRevision    int64
	ReplayTargetKey         string
	ReplayTargetValue       []byte
	ReplayTargetModRevision int64
}

func NewRetentionRepository(store retentionStore, guard PruneGuard) (*RetentionRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "idempotency retention store is required")
	}
	if guard == nil {
		return nil, errs.New(errs.KindInternal, "idempotency retention guard is required")
	}
	return &RetentionRepository{store: store, guard: guard}, nil
}

// PruneExpired performs one bounded daily collection. It reads retention
// entries and their marker counterparts at one fixed MVCC revision, then
// retries only a known CAS conflict from a fresh snapshot.
func (repository *RetentionRepository) PruneExpired(ctx context.Context, now time.Time) (int, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if repository == nil || repository.store == nil || repository.guard == nil {
		return 0, errs.New(errs.KindInternal, "idempotency retention repository is not initialized")
	}
	if !recordcodec.IsCanonicalUTC(now) {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune time must be UTC")
	}
	for attempt := 0; attempt < maximumPruneCASAttempts; attempt++ {
		candidates, scan, err := repository.collectExpired(ctx, now)
		if err != nil {
			return 0, err
		}
		count, err := repository.pruneRetainedBatch(ctx, now, candidates, scan)
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

func (repository *RetentionRepository) collectExpired(
	ctx context.Context,
	now time.Time,
) ([]pruneCandidate, pruneScan, error) {
	scan, err := repository.loadPruneScan(ctx)
	if err != nil {
		return nil, scan, err
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: IdempotencyRetentionPrefix, StartExclusive: scan.After,
		Limit: maximumPruneMarkers,
	})
	if err != nil {
		return nil, scan, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > maximumPruneMarkers {
		return nil, scan, CorruptIdempotencyMarker()
	}
	scan.ReadRevision, scan.More = page.ReadRevision, page.More
	defer etcdstore.ClearRangeValues(page.Values)
	markerKeys := make([]string, 0, len(page.Values))
	retentionEntries := make([]etcdstore.KeyValue, 0, len(page.Values))
	for _, entry := range page.Values {
		if entry.ModRevision <= 0 {
			return nil, scan, CorruptIdempotencyMarker()
		}
		markerKey, retainUntil, err := ParseIdempotencyRetentionKey(entry.Key)
		if err != nil {
			return nil, scan, err
		}
		if retainUntil.After(now) {
			break
		}
		if err := DecodeRetentionReference(entry.Value, markerKey); err != nil {
			return nil, scan, err
		}
		markerKeys = append(markerKeys, markerKey)
		retentionEntries = append(retentionEntries, entry)
	}
	if len(markerKeys) == 0 {
		return nil, scan, nil
	}
	markers, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: markerKeys, Revision: page.ReadRevision,
	})
	if err != nil {
		return nil, scan, err
	}
	if markers == nil || markers.ReadRevision != page.ReadRevision || len(markers.Values) != len(markerKeys) {
		return nil, scan, CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(markers.Values)
	candidates := make([]pruneCandidate, 0, len(markerKeys))
	for index, markerEntry := range markers.Values {
		if markerEntry == nil || markerEntry.Key != markerKeys[index] || markerEntry.ModRevision <= 0 {
			clearPruneCandidates(candidates)
			return nil, scan, CorruptIdempotencyMarker()
		}
		locator, err := ParseIdempotencyMarkerKey(markerEntry.Key)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		marker, err := DecodeIdempotencyMarker(markerEntry.Value, locator)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		if err := ValidateIdempotencyRetentionKey(
			retentionEntries[index].Key,
			markerEntry.Key,
			marker.RetainUntil,
		); err != nil {
			clear(marker.Intent.Ciphertext)
			clear(marker.Response.Body)
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		candidates = append(candidates, pruneCandidate{
			Marker: marker, MarkerModRevision: markerEntry.ModRevision,
			RetentionKey:         retentionEntries[index].Key,
			RetentionValue:       append([]byte(nil), retentionEntries[index].Value...),
			RetentionModRevision: retentionEntries[index].ModRevision,
		})
	}
	targetKeys := make([]string, 0, len(candidates))
	targetIndexes := make([]int, 0, len(candidates))
	for index := range candidates {
		marker := candidates[index].Marker
		if marker.ReplayTarget == nil {
			continue
		}
		targetKey, err := IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, CorruptIdempotencyMarker()
		}
		targetKeys = append(targetKeys, targetKey)
		targetIndexes = append(targetIndexes, index)
	}
	if len(targetKeys) != 0 {
		targets, err := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: targetKeys, Revision: page.ReadRevision},
		)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		if targets == nil || targets.ReadRevision != page.ReadRevision || len(targets.Values) != len(targetKeys) {
			clearPruneCandidates(candidates)
			return nil, scan, CorruptIdempotencyMarker()
		}
		defer etcdstore.ClearValues(targets.Values)
		for index, targetEntry := range targets.Values {
			candidateIndex := targetIndexes[index]
			markerKey, keyErr := IdempotencyMarkerKey(candidates[candidateIndex].Marker.Locator)
			if keyErr != nil || targetEntry == nil || targetEntry.Key != targetKeys[index] ||
				targetEntry.ModRevision <= 0 || DecodeReplayTargetReference(targetEntry.Value, markerKey) != nil {
				clearPruneCandidates(candidates)
				return nil, scan, CorruptIdempotencyMarker()
			}
			candidates[candidateIndex].ReplayTargetKey = targetEntry.Key
			candidates[candidateIndex].ReplayTargetValue = append([]byte(nil), targetEntry.Value...)
			candidates[candidateIndex].ReplayTargetModRevision = targetEntry.ModRevision
		}
	}
	return candidates, scan, nil
}

func clearPruneCandidates(values []pruneCandidate) {
	for index := range values {
		clear(values[index].Marker.Intent.Ciphertext)
		clear(values[index].Marker.Response.Body)
		clear(values[index].RetentionValue)
		values[index].RetentionValue = nil
		clear(values[index].ReplayTargetValue)
		values[index].ReplayTargetValue = nil
	}
}

func (repository *RetentionRepository) pruneExpired(
	ctx context.Context,
	now time.Time,
	candidates []pruneCandidate,
) (int64, error) {
	return repository.pruneExpiredWithFences(ctx, now, candidates, nil, nil)
}

func (repository *RetentionRepository) pruneExpiredWithFences(
	ctx context.Context, now time.Time, candidates []pruneCandidate,
	extraConditions []etcdstore.Condition, extraMutations []etcdstore.Mutation,
) (int64, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "idempotency retention repository is not initialized")
	}
	if !recordcodec.IsCanonicalUTC(now) {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune time must be UTC")
	}
	if len(candidates) == 0 && len(extraMutations) == 0 || len(candidates) > maximumPruneMarkers {
		return 0, errs.New(errs.KindValidationFailed, "idempotency prune batch must contain 1 through 16 markers")
	}
	conditions := append([]etcdstore.Condition(nil), extraConditions...)
	mutations := append([]etcdstore.Mutation(nil), extraMutations...)
	seenMarkers := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.MarkerModRevision <= 0 || candidate.RetentionModRevision <= 0 ||
			ValidateIdempotencyMarker(candidate.Marker) != nil || candidate.Marker.RetainUntil.After(now) {
			return 0, CorruptIdempotencyMarker()
		}
		markerKey, err := IdempotencyMarkerKey(candidate.Marker.Locator)
		if err != nil {
			return 0, CorruptIdempotencyMarker()
		}
		if _, duplicate := seenMarkers[markerKey]; duplicate {
			return 0, CorruptIdempotencyMarker()
		}
		seenMarkers[markerKey] = struct{}{}
		if err := ValidateIdempotencyRetentionKey(
			candidate.RetentionKey,
			markerKey,
			candidate.Marker.RetainUntil,
		); err != nil {
			return 0, err
		}
		if err := DecodeRetentionReference(candidate.RetentionValue, markerKey); err != nil {
			return 0, err
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: markerKey, ModRevision: candidate.MarkerModRevision},
			etcdstore.Condition{Key: candidate.RetentionKey, ModRevision: candidate.RetentionModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: markerKey},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: candidate.RetentionKey},
		)
		if candidate.Marker.ReplayTarget != nil {
			targetKey, targetErr := IdempotencyReplayTargetKey(
				*candidate.Marker.ReplayTarget,
				candidate.Marker.Locator.Method,
				candidate.Marker.Locator.Route,
				candidate.Marker.Locator.Key,
			)
			if targetErr != nil || candidate.ReplayTargetKey != targetKey ||
				candidate.ReplayTargetModRevision <= 0 ||
				DecodeReplayTargetReference(candidate.ReplayTargetValue, markerKey) != nil {
				return 0, CorruptIdempotencyMarker()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: candidate.ReplayTargetKey, ModRevision: candidate.ReplayTargetModRevision,
			})
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: candidate.ReplayTargetKey},
			)
		} else if candidate.ReplayTargetKey != "" || candidate.ReplayTargetModRevision != 0 ||
			len(candidate.ReplayTargetValue) != 0 {
			return 0, CorruptIdempotencyMarker()
		}
	}
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return 0, errs.New(errs.KindInternal, "idempotency prune transaction exceeds its operation budget")
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

func (repository *RetentionRepository) loadPruneScan(ctx context.Context) (pruneScan, error) {
	read, err := repository.store.Get(ctx, idempotencyPruneCursorKey)
	if err != nil {
		return pruneScan{}, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return pruneScan{}, CorruptIdempotencyMarker()
	}
	if read.Entry == nil {
		return pruneScan{}, nil
	}
	defer clear(read.Entry.Value)
	cursor, err := recordcodec.Decode[pruneCursor](read.Entry.Value, "idempotency_prune_cursor")
	if err != nil || read.Entry.Key != idempotencyPruneCursorKey || read.Entry.ModRevision <= 0 {
		return pruneScan{}, CorruptIdempotencyMarker()
	}
	if _, _, err := ParseIdempotencyRetentionKey(cursor.After); err != nil {
		return pruneScan{}, CorruptIdempotencyMarker()
	}
	return pruneScan{After: cursor.After, CursorRevision: read.Entry.ModRevision}, nil
}

// A durable scan cursor rotates past retained attempts without scanning more
// than the daily 16-entry bound or modifying any marker's retention timestamp.
func (repository *RetentionRepository) pruneRetainedBatch(
	ctx context.Context, now time.Time, candidates []pruneCandidate, scan pruneScan,
) (int, error) {
	guards := make([]PruneGuardResult, len(candidates))
	cursorMode := scan.After != ""
	for index, candidate := range candidates {
		var err error
		guards[index], err = repository.guard(ctx, PruneGuardInput{
			MarkerKind: candidate.Marker.Kind,
			TaskID:     candidate.Marker.TaskID,
			Revision:   scan.ReadRevision,
		})
		if err != nil {
			return 0, err
		}
		cursorMode = cursorMode || guards[index].Retain || len(guards[index].Conditions) != 0
	}
	budget := etcdstore.MaximumOperations
	if cursorMode {
		budget -= 2 // The cursor's compare and put/delete share the pruning commit.
	}
	selected := make([]pruneCandidate, 0, len(candidates))
	conditions := []etcdstore.Condition{}
	last, truncated := "", false
	for index, candidate := range candidates {
		if guards[index].Retain {
			last = candidate.RetentionKey
			continue
		}
		cost := 4 + len(guards[index].Conditions)
		if candidate.Marker.ReplayTarget != nil {
			cost += 2
		}
		if cost > budget {
			truncated = true
			break
		}
		budget -= cost
		selected = append(selected, candidate)
		for _, fence := range guards[index].Conditions {
			var err error
			conditions, err = appendPruneGuardCondition(conditions, fence)
			if err != nil {
				return 0, err
			}
		}
		last = candidate.RetentionKey
	}
	mutations := []etcdstore.Mutation{}
	if cursorMode {
		after := last
		if !truncated && !scan.More {
			after = ""
		}
		if after != scan.After {
			conditions = append(
				conditions,
				etcdstore.Condition{Key: idempotencyPruneCursorKey, ModRevision: scan.CursorRevision},
			)
			mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: idempotencyPruneCursorKey}
			if after != "" {
				value, err := recordcodec.Encode("idempotency_prune_cursor", pruneCursor{After: after})
				if err != nil {
					return 0, err
				}
				defer clear(value)
				mutation.Type, mutation.Value = etcdstore.MutationPut, value
			}
			mutations = append(mutations, mutation)
		}
	}
	if len(selected) == 0 && len(mutations) == 0 {
		return 0, nil
	}
	if _, err := repository.pruneExpiredWithFences(ctx, now, selected, conditions, mutations); err != nil {
		return 0, err
	}
	return len(selected), nil
}

func appendPruneGuardCondition(
	conditions []etcdstore.Condition,
	candidate etcdstore.Condition,
) ([]etcdstore.Condition, error) {
	for _, condition := range conditions {
		if condition.Key == candidate.Key {
			if condition != candidate {
				return nil, errs.New(
					errs.KindStateConflict,
					"Volume removal terminal authority is incomplete or changed",
				)
			}
			return conditions, nil
		}
	}
	return append(conditions, candidate), nil
}
