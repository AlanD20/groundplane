package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const idempotencyPruneCursorKey = "/v1/runtime/idempotency-prune-cursor"

type idempotencyPruneScan struct {
	After          string
	CursorRevision int64
	ReadRevision   int64
	More           bool
}

type idempotencyPruneCursor struct {
	After string `json:"after"`
}

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

func (repository *IdempotencyRepository) collectExpired(
	ctx context.Context,
	now time.Time,
) ([]idempotencyPruneCandidate, idempotencyPruneScan, error) {
	scan, err := repository.loadPruneScan(ctx)
	if err != nil {
		return nil, scan, err
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: idempotencyrecord.IdempotencyRetentionPrefix, StartExclusive: scan.After,
		Limit: maximumPruneMarkers,
	})
	if err != nil {
		return nil, scan, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > maximumPruneMarkers {
		return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
	}
	scan.ReadRevision, scan.More = page.ReadRevision, page.More
	defer clearKeyValueSlice(page.Values)
	markerKeys := make([]string, 0, len(page.Values))
	retentionEntries := make([]etcdstore.KeyValue, 0, len(page.Values))
	for _, entry := range page.Values {
		if entry.ModRevision <= 0 {
			return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
		}
		markerKey, retainUntil, err := idempotencyrecord.ParseIdempotencyRetentionKey(entry.Key)
		if err != nil {
			return nil, scan, err
		}
		if retainUntil.After(now) {
			break
		}
		if err := idempotencyrecord.DecodeRetentionReference(entry.Value, markerKey); err != nil {
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
		return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(markers.Values)
	candidates := make([]idempotencyPruneCandidate, 0, len(markerKeys))
	for index, markerEntry := range markers.Values {
		if markerEntry == nil || markerEntry.Key != markerKeys[index] || markerEntry.ModRevision <= 0 {
			clearPruneCandidates(candidates)
			return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
		}
		locator, err := idempotencyrecord.ParseIdempotencyMarkerKey(markerEntry.Key)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		marker, err := idempotencyrecord.DecodeIdempotencyMarker(markerEntry.Value, locator)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		if err := idempotencyrecord.ValidateIdempotencyRetentionKey(
			retentionEntries[index].Key,
			markerEntry.Key,
			marker.RetainUntil,
		); err != nil {
			clear(marker.Intent.Ciphertext)
			clear(marker.Response.Body)
			clearPruneCandidates(candidates)
			return nil, scan, err
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
		targetKey, err := idempotencyrecord.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
		}
		targetKeys = append(targetKeys, targetKey)
		targetIndexes = append(targetIndexes, index)
	}
	if len(targetKeys) != 0 {
		targets, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: targetKeys, Revision: page.ReadRevision})
		if err != nil {
			clearPruneCandidates(candidates)
			return nil, scan, err
		}
		if targets == nil || targets.ReadRevision != page.ReadRevision || len(targets.Values) != len(targetKeys) {
			clearPruneCandidates(candidates)
			return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
		}
		defer etcdstore.ClearValues(targets.Values)
		for index, targetEntry := range targets.Values {
			candidateIndex := targetIndexes[index]
			markerKey, keyErr := idempotencyrecord.IdempotencyMarkerKey(candidates[candidateIndex].Marker.marker.Locator)
			if keyErr != nil || targetEntry == nil || targetEntry.Key != targetKeys[index] ||
				targetEntry.ModRevision <= 0 ||
				idempotencyrecord.DecodeReplayTargetReference(targetEntry.Value, markerKey) != nil {
				clearPruneCandidates(candidates)
				return nil, scan, idempotencyrecord.CorruptIdempotencyMarker()
			}
			candidates[candidateIndex].ReplayTargetKey = targetEntry.Key
			candidates[candidateIndex].ReplayTargetValue = append([]byte(nil), targetEntry.Value...)
			candidates[candidateIndex].ReplayTargetModRevision = targetEntry.ModRevision
		}
	}
	return candidates, scan, nil
}

func clearKeyValueSlice(values []etcdstore.KeyValue) {
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
	return repository.pruneExpiredWithFences(ctx, now, candidates, nil, nil)
}

func (repository *IdempotencyRepository) pruneExpiredWithFences(
	ctx context.Context, now time.Time, candidates []idempotencyPruneCandidate,
	extraConditions []etcdstore.Condition, extraMutations []etcdstore.Mutation,
) (int64, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "idempotency context is required")
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "idempotency repository is not initialized")
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
		if candidate.Marker.modRevision <= 0 || candidate.RetentionModRevision <= 0 ||
			idempotencyrecord.ValidateIdempotencyMarker(candidate.Marker.marker) != nil ||
			candidate.Marker.marker.RetainUntil.After(now) {
			return 0, idempotencyrecord.CorruptIdempotencyMarker()
		}
		markerKey, err := idempotencyrecord.IdempotencyMarkerKey(candidate.Marker.marker.Locator)
		if err != nil {
			return 0, idempotencyrecord.CorruptIdempotencyMarker()
		}
		if _, duplicate := seenMarkers[markerKey]; duplicate {
			return 0, idempotencyrecord.CorruptIdempotencyMarker()
		}
		seenMarkers[markerKey] = struct{}{}
		if err := idempotencyrecord.ValidateIdempotencyRetentionKey(
			candidate.RetentionKey,
			markerKey,
			candidate.Marker.marker.RetainUntil,
		); err != nil {
			return 0, err
		}
		if err := idempotencyrecord.DecodeRetentionReference(candidate.RetentionValue, markerKey); err != nil {
			return 0, err
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: markerKey, ModRevision: candidate.Marker.modRevision},
			etcdstore.Condition{Key: candidate.RetentionKey, ModRevision: candidate.RetentionModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: markerKey},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: candidate.RetentionKey},
		)
		if candidate.Marker.marker.ReplayTarget != nil {
			targetKey, targetErr := idempotencyrecord.IdempotencyReplayTargetKey(
				*candidate.Marker.marker.ReplayTarget,
				candidate.Marker.marker.Locator.Method,
				candidate.Marker.marker.Locator.Route,
				candidate.Marker.marker.Locator.Key,
			)
			if targetErr != nil || candidate.ReplayTargetKey != targetKey ||
				candidate.ReplayTargetModRevision <= 0 ||
				idempotencyrecord.DecodeReplayTargetReference(candidate.ReplayTargetValue, markerKey) != nil {
				return 0, idempotencyrecord.CorruptIdempotencyMarker()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: candidate.ReplayTargetKey, ModRevision: candidate.ReplayTargetModRevision,
			})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: candidate.ReplayTargetKey})
		} else if candidate.ReplayTargetKey != "" || candidate.ReplayTargetModRevision != 0 ||
			len(candidate.ReplayTargetValue) != 0 {
			return 0, idempotencyrecord.CorruptIdempotencyMarker()
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

func (repository *IdempotencyRepository) loadPruneScan(ctx context.Context) (idempotencyPruneScan, error) {
	read, err := repository.store.Get(ctx, idempotencyPruneCursorKey)
	if err != nil {
		return idempotencyPruneScan{}, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return idempotencyPruneScan{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	if read.Entry == nil {
		return idempotencyPruneScan{}, nil
	}
	defer clear(read.Entry.Value)
	cursor, err := recordcodec.Decode[idempotencyPruneCursor](read.Entry.Value, "idempotency_prune_cursor")
	if err != nil || read.Entry.Key != idempotencyPruneCursorKey || read.Entry.ModRevision <= 0 {
		return idempotencyPruneScan{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	if _, _, err := idempotencyrecord.ParseIdempotencyRetentionKey(cursor.After); err != nil {
		return idempotencyPruneScan{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	return idempotencyPruneScan{After: cursor.After, CursorRevision: read.Entry.ModRevision}, nil
}

// A durable scan cursor rotates past retained attempts without scanning more
// than the daily 16-entry bound or modifying any marker's retention timestamp.
func (repository *IdempotencyRepository) pruneRetainedBatch(
	ctx context.Context, now time.Time, candidates []idempotencyPruneCandidate, scan idempotencyPruneScan,
) (int, error) {
	guards := make([][]etcdstore.Condition, len(candidates))
	retained := make([]bool, len(candidates))
	cursorMode := scan.After != ""
	for index, candidate := range candidates {
		var err error
		guards[index], retained[index], err = repository.volumeRemovalPruneFences(ctx, candidate, scan.ReadRevision)
		if err != nil {
			return 0, err
		}
		cursorMode = cursorMode || retained[index] || len(guards[index]) != 0
	}
	budget := etcdstore.MaximumOperations
	if cursorMode {
		budget -= 2 // The cursor's compare and put/delete share the pruning commit.
	}
	selected := make([]idempotencyPruneCandidate, 0, len(candidates))
	conditions := []etcdstore.Condition{}
	last, truncated := "", false
	for index, candidate := range candidates {
		if retained[index] {
			last = candidate.RetentionKey
			continue
		}
		cost := 4 + len(guards[index])
		if candidate.Marker.marker.ReplayTarget != nil {
			cost += 2
		}
		if cost > budget {
			truncated = true
			break
		}
		budget -= cost
		selected = append(selected, candidate)
		for _, fence := range guards[index] {
			var err error
			conditions, err = appendVolumeRemovalTerminalCondition(conditions, fence)
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
			conditions = append(conditions, etcdstore.Condition{Key: idempotencyPruneCursorKey, ModRevision: scan.CursorRevision})
			mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: idempotencyPruneCursorKey}
			if after != "" {
				value, err := recordcodec.Encode("idempotency_prune_cursor", idempotencyPruneCursor{After: after})
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

func (repository *IdempotencyRepository) volumeRemovalPruneFences(
	ctx context.Context, candidate idempotencyPruneCandidate, revision int64,
) ([]etcdstore.Condition, bool, error) {
	marker := candidate.Marker.marker
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask {
		return nil, false, nil
	}
	taskRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(marker.TaskID)}, Revision: revision},
	)
	if err != nil {
		return nil, false, err
	}
	if taskRead == nil || taskRead.ReadRevision != revision || len(taskRead.Values) != 1 || taskRead.Values[0] == nil ||
		taskRead.Values[0].Key != taskjournal.TaskStorageKey(marker.TaskID) || taskRead.Values[0].ModRevision <= 0 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(taskRead.Values)
	task, err := DecodeTaskRecord(taskRead.Values[0].Value)
	if err != nil || task.ID != marker.TaskID {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	if task.Type != taskjournal.TaskRemove || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceVolume {
		return nil, false, nil
	}
	root := removalrecord.Root(task.OperationID)
	operation, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: root, Limit: 1, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if operation == nil || operation.ReadRevision != revision || len(operation.Values) > 1 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer clearKeyValueSlice(operation.Values)
	if len(operation.Values) != 0 {
		return nil, true, nil
	}
	rootFences, rootPending, err := repository.volumeRemovalRootPruneFences(ctx, task, revision)
	if err != nil || rootPending {
		return nil, rootPending, err
	}
	keys := []string{removalrecord.OwnerKey(task.Target), removalrecord.EnvironmentLockKey(task.Owner.EnvironmentID)}
	owners, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if owners == nil || owners.ReadRevision != revision || len(owners.Values) != len(keys) {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(owners.Values)
	fences := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRead.Values[0].ModRevision},
		{Key: root, Prefix: true},
	}
	fences = append(fences, rootFences...)
	for index, value := range owners.Values {
		if value != nil {
			owner, err := removalrecord.DecodeOwner(value.Value)
			if err != nil || value.Key != keys[index] || value.ModRevision <= 0 ||
				index == 0 &&
					owner.VolumeID != task.Target || index == 1 && owner.EnvironmentID != task.Owner.EnvironmentID {
				return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
			}
			if owner.OperationID == task.OperationID {
				return nil, true, nil
			}
		}
		fences = append(fences, etcdstore.Condition{Key: keys[index], ModRevision: etcdstore.RevisionOf(value)})
	}
	return fences, false, nil
}

func (repository *IdempotencyRepository) volumeRemovalRootPruneFences(
	ctx context.Context, task TaskRecord, revision int64,
) ([]etcdstore.Condition, bool, error) {
	originID := task.Params[removalrecord.OriginTaskParam]
	if originID == task.ID {
		return nil, false, nil // The candidate's own terminal marker is the root.
	}
	if recordcodec.ValidateID(ids.KindTask, originID) != nil {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(originID)}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(read.Values)
	fences := []etcdstore.Condition{{Key: taskjournal.TaskStorageKey(originID), ModRevision: etcdstore.RevisionOf(read.Values[0])}}
	if read.Values[0] == nil {
		return fences, false, nil // Marker-first GC already removed the original Task.
	}
	origin, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || origin.ID != originID || origin.OperationID != task.OperationID || origin.Owner != task.Owner ||
		origin.idempotencyMarker == nil || read.Values[0].Key != taskjournal.TaskStorageKey(originID) || read.Values[0].ModRevision <= 0 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	key, err := idempotencyrecord.IdempotencyMarkerKey(*origin.idempotencyMarker)
	if err != nil {
		return nil, false, err
	}
	markerRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if markerRead == nil || markerRead.ReadRevision != revision || len(markerRead.Values) != 1 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(markerRead.Values)
	if value := markerRead.Values[0]; value != nil {
		marker, err := idempotencyrecord.DecodeIdempotencyMarker(value.Value, *origin.idempotencyMarker)
		if err != nil || value.Key != key || value.ModRevision <= 0 || marker.TaskID != originID {
			return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
		}
		defer clear(marker.Intent.Ciphertext)
		defer clear(marker.Response.Body)
		if marker.State == idempotencyrecord.IdempotencyMarkerPending {
			return nil, true, nil
		}
	}
	return append(fences, etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(markerRead.Values[0])}), false, nil
}
