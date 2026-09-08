package scriptsourcereference

import (
	"context"
	"math"
)

// Abandon releases exactly the committed prefix of a matching preparation.
// It never adopts refreshed evidence: the caller's canonical set, including
// source keys and staged value digests, must match the durable descriptor.
func (repository *Repository) Abandon(
	ctx context.Context,
	operationID string,
	input []Member,
) error {
	if ctx == nil {
		return validation("source abandonment context is missing")
	}
	_, digest, _, err := canonicalMembers(operationID, input)
	if err != nil {
		return err
	}
	return repository.abandonPreparation(ctx, Preparation{
		OperationID: operationID, MembershipCount: uint64(lenDeduplicated(input)), MembershipSHA256: digest,
	})
}

func (repository *Repository) abandonPreparation(ctx context.Context, expected Preparation) error {
	operationID := expected.OperationID
	for {
		read, readErr := repository.store.GetMany(ctx, []string{PreparationKey(operationID), RootKey(operationID)}, 0)
		if readErr != nil {
			return readErr
		}
		if read == nil || len(read.Values) != 2 {
			return corruption("source abandonment evidence is incomplete")
		}
		if read.Values[1] != nil {
			return conflict("active source set cannot be abandoned")
		}
		if read.Values[0] == nil {
			return nil
		}
		descriptor, decodeErr := decodePreparation(read.Values[0].Value)
		if decodeErr != nil {
			return decodeErr
		}
		if !validRecoverablePreparation(descriptor) {
			return corruption("source preparation descriptor is invalid")
		}
		if !samePreparationSet(descriptor, expected) {
			return conflict("source abandonment evidence does not match the prepared set")
		}
		if descriptor.Phase != PreparationAbandoning {
			if descriptor.Phase != PreparationPreparing && descriptor.Phase != PreparationSealed {
				return corruption("source preparation phase cannot be abandoned")
			}
			descriptor.Phase = PreparationAbandoning
			value, encodeErr := encodePreparation(descriptor)
			if encodeErr != nil {
				return encodeErr
			}
			result, transactErr := repository.store.Transact(
				ctx,
				[]Condition{
					{Key: PreparationKey(operationID), ModRevision: read.Values[0].ModRevision},
					{Key: RootKey(operationID)},
				},
				[]Mutation{{Type: MutationPut, Key: PreparationKey(operationID), Value: value}},
			)
			clear(value)
			if transactErr != nil {
				return transactErr
			}
			if !result.Succeeded {
				continue
			}
			continue
		}
		page, rangeErr := repository.store.Range(ctx, ReversePrefix(operationID), releaseBatchSize)
		if rangeErr != nil {
			return rangeErr
		}
		if page == nil || len(page.Values) > releaseBatchSize {
			return corruption("source abandonment page is invalid")
		}
		if len(page.Values) == 0 {
			if descriptor.ReleaseCursor != descriptor.PreparationCursor {
				return corruption("source abandonment membership count is inconsistent")
			}
			result, transactErr := repository.store.Transact(ctx, []Condition{
				{Key: PreparationKey(operationID), ModRevision: read.Values[0].ModRevision},
				{Key: ReversePrefix(operationID), Prefix: true},
			}, []Mutation{{Type: MutationDelete, Key: PreparationKey(operationID)}})
			if transactErr != nil {
				return transactErr
			}
			if result.Succeeded {
				return nil
			}
			continue
		}
		if err := repository.releaseAbandonedPage(ctx, descriptor, read.Values[0].ModRevision, page.Values); err != nil {
			return err
		}
	}
}

func (repository *Repository) releaseAbandonedPage(
	ctx context.Context,
	descriptor Preparation,
	descriptorRevision int64,
	page []KeyValue,
) error {
	references := make([]Reference, len(page))
	uniqueSources := make([]sourceBatch, 0, len(page))
	sourceIndexes := make(map[string]int)
	bodySources := make([]sourceBatch, 0, len(page))
	bodyIndexes := make(map[string]int)
	lookupKeys := make([]string, 0, len(page)*2)
	for index, reverse := range page {
		reference, err := decodeReference(reverse.Value)
		if err != nil || reference.OperationID != descriptor.OperationID || ReverseKey(reference) != reverse.Key {
			return corruption("source reverse membership is corrupt")
		}
		references[index] = reference
		lookupKeys = append(lookupKeys, ForwardKey(reference))
		suffix := SourceSuffix(reference.Source)
		sourceIndex, exists := sourceIndexes[suffix]
		if !exists {
			sourceIndexes[suffix] = len(uniqueSources)
			uniqueSources = append(uniqueSources, sourceBatch{source: reference.Source})
			sourceIndex = len(uniqueSources) - 1
		}
		uniqueSources[sourceIndex].count++
		if reference.Source.Kind == SourceBody {
			key := ScriptPrimaryKey(reference.Source)
			bodyIndex, found := bodyIndexes[key]
			if !found {
				bodyIndexes[key] = len(bodySources)
				bodySources = append(bodySources, sourceBatch{source: reference.Source})
				bodyIndex = len(bodySources) - 1
			}
			bodySources[bodyIndex].count++
		}
	}
	for _, source := range uniqueSources {
		lookupKeys = append(lookupKeys, CountKey(source.source))
	}
	for _, source := range bodySources {
		lookupKeys = append(lookupKeys, ScriptPrimaryKey(source.source))
	}
	read, err := repository.store.GetMany(ctx, lookupKeys, 0)
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(lookupKeys) {
		return corruption("source abandonment membership evidence is incomplete")
	}
	conditions := []Condition{{Key: PreparationKey(descriptor.OperationID), ModRevision: descriptorRevision}}
	mutations := make([]Mutation, 0, len(page)*2+len(uniqueSources)+len(bodySources)+1)
	defer clearMutations(mutations)
	for index, reverse := range page {
		forward := read.Values[index]
		if forward == nil || !bytesEqual(forward.Value, reverse.Value) {
			return corruption("source forward and reverse memberships differ")
		}
		conditions = append(conditions,
			Condition{Key: reverse.Key, ModRevision: reverse.ModRevision},
			Condition{Key: forward.Key, ModRevision: forward.ModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: reverse.Key}, Mutation{Type: MutationDelete, Key: forward.Key},
		)
	}
	countOffset := len(page)
	for index, source := range uniqueSources {
		current := read.Values[countOffset+index]
		if current == nil {
			return corruption("source count is missing during abandonment")
		}
		count, decodeErr := decodeCount(current.Value)
		if decodeErr != nil || count.Source != source.source || count.ReferencedExecutionCount < source.count {
			return corruption("source count underflows during abandonment")
		}
		conditions = append(conditions, Condition{Key: current.Key, ModRevision: current.ModRevision})
		count.ReferencedExecutionCount -= source.count
		if count.ReferencedExecutionCount == 0 {
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: current.Key})
		} else {
			value, encodeErr := encodeCount(count)
			if encodeErr != nil {
				return encodeErr
			}
			mutations = append(mutations, Mutation{Type: MutationPut, Key: current.Key, Value: value})
		}
	}
	bodyOffset := countOffset + len(uniqueSources)
	for index, source := range bodySources {
		current := read.Values[bodyOffset+index]
		if current == nil || source.count > math.MaxInt64 {
			return corruption("Script source primary is missing during abandonment")
		}
		value, adjustErr := repository.script.AdjustScriptPrimary(current.Value, source.source, -int64(source.count))
		if adjustErr != nil {
			return adjustErr
		}
		conditions = append(conditions, Condition{Key: current.Key, ModRevision: current.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: current.Key, Value: value})
	}
	descriptor.ReleaseCursor += uint64(len(page))
	if descriptor.ReleaseCursor > descriptor.PreparationCursor {
		return corruption("source abandonment cursor overflowed")
	}
	descriptorValue, err := encodePreparation(descriptor)
	if err != nil {
		return err
	}
	mutations = append(
		mutations,
		Mutation{Type: MutationPut, Key: PreparationKey(descriptor.OperationID), Value: descriptorValue},
	)
	if len(page) > releaseBatchSize || len(conditions)+len(mutations) > 96 {
		return corruption("source abandonment batch exceeds transaction ceiling")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict("source abandonment raced durable state")
	}
	return nil
}

func lenDeduplicated(input []Member) int {
	seen := make(map[string]struct{})
	for _, member := range input {
		seen[ReverseKey(member.Reference)] = struct{}{}
	}
	return len(seen)
}
