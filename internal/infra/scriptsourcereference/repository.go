package scriptsourcereference

import (
	"context"
	"math"
)

type Repository struct {
	store  Store
	script ScriptPrimaryCodec
}

func NewRepository(store Store, script ScriptPrimaryCodec) (*Repository, error) {
	if store == nil || script == nil {
		return nil, validation("source reference repository dependencies are missing")
	}
	return &Repository{store: store, script: script}, nil
}

func (repository *Repository) Prepare(
	ctx context.Context,
	operationID string,
	input []Member,
) (Prepared, error) {
	if ctx == nil {
		return Prepared{}, validation("source preparation context is missing")
	}
	members, digest, staged, err := canonicalMembers(operationID, input)
	if err != nil {
		return Prepared{}, err
	}
	expected := Preparation{
		OperationID: operationID, MembershipCount: uint64(len(members)), MembershipSHA256: digest,
		Phase: PreparationPreparing,
	}
	descriptor, revision, err := repository.ensurePreparation(ctx, expected)
	if err != nil {
		return Prepared{}, err
	}
	for descriptor.Phase == PreparationPreparing && descriptor.PreparationCursor < descriptor.MembershipCount {
		start := int(descriptor.PreparationCursor)
		end := min(start+preparationBatchSize, len(members))
		descriptor, revision, err = repository.prepareBatch(ctx, descriptor, revision, members[start:end])
		if err != nil {
			return Prepared{}, err
		}
	}
	if descriptor.Phase == PreparationPreparing {
		if descriptor.PreparationCursor != descriptor.MembershipCount || !samePreparationSet(descriptor, expected) {
			return Prepared{}, corruption("source preparation cursor is inconsistent")
		}
		descriptor.Phase = PreparationSealed
		value, encodeErr := encodePreparation(descriptor)
		if encodeErr != nil {
			return Prepared{}, encodeErr
		}
		result, transactErr := repository.store.Transact(ctx,
			[]Condition{{Key: PreparationKey(operationID), ModRevision: revision}},
			[]Mutation{{Type: MutationPut, Key: PreparationKey(operationID), Value: value}},
		)
		clear(value)
		if transactErr != nil {
			return Prepared{}, transactErr
		}
		if !result.Succeeded {
			return Prepared{}, conflict("source preparation seal raced durable state")
		}
		revision = result.Revision
	}
	if descriptor.Phase != PreparationSealed || descriptor.PreparationCursor != descriptor.MembershipCount ||
		!samePreparationSet(descriptor, expected) {
		return Prepared{}, conflict("source preparation descriptor is occupied")
	}
	return Prepared{
		operationID: operationID, descriptorRevision: revision,
		membershipCount: descriptor.MembershipCount, membershipSHA256: descriptor.MembershipSHA256,
		staged: cloneRequirements(staged),
	}, nil
}

func (repository *Repository) ensurePreparation(
	ctx context.Context,
	expected Preparation,
) (Preparation, int64, error) {
	read, err := repository.store.GetMany(
		ctx,
		[]string{PreparationKey(expected.OperationID), RootKey(expected.OperationID)},
		0,
	)
	if err != nil {
		return Preparation{}, 0, err
	}
	if read == nil || len(read.Values) != 2 {
		return Preparation{}, 0, corruption("source preparation evidence is incomplete")
	}
	if read.Values[1] != nil {
		return Preparation{}, 0, conflict("operation source root is already active")
	}
	if read.Values[0] != nil {
		return matchingPreparation(read.Values[0], expected)
	}
	value, err := encodePreparation(expected)
	if err != nil {
		return Preparation{}, 0, err
	}
	result, err := repository.store.Transact(ctx,
		[]Condition{{Key: PreparationKey(expected.OperationID)}, {Key: RootKey(expected.OperationID)}},
		[]Mutation{{Type: MutationPut, Key: PreparationKey(expected.OperationID), Value: value}},
	)
	clear(value)
	if err != nil {
		return Preparation{}, 0, err
	}
	if result.Succeeded {
		return expected, result.Revision, nil
	}
	retry, err := repository.store.GetMany(ctx, []string{PreparationKey(expected.OperationID)}, 0)
	if err != nil || retry == nil || len(retry.Values) != 1 || retry.Values[0] == nil {
		if err != nil {
			return Preparation{}, 0, err
		}
		return Preparation{}, 0, conflict("source preparation identity raced durable state")
	}
	return matchingPreparation(retry.Values[0], expected)
}

func matchingPreparation(value *KeyValue, expected Preparation) (Preparation, int64, error) {
	descriptor, err := decodePreparation(value.Value)
	if err != nil {
		return Preparation{}, 0, err
	}
	if !samePreparationSet(descriptor, expected) || descriptor.ReleaseCursor != 0 ||
		(descriptor.Phase != PreparationPreparing && descriptor.Phase != PreparationSealed) {
		return Preparation{}, 0, conflict("source preparation descriptor is occupied")
	}
	return descriptor, value.ModRevision, nil
}

func samePreparationSet(left, right Preparation) bool {
	return left.OperationID == right.OperationID && left.MembershipCount == right.MembershipCount &&
		left.MembershipSHA256 == right.MembershipSHA256
}

type sourceBatch struct {
	source SourceIdentity
	count  uint64
}

func (repository *Repository) prepareBatch(
	ctx context.Context,
	descriptor Preparation,
	descriptorRevision int64,
	members []Member,
) (Preparation, int64, error) {
	uniqueSources, bodySources := batchSources(members)
	lookupKeys := make([]string, 0, len(uniqueSources)+len(bodySources))
	for _, source := range uniqueSources {
		lookupKeys = append(lookupKeys, CountKey(source.source))
	}
	for _, source := range bodySources {
		lookupKeys = append(lookupKeys, ScriptPrimaryKey(source.source))
	}
	lookups, err := repository.store.GetMany(ctx, lookupKeys, 0)
	if err != nil {
		return Preparation{}, 0, err
	}
	if lookups == nil || len(lookups.Values) != len(lookupKeys) {
		return Preparation{}, 0, corruption("source count evidence is incomplete")
	}
	conditions := []Condition{{Key: PreparationKey(descriptor.OperationID), ModRevision: descriptorRevision}}
	mutations := make([]Mutation, 0, len(members)*2+len(uniqueSources)+len(bodySources)+1)
	defer clearMutations(mutations)
	sourceConditions := make(map[string]int64)
	for _, member := range members {
		if member.Mode == EvidenceExisting {
			revision := member.Reference.SourceModRevision
			if prior, exists := sourceConditions[member.SourceKey]; exists {
				if prior != revision {
					return Preparation{}, 0, validation("source revision evidence conflicts")
				}
			} else {
				sourceConditions[member.SourceKey] = revision
				conditions = append(conditions, Condition{Key: member.SourceKey, ModRevision: revision})
			}
		}
		conditions = append(conditions,
			Condition{Key: ForwardKey(member.Reference)}, Condition{Key: ReverseKey(member.Reference)},
		)
		value, encodeErr := encodeReference(member.Reference)
		if encodeErr != nil {
			return Preparation{}, 0, encodeErr
		}
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: ForwardKey(member.Reference), Value: value},
			Mutation{Type: MutationPut, Key: ReverseKey(member.Reference), Value: append([]byte(nil), value...)},
		)
	}
	for index, source := range uniqueSources {
		// Retirement and count acquisition must exclude each other; immutable
		// source values remain present until deletion finalizes.
		switch source.source.Kind {
		case SourceSecretValue:
			conditions = append(conditions, Condition{Key: "/v1/runtime/deletions/secret/" + source.source.SecretID})
		}
		key := CountKey(source.source)
		current := lookups.Values[index]
		count := Count{Source: source.source}
		condition := Condition{Key: key}
		if current != nil {
			decoded, decodeErr := decodeCount(current.Value)
			if decodeErr != nil || decoded.Source != source.source || decoded.ReferencedExecutionCount == 0 {
				return Preparation{}, 0, corruption("source count is corrupt")
			}
			count = decoded
			condition.ModRevision = current.ModRevision
		}
		if math.MaxUint64-count.ReferencedExecutionCount < source.count {
			return Preparation{}, 0, conflict("source reference count is exhausted")
		}
		count.ReferencedExecutionCount += source.count
		value, encodeErr := encodeCount(count)
		if encodeErr != nil {
			return Preparation{}, 0, encodeErr
		}
		conditions = append(conditions, condition)
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
	}
	for index, source := range bodySources {
		current := lookups.Values[len(uniqueSources)+index]
		if current == nil {
			return Preparation{}, 0, corruption("Script source primary is missing")
		}
		value, adjustErr := repository.script.AdjustScriptPrimary(current.Value, source.source, int64(source.count))
		if adjustErr != nil {
			return Preparation{}, 0, adjustErr
		}
		conditions = append(
			conditions,
			Condition{Key: ScriptPrimaryKey(source.source), ModRevision: current.ModRevision},
		)
		mutations = append(mutations, Mutation{Type: MutationPut, Key: ScriptPrimaryKey(source.source), Value: value})
	}
	descriptor.PreparationCursor += uint64(len(members))
	descriptorValue, err := encodePreparation(descriptor)
	if err != nil {
		return Preparation{}, 0, err
	}
	mutations = append(
		mutations,
		Mutation{Type: MutationPut, Key: PreparationKey(descriptor.OperationID), Value: descriptorValue},
	)
	if len(members) > 16 || len(conditions)+len(mutations) > 96 {
		return Preparation{}, 0, corruption("source preparation batch exceeds transaction ceiling")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Preparation{}, 0, err
	}
	if !result.Succeeded {
		return Preparation{}, 0, conflict("source preparation raced durable state")
	}
	return descriptor, result.Revision, nil
}

func batchSources(members []Member) ([]sourceBatch, []sourceBatch) {
	unique := make([]sourceBatch, 0, len(members))
	indexes := make(map[string]int)
	bodies := make([]sourceBatch, 0, len(members))
	bodyIndexes := make(map[string]int)
	for _, member := range members {
		suffix := SourceSuffix(member.Reference.Source)
		index, exists := indexes[suffix]
		if !exists {
			indexes[suffix] = len(unique)
			unique = append(unique, sourceBatch{source: member.Reference.Source})
			index = len(unique) - 1
		}
		unique[index].count++
		if member.Reference.Source.Kind == SourceBody {
			key := ScriptPrimaryKey(member.Reference.Source)
			bodyIndex, found := bodyIndexes[key]
			if !found {
				bodyIndexes[key] = len(bodies)
				bodies = append(bodies, sourceBatch{source: member.Reference.Source})
				bodyIndex = len(bodies) - 1
			}
			bodies[bodyIndex].count++
		}
	}
	return unique, bodies
}

func cloneRequirements(input []StagedRequirement) []StagedRequirement {
	result := make([]StagedRequirement, len(input))
	for index, requirement := range input {
		result[index] = requirement
		result[index].Stage = requirement.Stage
		result[index].Value = append([]byte(nil), requirement.Value...)
	}
	return result
}
