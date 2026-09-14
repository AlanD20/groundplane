package tasksecretpins

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

// Abandon resumes cleanup after a Controller restart without needing
// the process-local original input. The durable descriptor is validated, and
// every cleanup transaction still proves Task and active-operation absence.
func (repository *Repository) Abandon(ctx context.Context, operationID string) error {
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return validation("unpublished Secret pin cleanup identity is invalid")
	}
	read, err := repository.read(ctx, []string{PreparationKey(operationID), RootKey(operationID)}, 0)
	if err != nil {
		return err
	}
	if read.Values[1] != nil {
		return conflict("active Secret pin set cannot be abandoned")
	}
	if read.Values[0] == nil {
		return nil
	}
	expected, err := decodeSet(read.Values[0].Value)
	if err != nil || expected.OperationID != operationID {
		return corruption("unpublished Secret pin descriptor is invalid")
	}
	return repository.abandon(ctx, expected)
}

func (repository *Repository) abandon(ctx context.Context, expected setRecord) error {
	operationID := expected.OperationID
	for {
		descriptor, revision, present, err := repository.abandonmentState(ctx, expected)
		if err != nil || !present {
			return err
		}
		guards := abandonmentGuards(descriptor)
		if descriptor.Phase != phaseAbandoning {
			descriptor.Phase = phaseAbandoning
			value, err := encodeSet(descriptor)
			if err != nil {
				return err
			}
			conditions := append([]Condition{{
				Key: PreparationKey(operationID), ModRevision: revision,
			}}, guards...)
			result, err := repository.transact(ctx, conditions, []Mutation{{
				Type: MutationPut, Key: PreparationKey(operationID), Value: value,
			}})
			clear(value)
			if err != nil {
				return err
			}
			if !result.Succeeded {
				return conflict("secret pin abandonment raced Task publication")
			}
			continue
		}
		page, err := repository.rangeValues(ctx, ReversePrefix(operationID), releaseBatchSize)
		if err != nil {
			return err
		}
		if len(page.Values) == 0 {
			if descriptor.ReleaseCursor != descriptor.PreparationCursor {
				return corruption("secret pin abandonment count is inconsistent")
			}
			conditions := append([]Condition{{
				Key: PreparationKey(operationID), ModRevision: revision,
			}, {
				Key: ReversePrefix(operationID), Prefix: true,
			}}, guards...)
			result, err := repository.transact(ctx, conditions, []Mutation{{
				Type: MutationDelete, Key: PreparationKey(operationID),
			}})
			if err != nil {
				return err
			}
			if !result.Succeeded {
				return conflict("secret pin abandonment finalization raced durable state")
			}
			return nil
		}
		if err := repository.abandonPage(ctx, descriptor, revision, guards, page.Values); err != nil {
			return err
		}
	}
}

func (repository *Repository) abandonmentState(
	ctx context.Context,
	expected setRecord,
) (setRecord, int64, bool, error) {
	keys := []string{
		PreparationKey(expected.OperationID), RootKey(expected.OperationID),
		taskKey(expected.TaskID), activeTaskKey(expected.OperationID),
	}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return setRecord{}, 0, false, err
	}
	if read.Values[1] != nil {
		return setRecord{}, 0, false, conflict("active Secret pin set cannot be abandoned")
	}
	if read.Values[2] != nil || read.Values[3] != nil {
		return setRecord{}, 0, false, conflict("secret pin preparation belongs to a published Task")
	}
	if read.Values[0] == nil {
		return setRecord{}, 0, false, nil
	}
	descriptor, err := decodeSet(read.Values[0].Value)
	if err != nil {
		return setRecord{}, 0, false, err
	}
	if !sameSet(descriptor, expected) ||
		(descriptor.Phase != phasePreparing && descriptor.Phase != phaseSealed &&
			descriptor.Phase != phaseAbandoning) {
		return setRecord{}, 0, false, conflict("secret pin abandonment does not own the preparation")
	}
	return descriptor, read.Values[0].ModRevision, true, nil
}

func abandonmentGuards(descriptor setRecord) []Condition {
	return []Condition{
		{Key: RootKey(descriptor.OperationID)},
		{Key: taskKey(descriptor.TaskID)},
		{Key: activeTaskKey(descriptor.OperationID)},
	}
}

func (repository *Repository) abandonPage(
	ctx context.Context,
	descriptor setRecord,
	descriptorRevision int64,
	guards []Condition,
	reverseValues []KeyValue,
) error {
	forwardKeys := make([]string, len(reverseValues))
	for index, reverse := range reverseValues {
		ordinal, err := parseOrdinal(reverse.Key, descriptor.OperationID)
		if err != nil || ordinal != descriptor.ReleaseCursor+uint64(index)+1 ||
			ordinal > descriptor.PreparationCursor {
			return corruption("secret pin abandonment membership order is corrupt")
		}
		pin, err := tasksecretpinrecord.Decode(reverse.Value)
		if err != nil || pin.OperationID != descriptor.OperationID {
			return corruption("secret pin abandonment membership is corrupt")
		}
		forwardKeys[index] = tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
	}
	forward, err := repository.read(ctx, forwardKeys, 0)
	if err != nil {
		return err
	}
	conditions := append([]Condition{{
		Key: PreparationKey(descriptor.OperationID), ModRevision: descriptorRevision,
	}}, guards...)
	mutations := make([]Mutation, 0, len(reverseValues)*2+1)
	for index, reverse := range reverseValues {
		current := forward.Values[index]
		if current == nil || !equalValue(current.Value, reverse.Value) {
			return corruption("secret pin abandonment memberships differ")
		}
		conditions = append(conditions,
			Condition{Key: reverse.Key, ModRevision: reverse.ModRevision},
			Condition{Key: current.Key, ModRevision: current.ModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: reverse.Key},
			Mutation{Type: MutationDelete, Key: current.Key},
		)
	}
	descriptor.ReleaseCursor += uint64(len(reverseValues))
	if descriptor.ReleaseCursor > descriptor.PreparationCursor {
		return corruption("secret pin abandonment cursor overflowed")
	}
	value, err := encodeSet(descriptor)
	if err != nil {
		return err
	}
	defer clear(value)
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: PreparationKey(descriptor.OperationID), Value: value,
	})
	if len(conditions)+len(mutations) > 96 {
		return corruption("secret pin abandonment batch exceeds the transaction ceiling")
	}
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict("secret pin abandonment raced durable state")
	}
	return nil
}
