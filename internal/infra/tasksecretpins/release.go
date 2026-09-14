package tasksecretpins

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

// ReleaseBatch removes at most releaseBatchSize memberships after the caller
// has atomically transitioned the root with BeginRelease. The final batch
// deletes the root only while the operation-owned reverse prefix is absent.
func (repository *Repository) ReleaseBatch(
	ctx context.Context,
	operationID string,
) (bool, bool, error) {
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return false, false, validation("secret pin release input is invalid")
	}
	read, err := repository.read(ctx, []string{RootKey(operationID), PreparationKey(operationID)}, 0)
	if err != nil {
		return false, false, err
	}
	if read.Values[0] == nil {
		return false, false, conflict("secret pin root is unavailable")
	}
	if read.Values[1] != nil {
		return false, false, corruption("secret pin root retained its preparation")
	}
	root, err := decodeSet(read.Values[0].Value)
	if err != nil {
		return false, false, err
	}
	if root.OperationID != operationID || root.Phase != phaseReleasing {
		return false, false, conflict("secret pin root is not releasing")
	}
	page, err := repository.rangeValues(ctx, ReversePrefix(operationID), releaseBatchSize)
	if err != nil {
		return false, false, err
	}
	if len(page.Values) == 0 {
		if root.ReleaseCursor != root.MembershipCount {
			return false, false, corruption("secret pin release count is inconsistent")
		}
		result, err := repository.transact(ctx, []Condition{
			{Key: RootKey(operationID), ModRevision: read.Values[0].ModRevision},
			{Key: ReversePrefix(operationID), Prefix: true},
		}, []Mutation{{Type: MutationDelete, Key: RootKey(operationID)}})
		if err != nil {
			return false, false, err
		}
		if !result.Succeeded {
			return false, false, conflict("secret pin release finalization raced durable state")
		}
		return true, true, nil
	}
	return repository.releasePage(ctx, root, read.Values[0].ModRevision, page.Values)
}

func (repository *Repository) releasePage(
	ctx context.Context,
	root setRecord,
	rootRevision int64,
	reverseValues []KeyValue,
) (bool, bool, error) {
	forwardKeys := make([]string, len(reverseValues))
	for index, reverse := range reverseValues {
		ordinal, err := parseOrdinal(reverse.Key, root.OperationID)
		if err != nil || ordinal != root.ReleaseCursor+uint64(index)+1 || reverse.ModRevision <= 0 {
			return false, false, corruption("secret pin release membership order is corrupt")
		}
		pin, err := tasksecretpinrecord.Decode(reverse.Value)
		if err != nil || pin.OperationID != root.OperationID {
			return false, false, corruption("secret pin reverse membership is corrupt")
		}
		forwardKeys[index] = tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
	}
	forward, err := repository.read(ctx, forwardKeys, 0)
	if err != nil {
		return false, false, err
	}
	conditions := []Condition{{Key: RootKey(root.OperationID), ModRevision: rootRevision}}
	mutations := make([]Mutation, 0, len(reverseValues)*2+1)
	for index, reverse := range reverseValues {
		current := forward.Values[index]
		if current == nil || !equalValue(current.Value, reverse.Value) {
			return false, false, corruption("secret pin forward and reverse memberships differ")
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
	root.ReleaseCursor += uint64(len(reverseValues))
	if root.ReleaseCursor > root.MembershipCount {
		return false, false, corruption("secret pin release cursor overflowed")
	}
	value, err := encodeSet(root)
	if err != nil {
		return false, false, err
	}
	defer clear(value)
	mutations = append(mutations, Mutation{Type: MutationPut, Key: RootKey(root.OperationID), Value: value})
	if len(conditions)+len(mutations) > 96 {
		return false, false, corruption("secret pin release batch exceeds the transaction ceiling")
	}
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return false, false, err
	}
	if !result.Succeeded {
		return false, false, conflict("secret pin release raced durable state")
	}
	return true, false, nil
}
