package scriptsourcereference

import "context"

// AbandonOwnedPreparation performs one batch for a stopped owner's exact
// operation. Guards must fence the owner and its preparation descriptor; they
// join every write. No active source root can be abandoned through this path.
func (repository *Repository) AbandonOwnedPreparation(
	ctx context.Context,
	operationID string,
	guards []Condition,
) (bool, error) {
	return repository.abandonOwnedPreparation(ctx, operationID, guards, false)
}

// DiscardUnpublishedPreparation leaves published membership to its release owner.
// The caller must have stopped every producer sharing this operation identity.
func (repository *Repository) DiscardUnpublishedPreparation(
	ctx context.Context,
	operationID string,
	guards []Condition,
) (bool, error) {
	return repository.abandonOwnedPreparation(ctx, operationID, guards, true)
}

func (repository *Repository) abandonOwnedPreparation(
	ctx context.Context,
	operationID string,
	guards []Condition,
	retainPublished bool,
) (bool, error) {
	if ctx == nil || operationID == "" || len(guards) == 0 {
		return false, validation("owned source abandonment requires a fenced owner")
	}
	read, err := repository.store.GetMany(ctx, []string{PreparationKey(operationID), RootKey(operationID)}, 0)
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 2 {
		return false, corruption("source abandonment read is incomplete")
	}
	if read.Values[1] != nil {
		if retainPublished {
			root, err := decodeRoot(read.Values[1].Value)
			if err != nil || root.OperationID != operationID || read.Values[0] != nil {
				return false, corruption("published source membership is inconsistent")
			}
			return true, nil
		}
		return false, conflict("published sources cannot be abandoned")
	}
	if read.Values[0] == nil {
		return true, nil
	}
	value := read.Values[0]
	descriptor, err := decodePreparation(value.Value)
	if err != nil || value.Key != PreparationKey(operationID) || value.ModRevision <= 0 ||
		descriptor.OperationID != operationID || !validRecoverablePreparation(descriptor) {
		return false, corruption("owned source preparation is invalid")
	}
	conditions := append([]Condition(nil), guards...)
	conditions = append(
		conditions,
		Condition{Key: value.Key, ModRevision: value.ModRevision},
		Condition{Key: RootKey(operationID)},
	)
	if descriptor.Phase != PreparationAbandoning {
		descriptor.Phase = PreparationAbandoning
		raw, err := encodePreparation(descriptor)
		if err != nil {
			return false, err
		}
		defer clear(raw)
		_, err = repository.store.Transact(ctx, conditions, []Mutation{{Type: MutationPut, Key: value.Key, Value: raw}})
		return false, err
	}
	page, err := repository.store.Range(ctx, ReversePrefix(operationID), releaseBatchSize)
	if err != nil {
		return false, err
	}
	if page == nil || len(page.Values) > releaseBatchSize {
		return false, corruption("owned source abandonment page is invalid")
	}
	if len(page.Values) != 0 {
		return false, repository.releaseAbandonedPage(ctx, descriptor, value.ModRevision, page.Values, guards)
	}
	if descriptor.ReleaseCursor != descriptor.PreparationCursor {
		return false, corruption("owned source abandonment count is inconsistent")
	}
	conditions = append(conditions, Condition{Key: ReversePrefix(operationID), Prefix: true})
	_, err = repository.store.Transact(ctx, conditions, []Mutation{{Type: MutationDelete, Key: value.Key}})
	return false, err
}
