package tasksecretpins

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
)

// AbandonOwned removes at most one preparation batch for cancelled native work.
// Every write compares the caller's stopped-work guards and root absence.
func (repository *Repository) AbandonOwned(ctx context.Context, operationID, taskID, workID string, guards []Condition) (bool, error) {
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		ids.Validate(ids.KindWorkItem, workID) != nil || len(guards) == 0 {
		return false, validation("Secret abandonment requires captured work ownership")
	}
	read, err := repository.read(ctx, []string{PreparationKey(operationID), RootKey(operationID)}, 0)
	if err != nil {
		return false, err
	}
	if read.Values[1] != nil {
		return false, conflict("published Secret pins cannot be abandoned")
	}
	if read.Values[0] == nil {
		return true, nil
	}
	value := read.Values[0]
	descriptor, err := decodeSet(value.Value)
	if err != nil || descriptor.OperationID != operationID || descriptor.TaskID != taskID || descriptor.WorkItemID != workID ||
		(descriptor.Phase != phasePreparing && descriptor.Phase != phaseSealed && descriptor.Phase != phaseAbandoning) {
		return false, conflict("Secret preparation belongs to different work")
	}
	owned := append([]Condition(nil), guards...)
	owned = append(owned, abandonmentGuards(descriptor)...)
	conditions := append([]Condition{{Key: value.Key, ModRevision: value.ModRevision}}, owned...)
	if descriptor.Phase != phaseAbandoning {
		descriptor.Phase = phaseAbandoning
		raw, err := encodeSet(descriptor)
		if err != nil {
			return false, err
		}
		defer clear(raw)
		result, err := repository.transact(ctx, conditions, []Mutation{{Type: MutationPut, Key: value.Key, Value: raw}})
		if err != nil {
			return false, err
		}
		if !result.Succeeded {
			return false, conflict("Secret preparation ownership changed during abandonment")
		}
		return false, nil
	}
	page, err := repository.rangeValues(ctx, ReversePrefix(operationID), releaseBatchSize)
	if err != nil {
		return false, err
	}
	if len(page.Values) != 0 {
		return false, repository.abandonPage(ctx, descriptor, value.ModRevision, owned, page.Values)
	}
	if descriptor.ReleaseCursor != descriptor.PreparationCursor {
		return false, corruption("Secret abandonment count is inconsistent")
	}
	conditions = append(conditions, Condition{Key: ReversePrefix(operationID), Prefix: true})
	result, err := repository.transact(ctx, conditions, []Mutation{{Type: MutationDelete, Key: value.Key}})
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, conflict("Secret preparation ownership changed during abandonment")
	}
	return true, nil
}
