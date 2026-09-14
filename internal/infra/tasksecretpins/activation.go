package tasksecretpins

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Activation returns the constant-size root transition that the caller must
// commit atomically with the owning Task publication and its operation CAS.
func Activation(prepared Prepared) (Fragment, error) {
	if err := validatePrepared(prepared); err != nil {
		return Fragment{}, err
	}
	root := prepared.record
	root.Phase = phaseActive
	root.AttemptID = root.TaskID
	value, err := encodeSet(root)
	if err != nil {
		return Fragment{}, err
	}
	return Fragment{
		Conditions: []Condition{
			{Key: PreparationKey(root.OperationID), ModRevision: prepared.revision},
			{Key: RootKey(root.OperationID)},
		},
		Mutations: []Mutation{
			{Type: MutationPut, Key: RootKey(root.OperationID), Value: value},
			{Type: MutationDelete, Key: PreparationKey(root.OperationID)},
		},
	}, nil
}

// RetainForRetry transfers live attempt authority in the same transaction as
// Retry publication. This changes the root revision even if an attempt starts
// and finishes between an expiry reader's checks and its final compare.
func RetainForRetry(active ActiveRoot, taskID string) (Fragment, error) {
	if validateActive(active) != nil || active.record.Phase != phaseActive ||
		ids.Validate(ids.KindTask, taskID) != nil || taskID == active.record.AttemptID {
		return Fragment{}, validation("Secret pin Retry authority is invalid")
	}
	next := active.record
	next.AttemptID = taskID
	value, err := encodeSet(next)
	if err != nil {
		return Fragment{}, err
	}
	return Fragment{
		Conditions: []Condition{{Key: RootKey(next.OperationID), ModRevision: active.revision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(next.OperationID), Value: value}},
	}, nil
}

// LoadActive reconstructs restart-safe operation ownership from the current
// validated root. A missing root is distinct from corrupt or conflicting state.
func (repository *Repository) LoadActive(
	ctx context.Context,
	operationID string,
) (ActiveRoot, bool, error) {
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return ActiveRoot{}, false, validation("active Secret pin root lookup is invalid")
	}
	read, err := repository.read(ctx, []string{RootKey(operationID), PreparationKey(operationID)}, 0)
	if err != nil {
		return ActiveRoot{}, false, err
	}
	if read.Values[0] == nil && read.Values[1] == nil {
		return ActiveRoot{}, false, nil
	}
	if read.Values[0] == nil || read.Values[1] != nil {
		return ActiveRoot{}, false, corruption("active Secret pin root and preparation are inconsistent")
	}
	record, err := decodeSet(read.Values[0].Value)
	if err != nil {
		return ActiveRoot{}, false, err
	}
	root := ActiveRoot{record: record, revision: read.Values[0].ModRevision}
	if record.OperationID != operationID || validateActive(root) != nil {
		return ActiveRoot{}, false, corruption("active Secret pin root identity is invalid")
	}
	return root, true, nil
}

// BeginRelease returns the exact active-root transition that the caller must
// join to proven terminal or retry-expiry authority.
func BeginRelease(active ActiveRoot) (Fragment, error) {
	if err := validateActive(active); err != nil {
		return Fragment{}, err
	}
	if active.record.Phase != phaseActive {
		return Fragment{}, conflict("secret pin root cannot begin release")
	}
	releasing := active.record
	releasing.Phase = phaseReleasing
	value, err := encodeSet(releasing)
	if err != nil {
		return Fragment{}, err
	}
	return Fragment{
		Conditions: []Condition{{Key: RootKey(releasing.OperationID), ModRevision: active.revision},
			{Key: releaseKey(releasing.OperationID)}},
		Mutations: []Mutation{{Type: MutationPut, Key: RootKey(releasing.OperationID), Value: value},
			{Type: MutationPut, Key: releaseKey(releasing.OperationID), Value: []byte(releasing.OperationID)}},
	}, nil
}
