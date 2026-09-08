package scriptsourcereference

import (
	"context"
	"time"
)

// PrepareRetryAvailable binds retry retention to the caller's fixed-revision
// root. The lifecycle owner must join this fragment with terminal Task,
// retention, assignment, and durable not-started execution comparisons.
func (repository *Repository) PrepareRetryAvailable(
	ctx context.Context, operationID string, expectedRevision int64, expiresAt time.Time,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" || expectedRevision <= 0 || expiresAt.IsZero() {
		return ReleaseFragment{}, validation("source retry availability input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].ModRevision != expectedRevision {
		return ReleaseFragment{}, conflict("source retry authority changed")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseActive ||
		root.ReleasePath != sourceReleasePathAbsent ||
		(root.RetryDisposition != RetryDispositionUndecided && root.RetryDisposition != RetryDispositionTransferred) {
		return ReleaseFragment{}, conflict("source root cannot become retry available")
	}
	deadline := expiresAt.UTC()
	root.RetryDisposition, root.RetryExpiresAt = RetryDispositionAvailable, &deadline
	encoded, err := encodeRoot(root)
	if err != nil {
		return ReleaseFragment{}, err
	}
	return ReleaseFragment{
		Conditions: []Condition{{Key: RootKey(operationID), ModRevision: expectedRevision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(operationID), Value: encoded}},
	}, nil
}

// PrepareRetryExpiry consumes an expired, unused retry opportunity. Its
// deadline remains part of the durable release authority through finalization.
func (repository *Repository) PrepareRetryExpiry(
	ctx context.Context, operationID string, expectedRevision int64, now time.Time,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" || expectedRevision <= 0 || now.IsZero() {
		return ReleaseFragment{}, validation("source retry expiry input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].ModRevision != expectedRevision {
		return ReleaseFragment{}, conflict("source retry authority changed")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseActive ||
		root.ReleasePath != sourceReleasePathAbsent || root.RetryDisposition != RetryDispositionAvailable ||
		root.RetryExpiresAt == nil || now.Before(*root.RetryExpiresAt) {
		return ReleaseFragment{}, conflict("source retry opportunity has not expired")
	}
	root.Phase, root.ReleasePath, root.RetryDisposition = operationSourcePhaseReleasing, sourceReleasePathRetryExpiry, RetryDispositionExpired
	encoded, err := encodeRoot(root)
	if err != nil {
		return ReleaseFragment{}, err
	}
	return ReleaseFragment{
		Conditions: []Condition{{Key: RootKey(operationID), ModRevision: expectedRevision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(operationID), Value: encoded}},
	}, nil
}

// PrepareRetryActivation joins the new Task's assignment claim. Only the
// transferred root can become undecided again; no membership is rewritten.
func (repository *Repository) PrepareRetryActivation(
	ctx context.Context, operationID string, expectedRevision int64,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" || expectedRevision <= 0 {
		return ReleaseFragment{}, validation("source retry activation input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].ModRevision != expectedRevision {
		return ReleaseFragment{}, conflict("source retry authority changed")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseActive ||
		root.ReleasePath != sourceReleasePathAbsent || root.RetryDisposition != RetryDispositionTransferred {
		return ReleaseFragment{}, conflict("source retry transfer is unavailable")
	}
	root.RetryDisposition = RetryDispositionUndecided
	encoded, err := encodeRoot(root)
	if err != nil {
		return ReleaseFragment{}, err
	}
	return ReleaseFragment{
		Conditions: []Condition{{Key: RootKey(operationID), ModRevision: expectedRevision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(operationID), Value: encoded}},
	}, nil
}

// PrepareRetryTransfer consumes an unexpired retry opportunity without
// changing memberships or counts. The caller atomically publishes the new Task
// and transfers the still-not-started execution in the same transaction.
func (repository *Repository) PrepareRetryTransfer(
	ctx context.Context, operationID string, expectedRevision int64, at time.Time,
) (ReleaseFragment, error) {
	if ctx == nil || operationID == "" || expectedRevision <= 0 || at.IsZero() {
		return ReleaseFragment{}, validation("source retry transfer input is invalid")
	}
	read, err := repository.store.GetMany(ctx, []string{RootKey(operationID)}, 0)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].ModRevision != expectedRevision {
		return ReleaseFragment{}, conflict("source retry authority changed")
	}
	root, err := decodeRoot(read.Values[0].Value)
	if err != nil {
		return ReleaseFragment{}, err
	}
	if root.OperationID != operationID || root.Phase != operationSourcePhaseActive ||
		root.ReleasePath != sourceReleasePathAbsent || root.RetryDisposition != RetryDispositionAvailable ||
		root.RetryExpiresAt == nil || !at.Before(*root.RetryExpiresAt) {
		return ReleaseFragment{}, conflict("source retry opportunity is unavailable")
	}
	root.RetryDisposition, root.RetryExpiresAt = RetryDispositionTransferred, nil
	encoded, err := encodeRoot(root)
	if err != nil {
		return ReleaseFragment{}, err
	}
	return ReleaseFragment{
		Conditions: []Condition{{Key: RootKey(operationID), ModRevision: expectedRevision}},
		Mutations:  []Mutation{{Type: MutationPut, Key: RootKey(operationID), Value: encoded}},
	}, nil
}
