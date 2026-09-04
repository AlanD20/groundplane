package scriptsourcereference

import "context"

func (repository *Repository) FinalPublicationFragment(
	ctx context.Context,
	prepared Prepared,
) (PublicationFragment, error) {
	if ctx == nil || prepared.IsZero() || prepared.descriptorRevision <= 0 || prepared.membershipCount == 0 {
		return PublicationFragment{}, validation("prepared source set is invalid")
	}
	read, err := repository.store.GetMany(
		ctx,
		[]string{PreparationKey(prepared.operationID), RootKey(prepared.operationID)},
		0,
	)
	if err != nil {
		return PublicationFragment{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil {
		return PublicationFragment{}, conflict("prepared source set is unavailable")
	}
	if read.Values[1] != nil {
		return PublicationFragment{}, conflict("operation source root is already active")
	}
	descriptor, err := decodePreparation(read.Values[0].Value)
	if err != nil {
		return PublicationFragment{}, err
	}
	if read.Values[0].ModRevision != prepared.descriptorRevision || descriptor.Phase != PreparationSealed ||
		descriptor.PreparationCursor != descriptor.MembershipCount || descriptor.OperationID != prepared.operationID ||
		descriptor.MembershipCount != prepared.membershipCount || descriptor.MembershipSHA256 != prepared.membershipSHA256 {
		return PublicationFragment{}, conflict("prepared source set changed before publication")
	}
	root := OperationSourceRoot{
		OperationID: prepared.operationID, MembershipCount: prepared.membershipCount,
		MembershipSHA256: prepared.membershipSHA256,
		Phase:            operationSourcePhaseActive,
		ReleasePath:      sourceReleasePathAbsent,
		RetryDisposition: RetryDispositionUndecided,
	}
	value, err := encodeRoot(root)
	if err != nil {
		return PublicationFragment{}, err
	}
	conditions := []Condition{
		{Key: PreparationKey(prepared.operationID), ModRevision: prepared.descriptorRevision},
		{Key: RootKey(prepared.operationID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: RootKey(prepared.operationID), Value: value},
		{Type: MutationDelete, Key: PreparationKey(prepared.operationID)},
	}
	for _, requirement := range prepared.staged {
		conditions = append(conditions, Condition{
			Key: requirement.SourceKey, ModRevision: requirement.PredecessorModRevision,
		})
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: requirement.SourceKey,
			Value: append([]byte(nil), requirement.Value...),
		})
	}
	return PublicationFragment{
		Conditions:         conditions,
		Mutations:          mutations,
		StagedRequirements: cloneRequirements(prepared.staged),
	}, nil
}
