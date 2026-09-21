package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *Lifecycle) ReplaceLifecycle(
	ctx context.Context,
	current etcdstore.Versioned[Record],
	replacement Record,
) (etcdstore.Versioned[Record], error) {
	if err := ValidateAttachVersion(current); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := ValidateAttachRecord(replacement); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !validAttachLifecycleReplacement(current.Record, replacement) {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindStateConflict,
			"Attach lifecycle replacement is invalid",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: AttachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletions.TombstoneKey("attach", current.Record.ID)},
	}
	if replacement.Operation == AttachOperationDetach && replacement.Status != core.AttachFailed {
		revision := current.ReadRevision
		if revision < current.Revision {
			revision = current.Revision
		}
		exclusionCondition, err := RequireAttachBackupSourceExclusionAbsent(
			ctx, repository.store, current.Record.ID, revision,
		)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		conditions = append(conditions, exclusionCondition)
	}
	value, err := EncodeAttachRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: AttachKey(current.Record.ID), Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !result.Succeeded {
		if replacement.Operation == AttachOperationDetach && replacement.Status != core.AttachFailed {
			_, exclusionErr := RequireAttachBackupSourceExclusionAbsent(
				ctx,
				repository.store,
				current.Record.ID,
				result.Revision,
			)
			if exclusionErr != nil {
				return etcdstore.Versioned[Record]{}, exclusionErr
			}
		}
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindStateConflict, "Attach changed concurrently")
	}
	return etcdstore.Versioned[Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func validAttachLifecycleReplacement(current Record, replacement Record) bool {
	if !AttachImmutableEqual(current, replacement) {
		return false
	}
	switch {
	case current.Status == core.AttachPending && replacement.Status == core.AttachProvisioning:
		return current.Operation == AttachOperationProvision && replacement.Operation == current.Operation &&
			replacement.TaskID == current.TaskID
	case current.Status == core.AttachProvisioning &&
		(replacement.Status == core.AttachReady || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == AttachOperationProvision &&
		replacement.Status == core.AttachPending:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	case (current.Status == core.AttachReady || current.Status == core.AttachFailed) &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == AttachOperationDetach && replacement.TaskID != current.TaskID
	case current.Status == core.AttachDetaching &&
		(replacement.Status == core.AttachDetached || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == AttachOperationDetach &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	default:
		return false
	}
}

func AttachImmutableEqual(left Record, right Record) bool {
	if left.ID != right.ID || left.EnvironmentID != right.EnvironmentID || left.Name != right.Name ||
		left.BackingProjectID != right.BackingProjectID || left.BackingEnvironmentID != right.BackingEnvironmentID ||
		left.BackingServiceID != right.BackingServiceID || left.BackingNetworkID != right.BackingNetworkID ||
		left.ServiceID != right.ServiceID || left.CredentialAttachID != right.CredentialAttachID ||
		left.HookBundle != right.HookBundle ||
		!left.CreatedAt.Equal(right.CreatedAt) ||
		!slices.Equal(left.GrantAttachIDs, right.GrantAttachIDs) ||
		len(left.FactSets) != len(right.FactSets) {
		return false
	}
	for index := range left.FactSets {
		if left.FactSets[index].GrantAttachID != right.FactSets[index].GrantAttachID ||
			!slices.Equal(left.FactSets[index].Facts, right.FactSets[index].Facts) {
			return false
		}
	}
	return true
}
