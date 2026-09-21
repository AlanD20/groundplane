package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *AttachRepository) ReplaceLifecycle(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	replacement attachrecord.Record,
) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := validateAttachVersion(current); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := attachrecord.ValidateAttachRecord(replacement); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if !validAttachLifecycleReplacement(current.Record, replacement) {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindStateConflict, "Attach lifecycle replacement is invalid")
	}
	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletions.TombstoneKey("attach", current.Record.ID)},
	}
	if replacement.Operation == attachrecord.AttachOperationDetach && replacement.Status != core.AttachFailed {
		revision := current.ReadRevision
		if revision < current.Revision {
			revision = current.Revision
		}
		exclusionCondition, err := requireAttachBackupSourceExclusionAbsent(
			ctx, repository.store, current.Record.ID, revision,
		)
		if err != nil {
			return etcdstore.Versioned[attachrecord.Record]{}, err
		}
		conditions = append(conditions, exclusionCondition)
	}
	value, err := attachrecord.EncodeAttachRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.Record.ID), Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if !result.Succeeded {
		if replacement.Operation == attachrecord.AttachOperationDetach && replacement.Status != core.AttachFailed {
			_, exclusionErr := requireAttachBackupSourceExclusionAbsent(
				ctx,
				repository.store,
				current.Record.ID,
				result.Revision,
			)
			if exclusionErr != nil {
				return etcdstore.Versioned[attachrecord.Record]{}, exclusionErr
			}
		}
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindStateConflict, "Attach changed concurrently")
	}
	return etcdstore.Versioned[attachrecord.Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func validAttachLifecycleReplacement(current attachrecord.Record, replacement attachrecord.Record) bool {
	if !attachImmutableEqual(current, replacement) {
		return false
	}
	switch {
	case current.Status == core.AttachPending && replacement.Status == core.AttachProvisioning:
		return current.Operation == attachrecord.AttachOperationProvision && replacement.Operation == current.Operation &&
			replacement.TaskID == current.TaskID
	case current.Status == core.AttachProvisioning &&
		(replacement.Status == core.AttachReady || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == attachrecord.AttachOperationProvision &&
		replacement.Status == core.AttachPending:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	case (current.Status == core.AttachReady || current.Status == core.AttachFailed) &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == attachrecord.AttachOperationDetach && replacement.TaskID != current.TaskID
	case current.Status == core.AttachDetaching &&
		(replacement.Status == core.AttachDetached || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == attachrecord.AttachOperationDetach &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	default:
		return false
	}
}

func attachImmutableEqual(left attachrecord.Record, right attachrecord.Record) bool {
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

func validateAttachVersion(current etcdstore.Versioned[attachrecord.Record]) error {
	if current.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach record revision must be positive")
	}
	return attachrecord.ValidateAttachRecord(current.Record)
}
