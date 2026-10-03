package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumecleanup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Effects is the local retirement transaction that consumes an exact remote
// absence proof. Its buffers are borrowed by the hierarchy finalizer.
type Effects struct {
	fixedInputDigest string
	conditions       []keyvalue.Condition
	mutations        []keyvalue.Mutation
	values           [][]byte
}

func (effects Effects) FixedInputDigest() string         { return effects.fixedInputDigest }
func (effects Effects) Conditions() []keyvalue.Condition { return effects.conditions }
func (effects Effects) Mutations() []keyvalue.Mutation   { return effects.mutations }
func (effects Effects) Values() [][]byte                 { return effects.values }

// PrepareFinalization proves that the Controller durably observed the exact
// selected object absent, then prepares local ownership retirement. The intent
// is retained with hierarchy evidence until the ordinary deletion-evidence
// retention window expires.
func (repository *Repository) PrepareFinalization(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	if repository == nil || repository.store == nil {
		return Effects{}, errs.New(errs.KindInternal, "hierarchy deletion backup cleanup repository is unavailable")
	}
	if err := validateActive(operation, action); err != nil {
		return Effects{}, err
	}
	local, err := repository.readLocalAuthority(ctx, action)
	if err != nil {
		return Effects{}, err
	}
	key, err := cleanupIntentKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		clear(local.primary.Value)
		return Effects{}, err
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		clear(local.primary.Value)
		return Effects{}, err
	}
	if read == nil || read.Entry == nil {
		clear(local.primary.Value)
		return Effects{}, errs.New(errs.KindStateConflict, "hierarchy deletion backup cleanup proof is absent")
	}
	intent, err := decodeCleanupIntent(read.Entry.Value)
	if err != nil || !intentMatches(intent, operation, action, local.fixedDigest) {
		clear(local.primary.Value)
		clear(read.Entry.Value)
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if intent.Selected == (backupruntime.BackupObjectIdentity{}) ||
		intent.ConfirmedAbsent != intent.Selected {
		clear(local.primary.Value)
		clear(read.Entry.Value)
		return Effects{}, errs.New(errs.KindStateConflict, "hierarchy deletion backup exact absence is not proved")
	}
	if err := applySelected(&local.remote, intent.Selected); err != nil {
		clear(local.primary.Value)
		clear(read.Entry.Value)
		return Effects{}, err
	}
	conditions := append([]keyvalue.Condition(nil), local.conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Entry.ModRevision})
	mutations := make([]keyvalue.Mutation, 0, 4)
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionRecoveryPointRemove:
		// Local authority is prune, primary, Environment, Source, Connector.
		for _, position := range []int{1, 2, 3, 4} {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: local.conditions[position].Key})
		}
	case hierarchydeletion.HierarchyDeletionOrphanObjectRemove:
		for _, condition := range local.conditions {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: condition.Key})
		}
	default:
		clear(local.primary.Value)
		clear(read.Entry.Value)
		return Effects{}, errs.New(errs.KindValidationFailed, "hierarchy deletion backup cleanup action is unsupported")
	}
	volumeConditions, volumeMutations, err := backupvolumecleanup.Prepare(
		ctx, repository.store, local.remote.Point, local.readRevision,
	)
	if err != nil {
		clear(local.primary.Value)
		clear(read.Entry.Value)
		return Effects{}, err
	}
	for _, condition := range volumeConditions {
		if action.ActionKind == hierarchydeletion.HierarchyDeletionOrphanObjectRemove &&
			condition.Key == backupruntime.BackupOrphanKey(action.TargetID) && condition.ModRevision == 0 {
			continue
		}
		conditions = append(conditions, condition)
	}
	mutations = append(mutations, volumeMutations...)
	values := [][]byte{local.primary.Value, read.Entry.Value}
	for _, mutation := range volumeMutations {
		if len(mutation.Value) != 0 {
			values = append(values, mutation.Value)
		}
	}
	return Effects{fixedInputDigest: local.fixedDigest, conditions: conditions, mutations: mutations,
		values: values}, nil
}
