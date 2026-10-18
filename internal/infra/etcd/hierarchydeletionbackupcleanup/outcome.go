package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Outcome struct {
	Absent        bool
	DeleteGranted bool
	Conditions    []keyvalue.Condition
}

// ReadOutcome reads only saved evidence at the execution settlement snapshot.
// A delete grant without absence remains uncertain even after its deadline.
func (repository *Repository) ReadOutcome(ctx context.Context, operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction, revision int64,
) (Outcome, error) {
	if repository == nil || repository.store == nil || revision <= 0 {
		return Outcome{}, errs.New(errs.KindInternal, "backup cleanup outcome requires captured storage")
	}
	if err := validateActive(operation, action); err != nil {
		return Outcome{}, err
	}
	key, err := cleanupIntentKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		return Outcome{}, err
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Outcome{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Outcome{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	result := Outcome{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}}
	if read.Values[0] == nil {
		return result, nil
	}
	intent, err := decodeCleanupIntent(read.Values[0].Value)
	if err != nil || !intentMatches(intent, operation, action, intent.FixedInputDigest) {
		return Outcome{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if err := verifyRemoteTemplate(action, intent.FixedInputDigest); err != nil {
		return Outcome{}, err
	}
	result.Absent = intent.Selected != (backupruntime.BackupObjectIdentity{}) && intent.ConfirmedAbsent == intent.Selected
	result.DeleteGranted = intent.DeleteAttempt != ""
	return result, nil
}
