package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ScriptWorkPublication owns the source reservations staged by one deferred
// Script compiler. Its caller publishes Change with the original Task CAS.
type ScriptWorkPublication struct {
	Change           workqueue.Change
	authority        *scriptsourcepublication.Authority
	operationID      string
	members          []scriptsourceevidence.ScriptSourcePreparationMember
	store            hierarchyStore
	snapshotID       string
	workID           string
	snapshotRevision int64
}

func (publication ScriptWorkPublication) Finish(ctx context.Context, applied bool) error {
	defer clearScriptSourcePreparationMembers(publication.members)
	if applied || publication.authority == nil {
		return nil
	}
	// Abandon checks the active source root. A lost successful commit can never
	// release the source memberships of the now-executable Task.
	if err := publication.authority.Abandon(ctx, publication.operationID, publication.members); err != nil {
		return err
	}
	return abandonScriptSnapshot(
		ctx,
		publication.store,
		publication.snapshotID,
		publication.workID,
		publication.snapshotRevision,
	)
}

func (repository *ScriptRepository) abandonManualScriptSnapshot(
	ctx context.Context,
	snapshotID, workID string,
	revision int64,
) error {
	return abandonScriptSnapshot(ctx, repository.store, snapshotID, workID, revision)
}

func abandonScriptSnapshot(ctx context.Context, store hierarchyStore, snapshotID, workID string, revision int64) error {
	key := scriptexecutions.ScriptRunnerSnapshotKey(snapshotID)
	result, err := store.Transact(ctx,
		[]keyvalue.Condition{{Key: key, ModRevision: revision}, {Key: scriptexecutions.WorkKey(workID)}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}})
	keyvalue.ClearValues(result.FailureReads)
	// A changed snapshot or an active Work binding belongs to its new owner.
	// In particular, a lost successful publication cannot be collected here.
	return err
}
