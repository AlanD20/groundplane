package hierarchydeletionfinalization

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func PrepareEnvironmentDeletionFinalization(
	ctx context.Context,
	store finalizationStore,
	environmentID string,
	operationID string,
	revision int64,
	projectedKeys ...string,
) (Effects, error) {
	metadata, err := readIdleEnvironmentMetadata(ctx, store, environmentID, revision)
	if err != nil {
		return Effects{}, err
	}
	keys := append(environmentDeletionChildAuthorityKeys(environmentID), projectedKeys...)
	if err := requireEnvironmentDeletionAuthorityEmpty(ctx, store, environmentID, operationID, revision, keys); err != nil {
		return Effects{}, err
	}
	metadata.conditions = append(metadata.conditions,
		environmentDeletionAuthorityConditions(environmentID, operationID, keys)...)
	return metadata, nil
}

func requireEnvironmentDeletionAuthorityEmpty(
	ctx context.Context, store finalizationStore, environmentID, operationID string,
	revision int64, directKeys []string,
) error {
	direct, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: directKeys, Revision: revision,
	})
	if err != nil {
		return err
	}
	if direct == nil || direct.ReadRevision != revision ||
		len(direct.Values) != len(directKeys) {
		if direct != nil {
			etcdstore.ClearValues(direct.Values)
		}
		return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
	}
	for _, value := range direct.Values {
		if value != nil {
			etcdstore.ClearValues(direct.Values)
			return errs.New(errs.KindStateConflict, "environment retained live child authority")
		}
	}
	etcdstore.ClearValues(direct.Values)
	active, err := scriptrecord.ReadActiveScriptSet(ctx, store, environmentID, revision)
	if err != nil {
		return err
	}
	activeScripts, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: scriptrecord.ScriptSetOwnerPrefix(
			environmentID,
			active.Record.GenerationID,
		), Limit: 1, Revision: revision,
	})
	if err != nil {
		return err
	}
	if activeScripts == nil || activeScripts.ReadRevision != revision || len(activeScripts.Values) != 0 {
		if activeScripts != nil {
			etcdstore.ClearRangeValues(activeScripts.Values)
		}
		return errs.New(errs.KindStateConflict, "environment retained durable Scripts")
	}
	for _, prefix := range environmentDeletionLiveAuthorityPrefixes(environmentID, operationID) {
		page, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 1 {
			if page != nil {
				etcdstore.ClearRangeValues(page.Values)
			}
			return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
		}
		present := len(page.Values) != 0
		etcdstore.ClearRangeValues(page.Values)
		if present {
			return errs.New(errs.KindStateConflict, "environment retained durable children")
		}
	}
	return nil
}

func environmentDeletionAuthorityConditions(environmentID, operationID string, keys []string) []etcdstore.Condition {
	conditions := make([]etcdstore.Condition, 0, len(keys)+18)
	for _, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key})
	}
	for _, prefix := range environmentDeletionLiveAuthorityPrefixes(environmentID, operationID) {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	conditions = append(
		conditions,
		etcdstore.Condition{Key: scriptrecord.ScriptEnvironmentLocatorPrefixFor(environmentID), Prefix: true},
	)
	return conditions
}

func environmentDeletionChildAuthorityKeys(environmentID string) []string {
	return []string{
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
		removalrecord.EnvironmentLockKey(environmentID),
		backuppolicy.BackupKeyKey(environmentID),
		backuppolicy.BackupKeyValueKey(environmentID),
	}
}

func environmentDeletionLiveAuthorityPrefixes(environmentID string, operationID string) []string {
	return []string{
		runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerEnvironment, environmentID),
		blueprints.EnvironmentBlueprintRevisionsPrefix(environmentID),
		routerecord.OwnerPrefix(environmentID),
		entries.EntryOwnerCollectionPrefix(environmentID),
		attachrecord.AttachOwnerPrefix(environmentID),
		componentrecord.EnvironmentOwnerPrefix(environmentID),
		connectors.ConnectorEnvironmentPrefix(environmentID),
		releasegroups.ReleaseGroupOwnerPrefix + environmentID + "/",
		backuppolicy.BackupSourceEnvironmentPrefix(environmentID),
		backupruntime.BackupScheduleCursorPrefix + environmentID + "/",
		backupruntime.BackupDueOutcomePrefix + environmentID + "/",
		backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRunEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRestoreEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupKeyRotationEnvironmentPrefix + environmentID + "/",
		deletions.EnvironmentDeletionWorkOperationPrefix(operationID),
	}
}
