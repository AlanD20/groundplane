package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func requireEnvironmentDeletionLiveAuthorityEmpty(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
	projectedKeys ...string,
) error {
	directKeys := append(environmentDeletionLiveAuthorityKeys(environmentID), projectedKeys...)
	direct, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: directKeys, Revision: revision,
	})
	if err != nil {
		return err
	}
	if direct == nil || direct.ReadRevision != revision ||
		len(direct.Values) != len(directKeys) {
		if direct != nil {
			clearKeyValues(direct.Values)
		}
		return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
	}
	for _, value := range direct.Values {
		if value != nil {
			clearKeyValues(direct.Values)
			return errs.New(errs.KindStateConflict, "environment retained live child authority")
		}
	}
	clearKeyValues(direct.Values)
	active, err := readActiveScriptSet(ctx, store, environmentID, revision)
	if err != nil {
		return err
	}
	activeScripts, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: scriptrecord.ScriptSetOwnerPrefix(environmentID, active.Record.GenerationID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return err
	}
	if activeScripts == nil || activeScripts.ReadRevision != revision || len(activeScripts.Values) != 0 {
		if activeScripts != nil {
			clearRangeValues(activeScripts.Values)
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
				clearRangeValues(page.Values)
			}
			return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
		}
		present := len(page.Values) != 0
		clearRangeValues(page.Values)
		if present {
			return errs.New(errs.KindStateConflict, "environment retained durable children")
		}
	}
	return nil
}

func environmentDeletionLiveAuthorityConditions(
	environmentID string, operationID string, projectedKeys ...string,
) []etcdstore.Condition {
	keys := append(environmentDeletionLiveAuthorityKeys(environmentID), projectedKeys...)
	conditions := make([]etcdstore.Condition, 0, len(keys)+18)
	for _, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key})
	}
	for _, prefix := range environmentDeletionLiveAuthorityPrefixes(environmentID, operationID) {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptEnvironmentLocatorPrefixFor(environmentID), Prefix: true})
	return conditions
}

func environmentDeletionLiveAuthorityKeys(environmentID string) []string {
	return []string{
		zonePoolRegistryKey(environmentID),
		coordinationrecord.Key(environmentID),
		componentTaskActiveEnvironmentKey(environmentID),
		removalrecord.EnvironmentLockKey(environmentID),
		backuppolicy.BackupPolicyKey(environmentID),
		backuppolicy.BackupKeyKey(environmentID),
		backuppolicy.BackupKeyValueKey(environmentID),
	}
}

func environmentDeletionLiveAuthorityPrefixes(environmentID string, operationID string) []string {
	return []string{
		blueprints.EnvironmentBlueprintRevisionsPrefix(environmentID),
		routerecord.OwnerPrefix(environmentID),
		entryOwnerCollectionPrefix(environmentID),
		attachrecord.AttachOwnerPrefix(environmentID),
		componentrecord.EnvironmentOwnerPrefix(environmentID),
		connectorEnvironmentPrefix(environmentID),
		environmentReleaseGroupOwnerPrefix + environmentID + "/",
		backuppolicy.BackupSourceEnvironmentPrefix(environmentID),
		backupruntime.BackupScheduleCursorPrefix + environmentID + "/",
		backupruntime.BackupDueOutcomePrefix + environmentID + "/",
		backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRunEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRestoreEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupKeyRotationEnvironmentPrefix + environmentID + "/",
		environmentDeletionWorkOperationPrefix(operationID),
	}
}
