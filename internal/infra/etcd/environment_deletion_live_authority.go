package etcd

import (
	"context"
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
		environmentCoordinationKey(environmentID),
		componentTaskActiveEnvironmentKey(environmentID),
		removalrecord.EnvironmentLockKey(environmentID),
		backupPolicyKey(environmentID),
		backupKeyKey(environmentID),
		backupKeyValueKey(environmentID),
	}
}

func environmentDeletionLiveAuthorityPrefixes(environmentID string, operationID string) []string {
	return []string{
		environmentBlueprintRevisionsPrefix(environmentID),
		routerecord.OwnerPrefix(environmentID),
		entryOwnerCollectionPrefix(environmentID),
		attachOwnerPrefix(environmentID),
		componentEnvironmentOwnerPrefix(environmentID),
		connectorEnvironmentPrefix(environmentID),
		environmentReleaseGroupOwnerPrefix + environmentID + "/",
		backupSourceEnvironmentPrefix(environmentID),
		backupScheduleCursorPrefix + environmentID + "/",
		backupDueOutcomePrefix + environmentID + "/",
		backupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupRunEnvironmentPrefix + environmentID + "/",
		backupOrphanEnvironmentPrefix + environmentID + "/",
		backupRestoreEnvironmentPrefix + environmentID + "/",
		backupKeyRotationEnvironmentPrefix + environmentID + "/",
		environmentDeletionWorkOperationPrefix(operationID),
	}
}
