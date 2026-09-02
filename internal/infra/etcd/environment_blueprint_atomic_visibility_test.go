package etcd

import (
	"context"
	"testing"
)

func assertEnvironmentBlueprintAtomicShapeVisible(
	t *testing.T,
	published environmentBlueprintAtomicPublication,
	revision int64,
) {
	t.Helper()
	keys := []string{
		environmentBlueprintHeadKey(published.environmentID),
		taskKey(published.task.ID),
		taskQueueKey(published.task.Executor, published.task.ID),
		published.markerKey,
	}
	if published.releasePublicationID != "" {
		keys = append(keys, releasePublicationKey(published.releasePublicationID))
	}
	keys = append(keys, published.candidateAttachKeys()...)
	for _, retained := range published.retainedAttachRevisions {
		keys = append(keys, attachKey(retained.Record.ID))
	}
	for _, relation := range published.retainedGrantRelations {
		keys = append(keys, attachGrantedByKey(relation[0], relation[1]))
	}
	for _, sourceID := range published.newBackupSourceIDs {
		keys = append(
			keys,
			backupSourceKey(sourceID),
			backupSourceEnvironmentKey(published.environmentID, sourceID),
		)
	}
	if len(published.newBackupSourceIDs) != 0 {
		keys = append(keys,
			backupPolicyKey(published.environmentID),
			environmentCoordinationKey(published.environmentID),
			backupKeyKey(published.environmentID),
			backupKeyValueKey(published.environmentID),
		)
	}
	for _, source := range published.newBackupSources {
		keys = append(keys, backupSourceIdentityKey(
			published.environmentID, source.Kind, source.TargetID,
		))
	}
	for _, key := range keys {
		read, err := published.store.Get(context.Background(), key)
		if err != nil || read.Entry == nil || read.Entry.ModRevision != revision {
			t.Fatalf("atomic authority %q = %#v, %v; want revision %d", key, read, err, revision)
		}
	}
	if len(published.newBackupSourceIDs) != 0 {
		connectorKey := backupPolicyConnectorReferenceKey(
			published.storeConnectorID(t, published.environmentID), published.environmentID,
		)
		read, err := published.store.Get(context.Background(), connectorKey)
		condition, compared := published.audit.conditionByKey[connectorKey]
		if err != nil || read.Entry == nil || !compared || condition.ModRevision != read.Entry.ModRevision {
			t.Fatalf("enabled Connector reference fence %q = %#v/%#v, %v", connectorKey, read, condition, err)
		}
	}
}

func (published environmentBlueprintAtomicPublication) storeConnectorID(t *testing.T, environmentID string) string {
	t.Helper()
	policyRead, err := published.store.Get(context.Background(), backupPolicyKey(environmentID))
	if err != nil || policyRead.Entry == nil {
		t.Fatalf("read published Backup policy for Connector identity = %#v, %v", policyRead, err)
	}
	policy, err := decodeBackupPolicyRecord(policyRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode published Backup policy for Connector identity error = %v", err)
	}
	return policy.ConnectorID
}
