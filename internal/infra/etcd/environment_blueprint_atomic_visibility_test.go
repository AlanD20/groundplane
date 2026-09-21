package etcd

import (
	"context"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
)

func assertEnvironmentBlueprintAtomicShapeVisible(
	t *testing.T,
	published environmentBlueprintAtomicPublication,
	revision int64,
) {
	t.Helper()
	keys := []string{
		testblueprints.EnvironmentBlueprintHeadKey(published.environmentID),
		testtaskjournal.TaskStorageKey(published.task.ID),
		testtaskjournal.TaskQueueKey(published.task.Executor, published.task.ID),
		published.markerKey,
	}
	if published.releasePublicationID != "" {
		keys = append(keys, testreleases.ReleasePublicationKey(published.releasePublicationID))
	}
	keys = append(keys, published.candidateAttachKeys()...)
	for _, retained := range published.retainedAttachRevisions {
		keys = append(keys, testattachments.AttachKey(retained.Record.ID))
	}
	for _, relation := range published.retainedGrantRelations {
		keys = append(keys, testattachments.AttachGrantedByKey(relation[0], relation[1]))
	}
	for _, sourceID := range published.newBackupSourceIDs {
		keys = append(
			keys,
			testbackuppolicy.BackupSourceKey(sourceID),
			testbackuppolicy.BackupSourceEnvironmentKey(published.environmentID, sourceID),
		)
	}
	if len(published.newBackupSourceIDs) != 0 {
		keys = append(
			keys,
			testbackuppolicy.BackupPolicyKey(published.environmentID),
			testenvironmentcoordination.Key(published.environmentID),
			testbackuppolicy.BackupKeyKey(published.environmentID),
			testbackuppolicy.BackupKeyValueKey(published.environmentID),
		)
	}
	for _, source := range published.newBackupSources {
		keys = append(keys, testbackuppolicy.BackupSourceIdentityKey(
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
		connectorKey := testbackuppolicy.BackupPolicyConnectorReferenceKey(
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
	policyRead, err := published.store.Get(context.Background(), testbackuppolicy.BackupPolicyKey(environmentID))
	if err != nil || policyRead.Entry == nil {
		t.Fatalf("read published Backup policy for Connector identity = %#v, %v", policyRead, err)
	}
	policy, err := testbackuppolicy.DecodeBackupPolicyRecord(policyRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode published Backup policy for Connector identity error = %v", err)
	}
	return policy.ConnectorID
}
