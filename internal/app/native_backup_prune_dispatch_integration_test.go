package app

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// TASK-156: a fixed-size retention selection must not stall forever after native
// execution increases transaction cost. The actual dispatcher must publish a
// fitting batch and resume immediately before the first still-pending Point.
// Point/artifact metadata are supplied; this does not execute remote deletion.
func TestNativeBackupPruneDispatcherKeepsUnselectedPointsQueued(t *testing.T) {
	f := newNativeVolumeFixture(t)
	volume := f.start(t, f.created.TaskID)
	progress := f.progress(t, volume)
	for progress.Stage != volumeexecution.Complete {
		progress = f.advance(t, volume, progress)
	}
	f.complete(t, volume, progress)
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	connector, err := connectors.NewRecord(
		core.Connector{ID: ids.New(ids.KindConnector), EnvironmentID: f.environmentID,
			Name: "prune-store", Kind: core.ConnectorKindS3Compatible, Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
			Prefix: "qa/", Region: "auto", PathStyle: true, Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
				core.ConnectorCredentialAccessKey: {
					Kind: core.ConnectorCredentialDirect,
				}, core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialDirect}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := connectors.NewEncryptedCredentials(
		connector.Connector.ID,
		[]byte("supplied-connector-envelope"),
	)
	if err != nil {
		t.Fatal(err)
	}
	connectorRaw, err := connectors.EncodeRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	credentialRaw, err := connectors.EncodeEncryptedCredentials(credentials)
	if err != nil {
		t.Fatal(err)
	}
	connectorCommit, err := f.store.Transact(t.Context(), nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: connectors.RecordKey(connector.Connector.ID), Value: connectorRaw},
		{Type: keyvalue.MutationPut, Key: connectors.CredentialValueKey(connector.Connector.ID), Value: credentialRaw}})
	clear(connectorRaw)
	clear(credentialRaw)
	if err != nil || !connectorCommit.Succeeded {
		t.Fatalf("Connector atomic metadata/credentials: %v", err)
	}
	source := backuppolicy.BackupSourceRecord{ID: ids.New(ids.KindBackupSource), EnvironmentID: f.environmentID,
		Kind: core.BackupSourceVolume, TargetID: f.created.Volume.ID, CreatedAt: at.Add(-time.Hour)}
	nativeVolumePut(t, f.store, backuppolicy.BackupSourceKey(source.ID), source, backuppolicy.EncodeBackupSourceRecord)
	operationID := ids.New(ids.KindOperation)
	var points []backupruntime.BackupRecoveryPointRecord
	for index := 0; index < backupruntime.MaximumBackupPruneDispatchPoints; index++ {
		point := nativeBackupCredentialPoint(
			source,
			connector,
			f.authorization.Identity(),
			at.Add(time.Duration(index)*time.Millisecond),
		)
		points = append(points, point)
		raw, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
		if err != nil {
			t.Fatal(err)
		}
		environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if err != nil {
			t.Fatal(err)
		}
		sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if err != nil {
			t.Fatal(err)
		}
		connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if err != nil {
			t.Fatal(err)
		}
		commit, err := f.store.Transact(t.Context(), nil, []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: backupruntime.BackupRecoveryPointKey(point.ID), Value: raw},
			{Type: keyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
			{Type: keyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
			{Type: keyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)}})
		clear(raw)
		if err != nil || !commit.Succeeded {
			t.Fatalf("seed verified Point: %v", err)
		}
		prune := backupruntime.BackupRecoveryPointPruneRecord{
			Point:          point.BackupRecoveryPointSnapshot,
			PointRevision:  commit.Revision,
			PolicyRevision: 1,
			PolicySHA256:   strings.Repeat("a", 64),
			OperationID:    operationID,
			State:          backupruntime.BackupPrunePending,
			CreatedAt:      at.Add(2 * time.Second),
			UpdatedAt:      at.Add(2 * time.Second),
		}
		nativeVolumePut(
			t,
			f.store,
			backupruntime.BackupRecoveryPointPruneKey(point.ID),
			prune,
			backupruntime.EncodeBackupRecoveryPointPruneRecord,
		)
	}
	runtime, err := etcd.NewBackupRuntimeRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := backup.NewDurableBackupRunIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	service, err := backup.NewBackupPruneService(runtime, protected)
	if err != nil {
		t.Fatal(err)
	}
	cursor, accepted, err := service.Tick(t.Context(), "")
	if err != nil || !accepted || cursor == "" {
		t.Fatalf("bounded dispatch: %q/%t/%v", cursor, accepted, err)
	}
	selected := 0
	var lastAssigned string
	for _, point := range points {
		entry, err := f.store.Get(t.Context(), backupruntime.BackupRecoveryPointPruneKey(point.ID))
		if err != nil || entry.Entry == nil {
			t.Fatalf("selection lost Point: %v", err)
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		if prune.State == backupruntime.BackupPruneAssigned {
			selected++
			lastAssigned = entry.Entry.Key
		} else if prune.State != backupruntime.BackupPrunePending || prune.TaskID != "" {
			t.Fatal("unselected Point changed ownership")
		}
	}
	if selected == 0 || selected >= len(points) || cursor != lastAssigned {
		t.Fatalf("dispatch did not retain exact remainder: %d/%d, %q/%q", selected, len(points), cursor, lastAssigned)
	}
	page, err := runtime.ListPendingBackupPruneCandidates(t.Context(), cursor)
	if err != nil || len(page.Pending) != len(points)-selected {
		t.Fatalf("cursor skipped pending Points: %v/%v", page, err)
	}
	queued, err := f.queue.ListTasks(t.Context(), f.environmentID, "", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	native := 0
	owner, err := backupwork.NewInputRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range queued.Tasks {
		if task.Task.Action != workexecution.ActionBackupPrune {
			continue
		}
		native++
		work, err := f.queue.ListWork(t.Context(), task.Task.ID, "", 100, 0)
		if err != nil || len(work.Items) != 1 {
			t.Fatalf("prune native membership: %v", err)
		}
		view, err := f.queue.ReadWork(t.Context(), task.Task.ID, work.Items[0].Item.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		input, _, err := owner.Load(t.Context(), view)
		if err != nil || len(input.Steps) != selected {
			t.Fatalf("captured selection: %v", err)
		}
	}
	if native != 1 {
		t.Fatalf("dispatch published %d Tasks", native)
	}
}
