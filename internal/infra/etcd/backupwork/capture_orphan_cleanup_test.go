package backupwork

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-125: cleanup must not depend on expired Task diagnostics. Both a failure
// already persisted and one first accounted for during recovery must retain the
// exact native receipt, without overwriting a foreign or concurrently changed
// orphan. This checks the resource fragment, not host cleanup or full recovery.
func TestCaptureRecoveryRetainsExactOrphanCleanup(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "created during recovery"
		if existing {
			name = "retained from failure"
		}
		t.Run(name, func(t *testing.T) {
			store, view, input, current, next, evidence, at := orphanCleanupFixture(t, existing)
			repository, err := NewInputRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			change, err := repository.prepareCaptureOrphanCleanup(t.Context(), view, input, current, next, evidence, at)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(change.Mutations)
			// Evaluate the produced guards against observed and changed storage,
			// rather than just counting declarations in a transaction plan.
			for _, guard := range change.Conditions {
				if !keyvalue.ConditionMatchesRead(guard, store.values[guard.Key]) {
					t.Fatal("cleanup rejected its original source snapshot")
				}
				changed := &keyvalue.KeyValue{Key: guard.Key, ModRevision: 51}
				if keyvalue.ConditionMatchesRead(guard, changed) {
					t.Fatal("cleanup could overwrite a concurrently replaced source")
				}
			}
			version := int64(1)
			if existing {
				version = 2
			}
			values := make([]*keyvalue.KeyValue, 0, len(change.Mutations))
			for _, mutation := range change.Mutations {
				values = append(values, &keyvalue.KeyValue{Key: mutation.Key, Value: mutation.Value,
					Version: version, ModRevision: 51})
			}
			if len(values) == 0 {
				t.Fatal("cleanup lost the orphan")
			}
			orphan, err := backupruntime.DecodeBackupOrphanRecord(values[0].Value)
			if err != nil {
				t.Fatal(err)
			}
			if err := backupruntime.ValidateBackupOrphanCompanionEvidence(values, orphan); err != nil {
				t.Fatalf("retained cleanup cannot be reconciled independently: %v", err)
			}
			proof := orphan.CleanupProof
			digest, err := executionplan.BackupSourceStagingKey(
				next.TaskID,
				input.Steps[0].ExecutionId,
				orphan.Target.ID,
			)
			if err != nil {
				t.Fatal(err)
			}
			if proof.TaskID != next.TaskID || proof.WorkItemID != view.Work.Item.Item.ID ||
				proof.AttemptID != view.Attempt.Attempt.ID || proof.Executor != view.Attempt.Attempt.Executor ||
				proof.Input != view.Attempt.Attempt.Input || proof.Result != evidence ||
				proof.ExecutionID != input.Steps[0].ExecutionId || proof.SourceOrdinal != 0 ||
				proof.RecoveryKeySHA256 != hex.EncodeToString(digest) || !proof.RecordedAt.Equal(at) {
				t.Fatal("orphan cleanup did not retain its exact captured execution and receipt")
			}
			if !backupruntime.BackupOrphanMatchesRunSource(orphan, next, 0) {
				t.Fatal("cleanup changed the unresolved upload or captured source")
			}
			if existing && !orphan.CreatedAt.Equal(current.UpdatedAt) {
				t.Fatal("cleanup replaced the existing orphan's creation boundary")
			}
		})
	}
	for _, fault := range []string{"missing", "foreign source", "split indexes", "occupied new source"} {
		t.Run(fault, func(t *testing.T) {
			store, view, input, current, next, evidence, at := orphanCleanupFixture(t, fault != "occupied new source")
			orphan := backupruntime.BackupOrphanRecordFromRun(current, 0)
			key := backupruntime.BackupOrphanKey(orphan.Target.ID)
			switch fault {
			case "missing":
				delete(store.values, key)
			case "foreign source":
				orphan.Reconciliation.OperationID = ids.NewAt(ids.KindOperation, at, 99)
				raw, err := backupruntime.EncodeBackupOrphanRecord(orphan)
				if err != nil {
					t.Fatal(err)
				}
				store.values[key].Value = raw
			case "split indexes":
				store.values[key].ModRevision--
			case "occupied new source":
				store.values[key] = &keyvalue.KeyValue{
					Key:         key,
					Value:       []byte("another owner"),
					Version:     1,
					ModRevision: 40,
				}
			}
			repository, err := NewInputRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			change, err := repository.prepareCaptureOrphanCleanup(t.Context(), view, input, current, next, evidence, at)
			defer keyvalue.ClearMutationValues(change.Mutations)
			if err == nil || len(change.Mutations) != 0 {
				t.Fatal("invalid orphan ownership authorized cleanup settlement")
			}
		})
	}
}

func orphanCleanupFixture(t *testing.T, existing bool) (*orphanCleanupReadStore, workqueue.ResumeView,
	*agentpb.BackupWorkInput, backupruntime.BackupRunRecord, backupruntime.BackupRunRecord, workexecution.Input, time.Time,
) {
	t.Helper()
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	created := at.Add(-time.Minute)
	digest := strings.Repeat("a", 64)
	environment := ids.NewAt(ids.KindEnvironment, created, 1)
	source := backupruntime.BackupRunSourceAttemptRecord{
		Ordinal: 0, SourceID: ids.NewAt(ids.KindBackupSource, created, 2),
		Kind: backupruntime.BackupRuntimeSourceAttach, Format: backupruntime.BackupRuntimeFormatPostgres,
		TargetID: ids.NewAt(ids.KindAttach, created, 3), RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, created, 4),
		RecoveryPointCreatedAt: created, State: backupruntime.BackupSourceAttemptStaged, Phase: backupruntime.BackupSourcePhaseHeadVerification,
		Snapshot: backupruntime.BackupRunSourceSnapshot{Postgres: &backupruntime.BackupPostgresSourceSnapshot{
			Database: "app-web_000001", Role: "app-web_000001", BackingEnvironmentID: ids.NewAt(ids.KindEnvironment, created, 5),
			BackingServiceID: ids.NewAt(
				ids.KindService,
				created,
				6,
			), ConsumerServiceID: ids.NewAt(ids.KindService, created, 7)}},
		Evidence: backupruntime.BackupArtifactEvidence{SourceSizeBytes: 4096, StoredSizeBytes: 4096,
			SourceSHA256: digest, StoredSHA256: digest},
		PostgresArchive: backupruntime.BackupPostgresArchiveEvidence{PGDumpMajor: 16, AdapterContractVersion: 1,
			SourceServerVersion: "16.9", BackupToolVersion: "16.9"},
	}
	source.ObjectKey = "qa/" + environment + "/" + source.SourceID + "/" + source.RecoveryPointID + "/artifact.bin"
	current := backupruntime.BackupRunRecord{TaskID: ids.NewAt(ids.KindTask, created, 8),
		OperationID: ids.NewAt(ids.KindOperation, created, 9), EnvironmentID: environment,
		PolicyRevision: 7, PolicySHA256: digest, RetentionKeep: 3, ConnectorID: ids.NewAt(ids.KindConnector, created, 10),
		ConnectorPrefix: "qa/", ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups", ConnectorRegion: "auto",
		Encryption: backupruntime.BackupRuntimeEncryptionNone, State: backupruntime.BackupRunRunning,
		Sources: []backupruntime.BackupRunSourceAttemptRecord{source}, CreatedAt: created, UpdatedAt: created}
	target := backupruntime.BackupOrphanRecordFromRun(current, 0).Target.ObjectTarget()
	current.Sources[0].Upload = backupruntime.BackupUploadOutcome{
		Kind:   backupruntime.BackupUploadUnknown,
		Target: target,
	}
	if existing {
		current.State = backupruntime.BackupRunFailed
		current.Sources[0].State = backupruntime.BackupSourceAttemptOrphaned
		current.Sources[0].FailureCode = backupruntime.BackupFailureHeadVerification
	}
	next := backupruntime.CloneBackupRunPublicationRecord(current)
	next.State, next.UpdatedAt = backupruntime.BackupRunFailed, at
	next.Sources[0].State = backupruntime.BackupSourceAttemptOrphaned
	next.Sources[0].FailureCode = backupruntime.BackupFailureHeadVerification
	reference := workexecution.Input{ID: ids.NewAt(ids.KindPlan, created, 11), SHA256: digest}
	evidence := workexecution.Input{ID: ids.NewAt(ids.KindPlan, created, 12), SHA256: strings.Repeat("b", 64)}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: ids.NewAt(ids.KindHost, created, 13),
		AgentID: ids.NewAt(ids.KindAgent, created, 14), Generation: 2}
	item := workexecution.Item{ID: ids.NewAt(ids.KindWorkItem, created, 15)}
	view := workqueue.ResumeView{Work: workqueue.View{Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: 50},
		Attempt: workqueue.AttemptSnapshot{Attempt: workexecution.Attempt{ID: ids.NewAt(ids.KindAttempt, created, 16),
			Executor: identity, Input: reference}}}
	input := &agentpb.BackupWorkInput{Steps: []*agentpb.BackupStepAuthority{{ExecutionId: ids.NewULID()}}}
	store := &orphanCleanupReadStore{values: make(map[string]*keyvalue.KeyValue)}
	if existing {
		orphan := backupruntime.BackupOrphanRecordFromRun(current, 0)
		raw, err := backupruntime.EncodeBackupOrphanRecord(orphan)
		if err != nil {
			t.Fatal(err)
		}
		connector, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Target.ConnectorID, orphan.Target.ID)
		if err != nil {
			t.Fatal(err)
		}
		environment, err := backupruntime.BackupOrphanEnvironmentIndexKey(orphan.Target.EnvironmentID, orphan.Target.ID)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range map[string][]byte{backupruntime.BackupOrphanKey(orphan.Target.ID): raw,
			connector: []byte(orphan.Target.ID), environment: []byte(orphan.Target.ID)} {
			store.values[key] = &keyvalue.KeyValue{Key: key, Value: value, Version: 1, ModRevision: 40}
		}
	}
	return store, view, input, current, next, evidence, at
}

type orphanCleanupReadStore struct{ values map[string]*keyvalue.KeyValue }

func (store *orphanCleanupReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	result := &keyvalue.GetManyResult{
		ReadRevision: request.Revision,
		Values:       make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for index, key := range request.Keys {
		if value := store.values[key]; value != nil {
			copy := *value
			copy.Value = append([]byte(nil), value.Value...)
			result.Values[index] = &copy
		}
	}
	return result, nil
}

func (*orphanCleanupReadStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errs.New(errs.KindInternal, "orphan cleanup must use exact captured ownership")
}
