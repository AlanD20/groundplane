package etcd

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: one protected replacement must publish ordered sources, the
// Connector reference, the sealed era-1 key pair, and replay evidence at one revision.
func TestBackupPolicyProtectedReplacementCommitsAgeStateAndReplays(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	candidate := fixture.candidate(t, true, "age")
	marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-age-0001")
	result, err := fixture.repository.replaceBackupPolicyProtected(
		context.Background(), candidate, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("replaceBackupPolicyProtected() = %#v, %v", result, err)
	}
	replayed, err := fixture.repository.replaceBackupPolicyProtected(
		context.Background(), candidate, marker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting ||
		string(replayed.marker.Response.Body) != string(marker.Response.Body) {
		t.Fatalf("replaceBackupPolicyProtected(replay) = %#v, %v", replayed, err)
	}
	stored, found, err := fixture.repository.GetBackupPolicy(context.Background(), fixture.environment.Record.ID)
	if err != nil || !found || len(stored.Record.SourceIDs) != len(candidate.Sources) {
		t.Fatalf("GetBackupPolicy() = %#v, %t, %v", stored, found, err)
	}
	for index := range candidate.Sources {
		if stored.Record.SourceIDs[index] != candidate.Sources[index].Source.Record.ID {
			t.Fatalf("stored source order = %#v", stored.Record.SourceIDs)
		}
	}
	key, found, err := fixture.repository.GetBackupKey(context.Background(), fixture.environment.Record.ID)
	if err != nil || !found || key.Record.KeyEra != 1 ||
		string(key.Encrypted.Ciphertext) != string(candidate.InitialKey.Encrypted.Ciphertext) {
		t.Fatalf("GetBackupKey() = %#v, %t, %v", key, found, err)
	}
	clear(key.Encrypted.Ciphertext)
	assertBackupPolicyReference(t, fixture.store, fixture.connector.Record.Connector.ID,
		fixture.environment.Record.ID, true)
}

// Rationale: encryption none and a disabled policy must never synthesize a
// private identity, while disabling an age policy retains its existing key.
func TestBackupPolicyProtectedReplacementAvoidsAndRetainsKeys(t *testing.T) {
	t.Parallel()
	t.Run("none never creates", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		candidate := fixture.candidate(t, true, "none")
		marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-none-0001")
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate, marker,
		); err != nil {
			t.Fatalf("replaceBackupPolicyProtected(none) error = %v", err)
		}
		if _, found, err := fixture.repository.GetBackupKey(
			context.Background(), fixture.environment.Record.ID,
		); err != nil || found {
			t.Fatalf("GetBackupKey(none) found/error = %t/%v", found, err)
		}
	})

	t.Run("move and disable retain", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		first := fixture.candidate(t, true, "age")
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), first,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-move-0001"),
		); err != nil {
			t.Fatalf("replaceBackupPolicyProtected(first) error = %v", err)
		}
		current := mustBackupPolicy(t, fixture.repository, fixture.environment.Record.ID)
		existingKey := mustBackupKey(t, fixture.repository, fixture.environment.Record.ID)
		defer clear(existingKey.Encrypted.Ciphertext)
		secondConnector := fixture.createConnector(t, 2450, "archive-backups")
		oldReference := mustBackupPolicyReference(
			t, fixture.store, fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
		)
		move := first
		move.MutationEpoch = mustBackupPolicyMutationEpoch(t, fixture.store, fixture.environment.Record.ID)
		move.Current = &current
		move.Replacement.ConnectorID = secondConnector.Record.Connector.ID
		move.Replacement.Encryption = "none"
		move.Sources = append([]backupPolicySourceEvidence(nil), first.Sources[:1]...)
		move.Replacement.SourceIDs = []string{move.Sources[0].Source.Record.ID}
		move.Replacement.UpdatedAt = move.Replacement.UpdatedAt.Add(time.Second)
		move.Connector = &secondConnector
		move.ConnectorOwnerIndex = mustBackupPolicyIndex(
			t,
			fixture.store,
			connectorEnvironmentKey(fixture.environment.Record.ID, secondConnector.Record.Connector.ID),
		)
		move.ConnectorReferences = []backupPolicyConnectorReferenceEvidence{
			{ConnectorID: fixture.connector.Record.Connector.ID, Entry: oldReference},
			{ConnectorID: secondConnector.Record.Connector.ID},
		}
		move.ExistingKey = &existingKey
		move.InitialKey = nil
		move.Coordination = mustBackupPolicyCoordination(t, fixture.store, fixture.environment.Record.ID)
		if err := sealBackupPolicyCandidateSchedule(&move, move.Replacement.UpdatedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), move,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-move-0002"),
		); err != nil {
			t.Fatalf("replaceBackupPolicyProtected(move) error = %v", err)
		}
		assertBackupPolicyReference(t, fixture.store, fixture.connector.Record.Connector.ID,
			fixture.environment.Record.ID, false)
		assertBackupPolicyReference(t, fixture.store, secondConnector.Record.Connector.ID,
			fixture.environment.Record.ID, true)
		retained := mustBackupKey(t, fixture.repository, fixture.environment.Record.ID)
		defer clear(retained.Encrypted.Ciphertext)
		if retained.RecordRevision != existingKey.RecordRevision ||
			retained.EncryptedRevision != existingKey.EncryptedRevision {
			t.Fatalf("move changed retained key revisions: %#v -> %#v", existingKey, retained)
		}
		moved := mustBackupPolicy(t, fixture.repository, fixture.environment.Record.ID)
		newReference := mustBackupPolicyReference(
			t, fixture.store, secondConnector.Record.Connector.ID, fixture.environment.Record.ID,
		)
		disabled := backupPolicyReplacementCandidate{
			Environment:   fixture.environment,
			Project:       fixture.project,
			MutationEpoch: mustBackupPolicyMutationEpoch(t, fixture.store, fixture.environment.Record.ID),
			Current:       &moved,
			Replacement: BackupPolicyRecord{
				EnvironmentID: fixture.environment.Record.ID,
				UpdatedAt:     move.Replacement.UpdatedAt.Add(time.Second),
			},
			ConnectorReferences: []backupPolicyConnectorReferenceEvidence{{
				ConnectorID: secondConnector.Record.Connector.ID, Entry: newReference,
			}},
			ExistingKey: &retained,
		}
		disabled.Coordination = mustBackupPolicyCoordination(t, fixture.store, fixture.environment.Record.ID)
		if err := sealBackupPolicyCandidateSchedule(&disabled, disabled.Replacement.UpdatedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), disabled,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-disable-0001"),
		); err != nil {
			t.Fatalf("replaceBackupPolicyProtected(disabled) error = %v", err)
		}
		assertBackupPolicyReference(t, fixture.store, secondConnector.Record.Connector.ID,
			fixture.environment.Record.ID, false)
		afterDisable := mustBackupKey(t, fixture.repository, fixture.environment.Record.ID)
		defer clear(afterDisable.Encrypted.Ciphertext)
		if afterDisable.RecordRevision != retained.RecordRevision ||
			afterDisable.EncryptedRevision != retained.EncryptedRevision {
			t.Fatalf("disable changed retained key revisions: %#v -> %#v", retained, afterDisable)
		}
	})
}

// Rationale: hierarchy and Connector deletion fences must win the same CAS as
// the policy write, while malformed prevalidated evidence must fail before it.
func TestBackupPolicyProtectedReplacementRejectsFencesScopeAndCorruption(t *testing.T) {
	t.Parallel()
	t.Run("marker scope", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		candidate := fixture.candidate(t, true, "none")
		marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-scope-0001")
		marker.Locator.ScopeID = ids.NewAt(ids.KindEnvironment, marker.CreatedAt, 2500)
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate, marker,
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("replaceBackupPolicyProtected(scope) error = %v", err)
		}
	})

	t.Run("marker operation and retention", func(t *testing.T) {
		for _, mutate := range []func(*IdempotencyMarker){
			func(marker *IdempotencyMarker) { marker.Locator.Route = "/backup-policy" },
			func(marker *IdempotencyMarker) { marker.Response.Status = http.StatusCreated },
			func(marker *IdempotencyMarker) { marker.Response.ContentKind = "text/plain" },
			func(marker *IdempotencyMarker) { marker.Response.Body = []byte("not-json") },
			func(marker *IdempotencyMarker) { marker.RetainUntil = marker.RetainUntil.Add(time.Second) },
		} {
			fixture := newBackupPolicyReplacementFixture(t, true)
			candidate := fixture.candidate(t, true, "none")
			marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-marker-0001")
			mutate(&marker)
			if _, err := fixture.repository.replaceBackupPolicyProtected(
				context.Background(), candidate, marker,
			); !isKind(err, errs.KindValidationFailed) {
				t.Fatalf("replaceBackupPolicyProtected(marker) error = %v", err)
			}
		}
	})

	t.Run("non-ready environment", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		candidate := fixture.candidate(t, true, "none")
		candidate.Environment.Record.ProvisioningState = EnvironmentProvisioningProvisioning
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-not-ready-0001"),
		); !isKind(err, errs.KindStateConflict) {
			t.Fatalf("replaceBackupPolicyProtected(not ready) error = %v", err)
		}
	})

	for _, test := range []struct {
		name string
		kind DeletionTargetKind
		id   func(*backupPolicyReplacementFixture) string
	}{
		{name: "environment fence", kind: DeletionTargetEnvironment,
			id: func(f *backupPolicyReplacementFixture) string { return f.environment.Record.ID }},
		{name: "project fence", kind: DeletionTargetProject,
			id: func(f *backupPolicyReplacementFixture) string { return f.project.Record.ID }},
		{name: "tenant fence", kind: DeletionTargetTenant,
			id: func(f *backupPolicyReplacementFixture) string { return f.project.Record.TenantID }},
		{name: "connector delete race", kind: DeletionTargetConnector,
			id: func(f *backupPolicyReplacementFixture) string { return f.connector.Record.Connector.ID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, true)
			candidate := fixture.candidate(t, true, "none")
			id := test.id(fixture)
			transaction, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
				Type: MutationPut, Key: deletionTombstoneKey(string(test.kind), id), Value: []byte("fenced"),
			}})
			if err != nil || !transaction.Succeeded {
				t.Fatalf("seed deletion fence = %#v, %v", transaction, err)
			}
			result, err := fixture.repository.replaceBackupPolicyProtected(
				context.Background(), candidate,
				backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-fence-0001"),
			)
			if err != nil || result.kind != idempotencyTransactionConflict ||
				!isKind(result.conflict, errs.KindResourceInUse) {
				t.Fatalf("replaceBackupPolicyProtected(fenced) = %#v, %v", result, err)
			}
		})
	}

	t.Run("source order", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		candidate := fixture.candidate(t, true, "age")
		candidate.Sources[0], candidate.Sources[1] = candidate.Sources[1], candidate.Sources[0]
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-order-0001"),
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("replaceBackupPolicyProtected(order) error = %v", err)
		}
	})

	t.Run("corrupt reverse reference", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		first := fixture.candidate(t, true, "none")
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), first,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-corrupt-0001"),
		); err != nil {
			t.Fatalf("replaceBackupPolicyProtected(first) error = %v", err)
		}
		current := mustBackupPolicy(t, fixture.repository, fixture.environment.Record.ID)
		reference := mustBackupPolicyReference(
			t, fixture.store, fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
		)
		reference.Value = []byte("wrong-environment")
		disabled := backupPolicyReplacementCandidate{
			Environment:   fixture.environment,
			Project:       fixture.project,
			MutationEpoch: mustBackupPolicyMutationEpoch(t, fixture.store, fixture.environment.Record.ID),
			Current:       &current,
			Replacement: BackupPolicyRecord{
				EnvironmentID: fixture.environment.Record.ID,
				UpdatedAt:     first.Replacement.UpdatedAt.Add(time.Second),
			},
			ConnectorReferences: []backupPolicyConnectorReferenceEvidence{{
				ConnectorID: fixture.connector.Record.Connector.ID, Entry: reference,
			}},
		}
		disabled.Coordination = mustBackupPolicyCoordination(t, fixture.store, fixture.environment.Record.ID)
		if err := sealBackupPolicyCandidateSchedule(&disabled, disabled.Replacement.UpdatedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), disabled,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-corrupt-0002"),
		); !isKind(err, errs.KindInternal) {
			t.Fatalf("replaceBackupPolicyProtected(corrupt ref) error = %v", err)
		}
	})

	t.Run("config with none", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		candidate := fixture.candidate(t, true, "none")
		candidate.Sources = append([]backupPolicySourceEvidence(nil), fixture.sources...)
		candidate.Replacement.SourceIDs = make([]string, len(candidate.Sources))
		for index := range candidate.Sources {
			candidate.Replacement.SourceIDs[index] = candidate.Sources[index].Source.Record.ID
		}
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-config-none-0001"),
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("replaceBackupPolicyProtected(config none) error = %v", err)
		}
	})

	t.Run("disabled config still requires age", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, false)
		candidate := fixture.candidate(t, true, "age")
		candidate.Replacement.Enabled = false
		candidate.Replacement.Encryption = "none"
		candidate.Connector = nil
		candidate.ConnectorOwnerIndex = nil
		candidate.ConnectorReferences = nil
		candidate.InitialKey = nil
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-disabled-config-0001"),
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("replaceBackupPolicyProtected(disabled config) error = %v", err)
		}
	})

	t.Run("duplicate source identity", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, false)
		candidate := fixture.candidate(t, true, "age")
		duplicate := candidate.Sources[0]
		duplicate.Source.Record.ID = ids.NewAt(ids.KindBackupSource, fixture.now, 2550)
		duplicate.Source.Revision++
		duplicate.Source.ReadRevision++
		candidate.Sources = append(candidate.Sources, duplicate)
		candidate.Replacement.SourceIDs = append(candidate.Replacement.SourceIDs, duplicate.Source.Record.ID)
		if _, err := fixture.repository.replaceBackupPolicyProtected(
			context.Background(), candidate,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-duplicate-0001"),
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("replaceBackupPolicyProtected(duplicate) error = %v", err)
		}
	})

	for _, test := range []struct {
		name string
		key  func(backupPolicyReplacementCandidate) string
	}{
		{name: "source environment index", key: func(candidate backupPolicyReplacementCandidate) string {
			return candidate.Sources[0].EnvironmentIndex.Key
		}},
		{name: "source identity index", key: func(candidate backupPolicyReplacementCandidate) string {
			return candidate.Sources[0].IdentityIndex.Key
		}},
		{name: "volume projection root", key: func(candidate backupPolicyReplacementCandidate) string {
			return environmentBlueprintRootKey(
				candidate.Replacement.EnvironmentID,
				candidate.Sources[0].Volume.Projection.Record.RevisionID,
			)
		}},
		{name: "connector owner index", key: func(candidate backupPolicyReplacementCandidate) string {
			return candidate.ConnectorOwnerIndex.Key
		}},
	} {
		t.Run(test.name+" corruption", func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, true)
			candidate := fixture.candidate(t, true, "none")
			key := test.key(candidate)
			transaction, err := fixture.store.Transact(
				context.Background(), nil, []Mutation{{Type: MutationDelete, Key: key}},
			)
			if err != nil || !transaction.Succeeded {
				t.Fatalf("delete index = %#v, %v", transaction, err)
			}
			result, err := fixture.repository.replaceBackupPolicyProtected(
				context.Background(), candidate,
				backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-index-race-0001"),
			)
			if err != nil || result.kind != idempotencyTransactionConflict || result.conflict == nil {
				t.Fatalf("replaceBackupPolicyProtected(index corruption) = %#v, %v", result, err)
			}
		})
	}
}

// Rationale: the Controller-owned UTC grammar must accept only exact daily or
// weekly boundary clocks and reject every broader systemd calendar feature.
func TestBackupPolicyFrequencyGrammarBoundaries(t *testing.T) {
	t.Parallel()
	for _, frequency := range []string{
		"*-*-* 00:00:00", "*-*-* 23:59:59", "Mon *-*-* 00:00:00", "Sun *-*-* 23:59:59",
	} {
		if err := validateBackupPolicyFrequency(frequency); err != nil {
			t.Fatalf("validateBackupPolicyFrequency(%q) error = %v", frequency, err)
		}
	}
	for _, frequency := range []string{
		"0 3 * * *", "*-*-* 24:00:00", "*-*-* 23:60:00", "*-*-* 23:59:60",
		"*-*-* 3:00:00", "mon *-*-* 03:00:00", "Mon,Tue *-*-* 03:00:00",
		"Mon  *-*-* 03:00:00", "*-*-* 03:00:00 UTC", "2026-08-23 03:00:00",
	} {
		if err := validateBackupPolicyFrequency(frequency); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("validateBackupPolicyFrequency(%q) error = %v", frequency, err)
		}
	}
}

// Rationale: an ambiguous transport result must be returned unchanged and a
// later exact retry must resolve exclusively through the committed marker.
func TestBackupPolicyProtectedReplacementPreservesUnknownOutcomeAndConflicts(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, false)
	candidate := fixture.candidate(t, true, "age")
	unknown := errs.New(errs.KindStorageUnavailable, "unknown backup policy write outcome")
	store := &backupPolicyReplacementUnknownStore{memoryHierarchyStore: fixture.store, failNext: unknown}
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-unknown-0001")
	if _, err := repository.replaceBackupPolicyProtected(
		context.Background(), candidate, marker,
	); !errors.Is(err, unknown) {
		t.Fatalf("replaceBackupPolicyProtected(unknown) error = %v", err)
	}
	replayed, err := repository.replaceBackupPolicyProtected(context.Background(), candidate, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("replaceBackupPolicyProtected(after unknown) = %#v, %v", replayed, err)
	}
	conflict, err := repository.replaceBackupPolicyProtected(
		context.Background(), candidate,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-conflict-0001"),
	)
	if err != nil || conflict.kind != idempotencyTransactionConflict ||
		!isKind(conflict.conflict, errs.KindStateConflict) {
		t.Fatalf("replaceBackupPolicyProtected(stale) = %#v, %v", conflict, err)
	}
}

// Rationale: a canonical Environment operation lock acquired after preparation
// must fence policy publication without allowing any replacement write.
func TestBackupPolicyProtectedReplacementRejectsHeldEnvironmentOperationLockWithoutWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newBackupPolicyReplacementFixture(t, false)
	candidate := fixture.candidate(t, true, "age")
	transaction, err := fixture.store.Transact(ctx, nil, []Mutation{{
		Type:  MutationPut,
		Key:   environmentOperationLockKey(fixture.environment.Record.ID),
		Value: []byte("held"),
	}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed Environment operation lock = %#v, %v", transaction, err)
	}
	revisionBefore := fixture.store.revision
	result, err := fixture.repository.replaceBackupPolicyProtected(
		ctx,
		candidate,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-held-lock-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindResourceInUse) {
		t.Fatalf("replaceBackupPolicyProtected(held lock) = %#v, %v", result, err)
	}
	if fixture.store.revision != revisionBefore {
		t.Fatalf("held-lock replacement revision = %d, want unchanged %d", fixture.store.revision, revisionBefore)
	}
}

// Rationale: preparation is valid only for its exact Environment coordination
// revision; advancing that fence must reject the stale candidate without writes.
func TestBackupPolicyProtectedReplacementRejectsAdvancedCoordinationWithoutWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newBackupPolicyReplacementFixture(t, false)
	candidate := fixture.candidate(t, true, "age")
	coordinationValue, err := encodeEnvironmentCoordinationRecord(candidate.Coordination.Record)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := fixture.store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentCoordinationKey(fixture.environment.Record.ID),
		Value: coordinationValue,
	}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("advance Environment coordination = %#v, %v", transaction, err)
	}
	revisionBefore := fixture.store.revision
	result, err := fixture.repository.replaceBackupPolicyProtected(
		ctx,
		candidate,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-stale-coordination-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindStateConflict) {
		t.Fatalf("replaceBackupPolicyProtected(stale epoch) = %#v, %v", result, err)
	}
	if fixture.store.revision != revisionBefore {
		t.Fatalf("stale-coordination replacement revision = %d, want unchanged %d", fixture.store.revision, revisionBefore)
	}
}

// Rationale: even a prevalidated candidate must be rejected before etcd when
// its source compares and protected evidence exceed the 96-operation ceiling.
func TestBackupPolicyProtectedReplacementEnforcesTransactionBound(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, false)
	candidate := fixture.candidate(t, true, "age")
	candidate.Sources = make([]backupPolicySourceEvidence, 90)
	candidate.Replacement.SourceIDs = make([]string, len(candidate.Sources))
	for index := range candidate.Sources {
		sourceID := ids.NewAt(ids.KindBackupSource, candidate.Replacement.UpdatedAt, int64(2600+index))
		volumeID := ids.NewAt(ids.KindVolume, candidate.Replacement.UpdatedAt, int64(2700+index))
		record := BackupSourceRecord{
			ID: sourceID, EnvironmentID: fixture.environment.Record.ID,
			Kind: core.BackupSourceVolume, TargetID: volumeID,
			CreatedAt: candidate.Replacement.UpdatedAt,
		}
		candidate.Replacement.SourceIDs[index] = sourceID
		candidate.Sources[index] = backupPolicySourceEvidence{Source: Versioned[BackupSourceRecord]{
			Record: record, Revision: 1, ReadRevision: 1,
		}, EnvironmentIndex: &KeyValue{
			Key:   backupSourceEnvironmentKey(fixture.environment.Record.ID, sourceID),
			Value: []byte(sourceID), ModRevision: 1,
		}, IdentityIndex: &KeyValue{
			Key:   backupSourceIdentityKey(fixture.environment.Record.ID, record.Kind, record.TargetID),
			Value: []byte(sourceID), ModRevision: 1,
		}, Volume: syntheticBackupPolicyVolumeEvidence(
			fixture.environment.Record.ID, volumeID, candidate.Replacement.UpdatedAt, int64(2800+index),
		)}
	}
	if _, err := fixture.repository.replaceBackupPolicyProtected(
		context.Background(), candidate,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-bound-0001"),
	); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("replaceBackupPolicyProtected(bound) error = %v", err)
	}
}

type backupPolicyReplacementFixture struct {
	repository    *BackupPolicyRepository
	store         *memoryHierarchyStore
	environment   Versioned[EnvironmentRecord]
	project       Versioned[ProjectRecord]
	mutationEpoch Versioned[EnvironmentMutationEpochRecord]
	sources       []backupPolicySourceEvidence
	connector     Versioned[ConnectorRecord]
	now           time.Time
}

func newBackupPolicyReplacementFixture(t *testing.T, includeVolume bool) *backupPolicyReplacementFixture {
	t.Helper()
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 2300)
	if _, err := hierarchy.CreateTenant(context.Background(), TenantRecord{
		ID: tenantID, Slug: "backup-tenant", Name: "Backup Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(context.Background(), ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2301), TenantID: tenantID,
		Slug: "backup-project", Name: "Backup Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2302)
	environment, err := hierarchy.CreateEnvironment(context.Background(), EnvironmentRecord{
		ID: environmentID, ProjectID: project.Record.ID,
		Name: "production", NetworkPool: "10.240.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 2303), CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	repository.now = func() time.Time { return now }
	configSource, err := repository.EnsureBackupSource(
		context.Background(), environment, project, core.BackupSourceConfig, environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(config) error = %v", err)
	}
	sources := []backupPolicySourceEvidence{backupPolicyReplacementSourceEvidence(
		t, store, configSource, nil, nil,
	)}
	if includeVolume {
		volume := EnvironmentVolumeIdentity{
			ID: ids.NewAt(ids.KindVolume, now, 2400), Slug: "backup-data", Key: "backup-data",
		}
		createdVolume := seedBackupPolicyVolumeProjection(
			t, store, environment, project, volume, 2401,
		)
		volumeSource, err := repository.EnsureBackupSource(
			context.Background(), environment, project, core.BackupSourceVolume, volume.ID,
		)
		if err != nil {
			t.Fatalf("EnsureBackupSource(volume) error = %v", err)
		}
		sources = []backupPolicySourceEvidence{
			backupPolicyReplacementSourceEvidence(t, store, volumeSource, nil, &createdVolume),
			backupPolicyReplacementSourceEvidence(t, store, configSource, nil, nil),
		}
	}
	fixture := &backupPolicyReplacementFixture{
		repository: repository, store: store, environment: environment, project: project,
		sources: sources, now: now,
	}
	fixture.connector = fixture.createConnector(t, 2410, "primary-backups")
	fixture.mutationEpoch = mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	return fixture
}

func (fixture *backupPolicyReplacementFixture) createConnector(
	t *testing.T,
	seed int64,
	name string,
) Versioned[ConnectorRecord] {
	t.Helper()
	repository, err := newConnectorRepository(fixture.store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	record := testConnectorRecord(t, fixture.environment.Record.ID, fixture.now, seed, name)
	credentials, err := NewConnectorEncryptedCredentials(record.Connector.ID, []byte("sealed-connector-credentials"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	created, err := repository.CreateConnector(
		context.Background(), fixture.environment, fixture.project, record, credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	return created
}

func (fixture *backupPolicyReplacementFixture) candidate(
	t *testing.T,
	enabled bool,
	encryption string,
) backupPolicyReplacementCandidate {
	t.Helper()
	policy := BackupPolicyRecord{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       enabled,
		UpdatedAt:     fixture.now,
	}
	coordinationValue, err := fixture.store.Get(
		context.Background(), environmentCoordinationKey(fixture.environment.Record.ID),
	)
	if err != nil || coordinationValue == nil {
		t.Fatalf("get Environment coordination = %#v, %v", coordinationValue, err)
	}
	coordination := EnvironmentCoordinationRecord{
		EnvironmentID: fixture.environment.Record.ID, ScheduleClockFloor: fixture.now,
	}
	coordinationRevision := int64(0)
	if coordinationValue.Entry != nil {
		coordination, err = decodeEnvironmentCoordinationRecord(coordinationValue.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		coordinationRevision = coordinationValue.Entry.ModRevision
	}
	candidate := backupPolicyReplacementCandidate{
		Environment: fixture.environment, Project: fixture.project,
		MutationEpoch: fixture.mutationEpoch, Replacement: policy,
		Coordination: Versioned[EnvironmentCoordinationRecord]{
			Record: coordination, Revision: coordinationRevision,
			ReadRevision: coordinationValue.ReadRevision,
		},
	}
	if !enabled {
		if err := sealBackupPolicyCandidateSchedule(&candidate, fixture.now); err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	policy.Frequency = "*-*-* 03:00:00"
	policy.Keep = 7
	policy.Encryption = encryption
	policy.ConnectorID = fixture.connector.Record.Connector.ID
	selectedSources := fixture.sources
	if encryption == "none" {
		selectedSources = nil
		for _, source := range fixture.sources {
			if string(source.Source.Record.Kind) != "config" {
				selectedSources = append(selectedSources, source)
			}
		}
		if len(selectedSources) == 0 {
			t.Fatal("none-encrypted test candidate requires a non-config source")
		}
	}
	policy.SourceIDs = make([]string, len(selectedSources))
	for index := range selectedSources {
		policy.SourceIDs[index] = selectedSources[index].Source.Record.ID
	}
	candidate.Replacement = policy
	candidate.Sources = append([]backupPolicySourceEvidence(nil), selectedSources...)
	connector := fixture.connector
	candidate.Connector = &connector
	candidate.ConnectorOwnerIndex = mustBackupPolicyIndex(
		t,
		fixture.store,
		connectorEnvironmentKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.ID),
	)
	candidate.ConnectorReferences = []backupPolicyConnectorReferenceEvidence{{
		ConnectorID: fixture.connector.Record.Connector.ID,
	}}
	if encryption == "age" {
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatalf("age.GenerateX25519Identity() error = %v", err)
		}
		candidate.InitialKey = &backupPolicyInitialKey{
			Record: BackupKeyRecord{
				EnvironmentID: fixture.environment.Record.ID,
				Recipient:     identity.Recipient().String(), KeyEra: 1,
				CreatedAt: fixture.now, RotatedAt: fixture.now,
			},
			Encrypted: BackupKeyEncryptedValue{
				EnvironmentID: fixture.environment.Record.ID,
				KeyEra:        1, Ciphertext: []byte("controller-sealed-age-identity"),
			},
		}
	}
	if err := sealBackupPolicyCandidateSchedule(&candidate, fixture.now); err != nil {
		t.Fatal(err)
	}
	return candidate
}

func backupPolicyReplacementMarker(environmentID string, key string) IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPut,
		Route:     backupPolicyReplacementRoute,
		Key:       key,
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"status":"saved"}`),
	}
	return marker
}

func backupPolicyReplacementSourceEvidence(
	t *testing.T,
	store *memoryHierarchyStore,
	source Versioned[BackupSourceRecord],
	attach *Versioned[AttachRecord],
	volume *backupVolumeProjectionEvidence,
) backupPolicySourceEvidence {
	t.Helper()
	evidence := backupPolicySourceEvidence{
		Source: source,
		EnvironmentIndex: mustBackupPolicyIndex(
			t, store, backupSourceEnvironmentKey(source.Record.EnvironmentID, source.Record.ID),
		),
		IdentityIndex: mustBackupPolicyIndex(
			t,
			store,
			backupSourceIdentityKey(source.Record.EnvironmentID, source.Record.Kind, source.Record.TargetID),
		),
		Attach: attach,
		Volume: volume,
	}
	if attach != nil {
		evidence.TargetOwnerIndex = mustBackupPolicyIndex(
			t, store, attachOwnerKey(source.Record.EnvironmentID, attach.Record.ID),
		)
	}
	return evidence
}

func seedBackupPolicyVolumeProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	volume EnvironmentVolumeIdentity,
	seed int64,
) backupVolumeProjectionEvidence {
	t.Helper()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, seed)
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	projection.Volumes = []EnvironmentVolumeIdentity{volume}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.Volumes = []*agentpb.ComposeVolume{{VolumeId: volume.ID, ComposeName: volume.Key}}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0, revision, projection, marker)
	headValue, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: environmentBlueprintHeadKey(environment.Record.ID), ModRevision: 0,
	}}, []Mutation{{
		Type: MutationPut, Key: environmentBlueprintHeadKey(environment.Record.ID), Value: headValue,
	}})
	clear(headValue)
	if err != nil || !result.Succeeded {
		t.Fatalf("publish backup Volume fixture = %#v, %v", result, err)
	}
	evidence, err := loadBackupVolumeProjectionEvidence(
		context.Background(), store, environment.Record.ID, volume.ID, result.Revision,
	)
	if err != nil {
		t.Fatalf("load backup Volume fixture = %v", err)
	}
	return evidence
}

func syntheticBackupPolicyVolumeEvidence(
	environmentID string,
	volumeID string,
	at time.Time,
	seed int64,
) *backupVolumeProjectionEvidence {
	revisionID := ids.NewAt(ids.KindTask, at, seed)
	return &backupVolumeProjectionEvidence{
		Projection: Versioned[EnvironmentComposeProjection]{
			Record: EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
				Volumes: []EnvironmentVolumeIdentity{{
					ID: volumeID, Slug: "volume-" + volumeID, Key: "volume-" + volumeID,
				}},
			},
			Revision: 1, ReadRevision: 1,
		},
		ProjectionRoot: 1, DependencyDigest: strings.Repeat("a", 64),
		Volume: EnvironmentVolumeIdentity{
			ID: volumeID, Slug: "volume-" + volumeID, Key: "volume-" + volumeID,
		},
	}
}

func mustBackupPolicyIndex(t *testing.T, store *memoryHierarchyStore, key string) *KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("index %q read = %#v, %v", key, result, err)
	}
	return result.Entry
}

func mustBackupPolicy(
	t *testing.T,
	repository *BackupPolicyRepository,
	environmentID string,
) Versioned[BackupPolicyRecord] {
	t.Helper()
	record, found, err := repository.GetBackupPolicy(context.Background(), environmentID)
	if err != nil || !found {
		t.Fatalf("GetBackupPolicy() = %#v, %t, %v", record, found, err)
	}
	return record
}

func mustBackupKey(
	t *testing.T,
	repository *BackupPolicyRepository,
	environmentID string,
) VersionedBackupKey {
	t.Helper()
	key, found, err := repository.GetBackupKey(context.Background(), environmentID)
	if err != nil || !found {
		t.Fatalf("GetBackupKey() = %#v, %t, %v", key, found, err)
	}
	return key
}

func mustBackupPolicyReference(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	environmentID string,
) *KeyValue {
	t.Helper()
	result, err := store.Get(
		context.Background(), backupPolicyConnectorReferenceKey(connectorID, environmentID),
	)
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("Connector reference read = %#v, %v", result, err)
	}
	return result.Entry
}

func assertBackupPolicyReference(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	environmentID string,
	want bool,
) {
	t.Helper()
	result, err := store.Get(
		context.Background(), backupPolicyConnectorReferenceKey(connectorID, environmentID),
	)
	if err != nil || result == nil || (result.Entry != nil) != want {
		t.Fatalf("Connector reference present = %t, want %t; result/error = %#v/%v",
			result != nil && result.Entry != nil, want, result, err)
	}
}

type backupPolicyReplacementUnknownStore struct {
	*memoryHierarchyStore
	failNext error
}

func (store *backupPolicyReplacementUnknownStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return TransactionResult{}, failure
	}
	return result, err
}

func replaceBackupSchedulePolicy(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	input BackupPolicyReplacementInput,
	at time.Time,
	key string,
) BackupPolicyProjection {
	t.Helper()
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(), input,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Destroy()
	prepared, err = prepared.FinalizeSchedule(at)
	if err != nil {
		t.Fatal(err)
	}
	projection := prepared.Projection()
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		context.Background(),
		prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, key),
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected() = %#v, %v", result, err)
	}
	return projection
}

func backupSchedulePolicyInput(
	fixture *backupPolicyReplacementFixture,
	enabled bool,
) BackupPolicyReplacementInput {
	volume := fixture.sources[0].Source.Record
	return BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       enabled, Frequency: "*-*-* 03:00:00", Keep: 7,
		Encryption: "none", ConnectorID: fixture.connector.Record.Connector.ID,
		Sources: []BackupPolicySourceSelection{{
			Kind: core.BackupSourceVolume, TargetID: volume.TargetID,
		}},
	}
}

// Rationale: the policy, nullable next_run_at source state, and coordination
// transition are one protected commit; a delayed first tick still selects only
// the latest occurrence since that exact transition boundary.
func TestBackupPolicyReplacementPublishesScheduleAtomicallyAndLatestCatchUp(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	transition := fixture.now.Add(time.Minute)
	projection := replaceBackupSchedulePolicy(
		t, fixture, backupSchedulePolicyInput(fixture, true), transition,
		"backup-schedule-atomic-0001",
	)
	wantNext := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)
	if projection.NextRunAt == nil || !projection.NextRunAt.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %s", projection.NextRunAt, wantNext)
	}
	read, err := fixture.store.GetMany(context.Background(), GetManyRequest{Keys: []string{
		backupPolicyKey(fixture.environment.Record.ID),
		environmentCoordinationKey(fixture.environment.Record.ID),
	}})
	if err != nil || read == nil || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].ModRevision != read.Values[1].ModRevision {
		t.Fatalf("atomic policy/coordination read = %#v, %v", read, err)
	}
	runtime, err := newBackupRuntimeRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	delayed := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluation, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, delayed,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantLatest := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)
	if !evaluation.Due || !evaluation.ScheduledAt.Equal(wantLatest) ||
		!evaluation.EvaluatedAt.Equal(delayed) {
		t.Fatalf("delayed evaluation = %#v, want latest %s", evaluation, wantLatest)
	}
}

// Rationale: disable clears only schedule state while advancing the monotonic
// floor, and re-enable seeds strictly from its own later transition boundary.
func TestBackupPolicyDisableReenableCannotReplayDisabledOccurrences(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	input := backupSchedulePolicyInput(fixture, true)
	replaceBackupSchedulePolicy(
		t, fixture, input, fixture.now.Add(time.Minute),
		"backup-schedule-enable-0001",
	)
	disabledAt := fixture.now.Add(48 * time.Hour)
	input.Enabled = false
	disabled := replaceBackupSchedulePolicy(
		t, fixture, input, disabledAt,
		"backup-schedule-disable-0001",
	)
	if disabled.NextRunAt != nil {
		t.Fatalf("disabled next_run_at = %v", disabled.NextRunAt)
	}
	reenabledAt := disabledAt.Add(2 * time.Hour)
	input.Enabled = true
	reenabled := replaceBackupSchedulePolicy(
		t, fixture, input, reenabledAt,
		"backup-schedule-reenable-0001",
	)
	wantNext := time.Date(2026, 8, 26, 3, 0, 0, 0, time.UTC)
	if reenabled.NextRunAt == nil || !reenabled.NextRunAt.Equal(wantNext) {
		t.Fatalf("re-enabled next_run_at = %v, want %s", reenabled.NextRunAt, wantNext)
	}
	runtime, err := newBackupRuntimeRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	beforeNext, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, reenabledAt.Add(30*time.Minute),
	)
	if err != nil || beforeNext.Due {
		t.Fatalf("re-enabled pre-occurrence evaluation = %#v, %v", beforeNext, err)
	}
}

// Rationale: a held Environment operation lock publishes one immutable overlap
// outcome with coordination progress and never creates a competing Task.
func TestBackupScheduleOverlapPublishesOutcomeAtomically(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	replaceBackupSchedulePolicy(
		t, fixture, backupSchedulePolicyInput(fixture, true), fixture.now,
		"backup-schedule-overlap-0001",
	)
	lock, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
		Type:  MutationPut,
		Key:   environmentOperationLockKey(fixture.environment.Record.ID),
		Value: []byte("held"),
	}})
	if err != nil || !lock.Succeeded {
		t.Fatalf("seed operation lock = %#v, %v", lock, err)
	}
	runtime, err := newBackupRuntimeRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 4, 0, 0, 0, time.UTC)
	evaluation, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, now,
	)
	if err != nil || !evaluation.Due || !evaluation.Overlap {
		t.Fatalf("overlap evaluation = %#v, %v", evaluation, err)
	}
	if err := runtime.SkipScheduledBackup(context.Background(), evaluation, now); err != nil {
		t.Fatal(err)
	}
	dueKey, err := backupDueOutcomeKey(
		evaluation.EnvironmentID, evaluation.PolicyRevision, evaluation.ScheduledAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	dueValue, err := fixture.store.Get(context.Background(), dueKey)
	if err != nil || dueValue == nil || dueValue.Entry == nil {
		t.Fatalf("due outcome read = %#v, %v", dueValue, err)
	}
	due, err := decodeBackupDueOutcomeRecord(dueValue.Entry.Value)
	if err != nil || due.Outcome != BackupDueSkippedOverlap || due.TaskID != "" {
		t.Fatalf("overlap due outcome = %#v, %v", due, err)
	}
	coordinationValue, err := fixture.store.Get(
		context.Background(), environmentCoordinationKey(evaluation.EnvironmentID),
	)
	if err != nil || coordinationValue == nil || coordinationValue.Entry == nil ||
		coordinationValue.Entry.ModRevision != dueValue.Entry.ModRevision {
		t.Fatalf("atomic overlap coordination = %#v, %v", coordinationValue, err)
	}
}

func mustBackupPolicyCoordination(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) Versioned[EnvironmentCoordinationRecord] {
	t.Helper()
	value, err := store.Get(context.Background(), environmentCoordinationKey(environmentID))
	if err != nil || value == nil || value.Entry == nil {
		t.Fatalf("get Environment coordination = %#v, %v", value, err)
	}
	record, err := decodeEnvironmentCoordinationRecord(value.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	return Versioned[EnvironmentCoordinationRecord]{
		Record: record, Revision: value.Entry.ModRevision, ReadRevision: value.ReadRevision,
	}
}
