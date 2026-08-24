package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an absent singleton is the accepted disabled and wholly
// unconfigured state, not a not-found response or a synthetic durable record.
func TestBackupPolicyProjectionUsesEffectiveDisabledAbsence(t *testing.T) {
	t.Parallel()
	repository, _, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	projection, err := repository.GetBackupPolicyProjection(context.Background(), environment.Record.ID)
	if err != nil {
		t.Fatalf("GetBackupPolicyProjection() error = %v", err)
	}
	if projection.EnvironmentID != environment.Record.ID || projection.Enabled || projection.Frequency != "" ||
		projection.Keep != 0 || projection.Encryption != "" || projection.ConnectorID != "" ||
		projection.Sources == nil || len(projection.Sources) != 0 || projection.AgeRecipient != "" ||
		projection.KeyEra != 0 || !projection.KeyCreatedAt.IsZero() || !projection.KeyRotatedAt.IsZero() {
		t.Fatalf("GetBackupPolicyProjection(absent) = %#v", projection)
	}
}

// Rationale: the typed preparation boundary must preserve source order and
// stable ids while keeping raw MVCC evidence outside the application layer.
func TestBackupPolicyTypedPreparationCommitsStableProjection(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	input := BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       true,
		Frequency:     "Mon *-*-* 03:00:00",
		Keep:          7,
		Encryption:    "age",
		ConnectorID:   fixture.connector.Record.Connector.ID,
		Sources: []BackupPolicySourceSelection{
			{Kind: fixture.sources[1].Source.Record.Kind, TargetID: fixture.sources[1].Source.Record.TargetID},
			{Kind: fixture.sources[0].Source.Record.Kind, TargetID: fixture.sources[0].Source.Record.TargetID},
		},
	}
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		input,
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	defer prepared.Destroy()
	prepared, err = fixture.repository.SupplyBackupPolicyInitialKey(
		context.Background(),
		prepared,
		BackupPolicyInitialKeyMaterial{
			Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-age-identity"),
		},
	)
	if err != nil {
		t.Fatalf("SupplyBackupPolicyInitialKey() error = %v", err)
	}
	projection := prepared.Projection()
	if len(projection.Sources) != 2 || projection.Sources[0].ID != fixture.sources[1].Source.Record.ID ||
		projection.Sources[1].ID != fixture.sources[0].Source.Record.ID || projection.KeyEra != 1 ||
		projection.AgeRecipient != identity.Recipient().String() {
		t.Fatalf("Projection() = %#v", projection)
	}
	projection.Sources[0].ID = "mutated"
	if prepared.Projection().Sources[0].ID != fixture.sources[1].Source.Record.ID {
		t.Fatal("Projection() aliased prepared source state")
	}
	marker := backupPolicyReplacementMarker(
		fixture.environment.Record.ID,
		"backup-policy-typed-0001",
	)
	result, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), prepared, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected() = %#v, %v", result, err)
	}
	stored, err := fixture.repository.GetBackupPolicyProjection(
		context.Background(),
		fixture.environment.Record.ID,
	)
	if err != nil || len(stored.Sources) != 2 || stored.Sources[0].ID != fixture.sources[1].Source.Record.ID ||
		stored.Sources[1].ID != fixture.sources[0].Source.Record.ID || stored.AgeRecipient != projection.AgeRecipient {
		t.Fatalf("GetBackupPolicyProjection(stored) = %#v, %v", stored, err)
	}
	deleted, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationDelete,
		Key: backupSourceIdentityKey(
			stored.EnvironmentID,
			stored.Sources[0].Kind,
			stored.Sources[0].TargetID,
		),
	}})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete source identity index = %#v, %v", deleted, err)
	}
	if _, err := fixture.repository.GetBackupPolicyProjection(
		context.Background(),
		fixture.environment.Record.ID,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("GetBackupPolicyProjection(corrupt identity) error = %v", err)
	}
}

// Rationale: retained disabled configuration is operator intent, not a live
// Connector dependency; re-enabling must establish that dependency anew.
func TestBackupPolicyDisabledReplacementRetainsDeletedConnectorWithoutFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newBackupPolicyReplacementFixture(t, true)
	connectorID := fixture.connector.Record.Connector.ID
	deleted, err := fixture.store.Transact(ctx, nil, []Mutation{
		{Type: MutationDelete, Key: connectorRecordKey(connectorID)},
		{
			Type: MutationDelete,
			Key:  connectorEnvironmentKey(fixture.environment.Record.ID, connectorID),
		},
		{
			Type:  MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetConnector), connectorID),
			Value: []byte("deleted"),
		},
	})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete Connector fixture = %#v, %v", deleted, err)
	}
	volume := fixture.sources[0].Source.Record
	input := BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Frequency:     "*-*-* 03:00:00",
		Keep:          7,
		Encryption:    "none",
		ConnectorID:   connectorID,
		Sources: []BackupPolicySourceSelection{{
			Kind: volume.Kind, TargetID: volume.TargetID,
		}},
	}
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(ctx, input)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement(disabled) error = %v", err)
	}
	defer prepared.Destroy()
	if projection := prepared.Projection(); projection.Enabled || projection.ConnectorID != connectorID {
		t.Fatalf("disabled Projection() = %#v", projection)
	}
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		ctx,
		prepared,
		backupPolicyReplacementMarker(
			fixture.environment.Record.ID, "backup-policy-deleted-connector-disabled-0001",
		),
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected(disabled) = %#v, %v", result, err)
	}
	stored, err := fixture.repository.GetBackupPolicyProjection(ctx, fixture.environment.Record.ID)
	if err != nil || stored.Enabled || stored.ConnectorID != connectorID {
		t.Fatalf("GetBackupPolicyProjection(disabled) = %#v, %v", stored, err)
	}
	assertBackupPolicyReference(
		t, fixture.store, connectorID, fixture.environment.Record.ID, false,
	)
	input.Enabled = true
	if _, err := fixture.repository.PrepareBackupPolicyReplacement(ctx, input); !isKind(
		err, errs.KindConnectorNotFound,
	) && !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("PrepareBackupPolicyReplacement(re-enable) error = %v", err)
	}
}

// Rationale: rejecting a foreign target before source ensure prevents an
// invalid Environment/target identity tuple from entering the stable catalog.
func TestBackupPolicyTypedPreparationRejectsForeignTargetBeforeCatalogCreation(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, false)
	otherEnvironmentID := ids.NewAt(ids.KindEnvironment, fixture.now, 2900)
	volume, err := NewVolumeRecord(
		otherEnvironmentID,
		ids.NewAt(ids.KindVolume, fixture.now, 2901),
		"foreign-data",
	)
	if err != nil {
		t.Fatalf("NewVolumeRecord() error = %v", err)
	}
	value, err := encodeVolumeRecord(volume)
	if err != nil {
		t.Fatalf("encodeVolumeRecord() error = %v", err)
	}
	defer clear(value)
	transaction, err := fixture.store.Transact(context.Background(), []Condition{
		{Key: volumeKey(volume.ID)},
		{Key: volumeOwnerKey(otherEnvironmentID, volume.ID)},
	}, []Mutation{
		{Type: MutationPut, Key: volumeKey(volume.ID), Value: value},
		{Type: MutationPut, Key: volumeOwnerKey(otherEnvironmentID, volume.ID), Value: []byte(volume.ID)},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed foreign Volume = %#v, %v", transaction, err)
	}
	_, err = fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []BackupPolicySourceSelection{{
				Kind: core.BackupSourceVolume, TargetID: volume.ID,
			}},
		},
	)
	if !isKind(err, errs.KindScopeUnauthorized) {
		t.Fatalf("PrepareBackupPolicyReplacement(foreign) error = %v", err)
	}
	index, readErr := fixture.store.Get(context.Background(), backupSourceIdentityKey(
		fixture.environment.Record.ID,
		core.BackupSourceVolume,
		volume.ID,
	))
	if readErr != nil || index == nil || index.Entry != nil {
		t.Fatalf("foreign source catalog entry = %#v, %v", index, readErr)
	}
}

// Rationale: selected source ownership is part of the same final CAS as the
// policy, so an owner-index change after preparation must lose atomically.
func TestBackupPolicyTypedReplacementFencesSourceOwnerRace(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	volumeSource := fixture.sources[0].Source.Record
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []BackupPolicySourceSelection{{
				Kind: volumeSource.Kind, TargetID: volumeSource.TargetID,
			}},
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	defer prepared.Destroy()
	deleted, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationDelete,
		Key:  volumeOwnerKey(fixture.environment.Record.ID, volumeSource.TargetID),
	}})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete Volume owner index = %#v, %v", deleted, err)
	}
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		context.Background(),
		prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-owner-race-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindInternal) {
		t.Fatalf("ReplaceBackupPolicyProtected(owner race) = %#v, %v", result, err)
	}
}

// Rationale: an enabled policy is readable only while its Connector primary,
// Environment owner index, reverse fence, and deletion state agree at the
// policy's fixed MVCC revision.
func TestBackupPolicyProjectionRejectsEnabledConnectorInconsistency(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		wantKind errs.Kind
		mutate   func(*testing.T, *backupPolicyReplacementFixture)
	}{
		{
			name: "missing primary", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationDelete, Key: connectorRecordKey(fixture.connector.Record.Connector.ID),
				})
			},
		},
		{
			name: "corrupt primary", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationPut, Key: connectorRecordKey(fixture.connector.Record.Connector.ID),
					Value: []byte("corrupt"),
				})
			},
		},
		{
			name: "cross owner", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				record := fixture.connector.Record
				record.Connector.EnvironmentID = ids.NewAt(ids.KindEnvironment, fixture.now, 3100)
				value, err := encodeConnectorRecord(record)
				if err != nil {
					t.Fatalf("encodeConnectorRecord(cross owner) error = %v", err)
				}
				defer clear(value)
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationPut, Key: connectorRecordKey(fixture.connector.Record.Connector.ID), Value: value,
				})
			},
		},
		{
			name: "missing owner index", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationDelete,
					Key: connectorEnvironmentKey(
						fixture.environment.Record.ID,
						fixture.connector.Record.Connector.ID,
					),
				})
			},
		},
		{
			name: "missing reverse reference", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationDelete,
					Key: backupPolicyConnectorReferenceKey(
						fixture.connector.Record.Connector.ID,
						fixture.environment.Record.ID,
					),
				})
			},
		},
		{
			name: "deletion fence", wantKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, Mutation{
					Type: MutationPut,
					Key: deletionTombstoneKey(
						string(DeletionTargetConnector),
						fixture.connector.Record.Connector.ID,
					),
					Value: []byte("deleting"),
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, true)
			commitEnabledBackupPolicyProjection(t, fixture)
			test.mutate(t, fixture)
			_, err := fixture.repository.GetBackupPolicyProjection(
				context.Background(),
				fixture.environment.Record.ID,
			)
			if !isKind(err, test.wantKind) {
				t.Fatalf("GetBackupPolicyProjection(%s) error = %v", test.name, err)
			}
		})
	}
}

// Rationale: the public input limit must be the largest invariant that fits
// even an enabled Connector move, lazy key creation, retained history marker,
// and an all-Attach/Volume source set in the 96-operation transaction ceiling.
func TestMaximumBackupPolicySourcesMatchesWorstCaseAtomicBudget(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	candidate := fixture.candidate(t, true, "age")
	oldConnectorID := ids.NewAt(ids.KindConnector, fixture.now, 3200)
	candidate.Current = &Versioned[BackupPolicyRecord]{
		Record: BackupPolicyRecord{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 02:00:00",
			Keep:          3,
			Encryption:    "none",
			ConnectorID:   oldConnectorID,
			SourceIDs:     []string{fixture.sources[0].Source.Record.ID},
			UpdatedAt:     fixture.now.Add(-time.Second),
		},
		Revision: 1, ReadRevision: 1,
	}
	candidate.ConnectorReferences = []backupPolicyConnectorReferenceEvidence{
		{
			ConnectorID: oldConnectorID,
			Entry: &KeyValue{
				Key:   backupPolicyConnectorReferenceKey(oldConnectorID, fixture.environment.Record.ID),
				Value: []byte(fixture.environment.Record.ID), ModRevision: 1,
			},
		},
		{ConnectorID: fixture.connector.Record.Connector.ID},
	}
	candidate.Sources = backupPolicyBudgetVolumeEvidence(
		t,
		fixture.environment.Record.ID,
		fixture.now,
		MaximumBackupPolicySources+1,
	)
	candidate.Replacement.SourceIDs = make([]string, MaximumBackupPolicySources)
	for index := range candidate.Replacement.SourceIDs {
		candidate.Replacement.SourceIDs[index] = candidate.Sources[index].Source.Record.ID
	}
	candidate.Sources = candidate.Sources[:MaximumBackupPolicySources]
	plan, err := prepareBackupPolicyReplacement(candidate)
	if err != nil {
		t.Fatalf("prepareBackupPolicyReplacement(maximum) error = %v", err)
	}
	marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-budget-0001")
	if operations := backupPolicyReplacementOperationCount(plan, marker); operations != 93 ||
		operations > maximumTransactionOperations {
		t.Fatalf("maximum source operation budget = %d, want 93 <= %d", operations, maximumTransactionOperations)
	}
	candidate.Sources = backupPolicyBudgetVolumeEvidence(
		t,
		fixture.environment.Record.ID,
		fixture.now,
		MaximumBackupPolicySources+1,
	)
	candidate.Replacement.SourceIDs = make([]string, len(candidate.Sources))
	for index := range candidate.Replacement.SourceIDs {
		candidate.Replacement.SourceIDs[index] = candidate.Sources[index].Source.Record.ID
	}
	above, err := prepareBackupPolicyReplacement(candidate)
	if err != nil {
		t.Fatalf("prepareBackupPolicyReplacement(above maximum) error = %v", err)
	}
	if operations := backupPolicyReplacementOperationCount(above, marker); operations != 99 ||
		operations <= maximumTransactionOperations {
		t.Fatalf("above-maximum operation budget = %d, want 99 > %d", operations, maximumTransactionOperations)
	}

	audit := &backupPolicyPreparationAuditStore{memoryHierarchyStore: fixture.store}
	repository, err := newBackupPolicyRepository(audit)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository(audit) error = %v", err)
	}
	_, err = repository.PrepareBackupPolicyReplacement(
		context.Background(),
		BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Sources:       make([]BackupPolicySourceSelection, MaximumBackupPolicySources+1),
		},
	)
	if !isKind(err, errs.KindValidationFailed) || !strings.Contains(err.Error(), "at most 12") {
		t.Fatalf("PrepareBackupPolicyReplacement(over limit) error = %v", err)
	}
	if audit.transactions != 0 {
		t.Fatalf("over-limit preparation transactions = %d, want 0", audit.transactions)
	}
}

func commitEnabledBackupPolicyProjection(t *testing.T, fixture *backupPolicyReplacementFixture) {
	t.Helper()
	volume := fixture.sources[0].Source.Record
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []BackupPolicySourceSelection{{
				Kind: volume.Kind, TargetID: volume.TargetID,
			}},
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	defer prepared.Destroy()
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		context.Background(),
		prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-projection-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected() = %#v, %v", result, err)
	}
}

func backupPolicyProjectionMutation(t *testing.T, store *memoryHierarchyStore, mutation Mutation) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("projection mutation = %#v, %v", result, err)
	}
}

func backupPolicyBudgetVolumeEvidence(
	t *testing.T,
	environmentID string,
	now time.Time,
	count int,
) []backupPolicySourceEvidence {
	t.Helper()
	evidence := make([]backupPolicySourceEvidence, count)
	for index := range evidence {
		sourceID := ids.NewAt(ids.KindBackupSource, now, int64(3300+index))
		volumeID := ids.NewAt(ids.KindVolume, now, int64(3400+index))
		volume, err := NewVolumeRecord(environmentID, volumeID, "budget-volume-"+sourceID)
		if err != nil {
			t.Fatalf("NewVolumeRecord() error = %v", err)
		}
		record := BackupSourceRecord{
			ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceVolume,
			TargetID: volumeID, CreatedAt: now,
		}
		evidence[index] = backupPolicySourceEvidence{
			Source: Versioned[BackupSourceRecord]{Record: record, Revision: 1, ReadRevision: 1},
			EnvironmentIndex: &KeyValue{
				Key: backupSourceEnvironmentKey(environmentID, sourceID), Value: []byte(sourceID), ModRevision: 1,
			},
			IdentityIndex: &KeyValue{
				Key:   backupSourceIdentityKey(environmentID, record.Kind, record.TargetID),
				Value: []byte(sourceID), ModRevision: 1,
			},
			Volume: &Versioned[VolumeRecord]{Record: volume, Revision: 1, ReadRevision: 1},
			TargetOwnerIndex: &KeyValue{
				Key: volumeOwnerKey(environmentID, volumeID), Value: []byte(volumeID), ModRevision: 1,
			},
		}
	}
	return evidence
}

type backupPolicyPreparationAuditStore struct {
	*memoryHierarchyStore
	transactions int
}

func (store *backupPolicyPreparationAuditStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.transactions++
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
