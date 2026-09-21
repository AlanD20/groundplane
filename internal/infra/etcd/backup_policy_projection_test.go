package etcd

import (
	"context"
	"strings"
	"testing"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	input := testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       true,
		Frequency:     "Mon *-*-* 03:00:00",
		Keep:          7,
		Encryption:    "age",
		ConnectorID:   fixture.connector.Record.Connector.ID,
		Sources: []testbackuppolicy.BackupPolicySourceSelection{
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
		prepared, testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
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
	deleted, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete,
		Key: testbackuppolicy.BackupSourceIdentityKey(
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
	deleted, err := fixture.store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationDelete, Key: testconnectors.RecordKey(connectorID)},
		{
			Type: testkeyvalue.MutationDelete,
			Key:  testconnectors.ConnectorEnvironmentKey(fixture.environment.Record.ID, connectorID),
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testdeletions.TombstoneKey(string(testdeletions.DeletionTargetConnector), connectorID),
			Value: []byte("deleted"),
		},
	})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete Connector fixture = %#v, %v", deleted, err)
	}
	volume := fixture.sources[0].Source.Record
	input := testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Frequency:     "*-*-* 03:00:00",
		Keep:          7,
		Encryption:    "none",
		ConnectorID:   connectorID,
		Sources: []testbackuppolicy.BackupPolicySourceSelection{{
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
	hierarchy, err := newHierarchyRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	otherEnvironment, err := hierarchy.CreateEnvironment(context.Background(), testhierarchy.EnvironmentRecord{
		ID: otherEnvironmentID, ProjectID: fixture.project.Record.ID,
		Name: "foreign", NetworkPool: "10.241.0.0/24",
		VolumeDir: "/var/lib/groundplane/vol/" + fixture.project.Record.TenantID + "/" +
			fixture.project.Record.ID + "/" + otherEnvironmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, fixture.now, 2902), CreatedAt: fixture.now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment(foreign) error = %v", err)
	}
	volume := testenvironmentprojection.EnvironmentVolumeIdentity{
		ID: ids.NewAt(ids.KindVolume, fixture.now, 2901), Slug: "foreign-data", Key: "foreign-data",
	}
	seedBackupPolicyVolumeProjection(
		t, fixture.store, otherEnvironment, fixture.project, volume, 2903,
	)
	_, err = fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(), testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
				Kind: core.BackupSourceVolume, TargetID: volume.ID,
			}},
		},
	)
	if !isKind(err, errs.KindVolumeNotFound) {
		t.Fatalf("PrepareBackupPolicyReplacement(foreign) error = %v", err)
	}
	index, readErr := fixture.store.Get(context.Background(), testbackuppolicy.BackupSourceIdentityKey(
		fixture.environment.Record.ID,
		core.BackupSourceVolume,
		volume.ID,
	))
	if readErr != nil || index == nil || index.Entry != nil {
		t.Fatalf("foreign source catalog entry = %#v, %v", index, readErr)
	}
}

// Rationale: the selected desired head is part of the same final CAS as the
// policy, so a head change after preparation must lose atomically.
func TestBackupPolicyTypedReplacementFencesDesiredHeadRace(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	volumeSource := fixture.sources[0].Source.Record
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(), testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
				Kind: volumeSource.Kind, TargetID: volumeSource.TargetID,
			}},
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	defer prepared.Destroy()
	headValue, err := testidempotency.EncodeTaskReference(ids.NewAt(ids.KindTask, fixture.now, 3001))
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID), Value: headValue,
	}})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("advance Volume desired head = %#v, %v", deleted, err)
	}
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		context.Background(),
		prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-owner-race-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionConflict || result.conflict == nil {
		t.Fatalf("ReplaceBackupPolicyProtected(head race) = %#v, %v", result, err)
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
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationDelete, Key: testconnectors.RecordKey(fixture.connector.Record.Connector.ID),
				})
			},
		},
		{
			name: "corrupt primary", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(fixture.connector.Record.Connector.ID),
					Value: []byte("corrupt"),
				})
			},
		},
		{
			name: "cross owner", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				record := fixture.connector.Record
				record.Connector.EnvironmentID = ids.NewAt(ids.KindEnvironment, fixture.now, 3100)
				value, err := testconnectors.EncodeRecord(record)
				if err != nil {
					t.Fatalf("encodeConnectorRecord(cross owner) error = %v", err)
				}
				defer clear(value)
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(fixture.connector.Record.Connector.ID), Value: value,
				})
			},
		},
		{
			name: "missing owner index", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationDelete,
					Key: testconnectors.ConnectorEnvironmentKey(
						fixture.environment.Record.ID,
						fixture.connector.Record.Connector.ID,
					),
				})
			},
		},
		{
			name: "missing reverse reference", wantKind: errs.KindInternal,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationDelete,
					Key: testbackuppolicy.BackupPolicyConnectorReferenceKey(
						fixture.connector.Record.Connector.ID,
						fixture.environment.Record.ID,
					),
				})
			},
		},
		{
			name: "deletion fence", wantKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, fixture *backupPolicyReplacementFixture) {
				backupPolicyProjectionMutation(t, fixture.store, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut,
					Key: testdeletions.TombstoneKey(
						string(testdeletions.DeletionTargetConnector),
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

// Rationale: the public source limit must reject impossible replacement input
// before preparation mutates the durable source catalog.
func TestMaximumBackupPolicySourcesRejectsBeforeCatalogMutation(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	audit := &backupPolicyPreparationAuditStore{memoryHierarchyStore: fixture.store}
	repository, err := newBackupPolicyRepository(audit)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository(audit) error = %v", err)
	}
	_, err = repository.PrepareBackupPolicyReplacement(
		context.Background(), testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Sources: make(
				[]testbackuppolicy.BackupPolicySourceSelection,
				testbackuppolicy.MaximumBackupPolicySources+1,
			),
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
		context.Background(), testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
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

func backupPolicyProjectionMutation(t *testing.T, store *memoryHierarchyStore, mutation testkeyvalue.Mutation) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("projection mutation = %#v, %v", result, err)
	}
}

type backupPolicyPreparationAuditStore struct {
	*memoryHierarchyStore
	transactions int
}

func (store *backupPolicyPreparationAuditStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.transactions++
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
