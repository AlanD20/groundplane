package etcd

import (
	"context"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBlueprintBackupCanonicalValidationRejectsDisabledConfigWithoutAge(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID, Enabled: false,
			Frequency: "0 2 * * *", Keep: 7, Encryption: "none",
			ConnectorID: fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
				Kind: core.BackupSourceConfig, TargetID: fixture.environment.Record.ID,
			}},
		})
	defer prepared.Destroy()
	if !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("disabled config validation error = %v, want validation", err)
	}
}

func TestBlueprintBackupPreparationDefersSourceAndKeyWritesAndReusesStableSource(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	taskID := ids.NewAt(ids.KindTask, fixture.now, 3001)
	volumeID := ids.NewAt(ids.KindVolume, fixture.now, 3002)
	sourceID := ids.NewAt(ids.KindBackupSource, fixture.now, 3003)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: fixture.environment.Record.ID, RevisionID: taskID,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "archive", Key: "archive"}},
	}
	before := fixture.store.revision
	prepared, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(
		ctx,
		testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: fixture.environment.Record.ID, TaskID: taskID,
			ReadRevision: before, Enabled: true, Frequency: "0 2 * * *", Keep: 7,
			Encryption: "age", ConnectorName: fixture.connector.Record.Connector.Name,
			Sources: []testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{{
				CandidateID: sourceID, Kind: core.BackupSourceVolume, TargetID: volumeID,
			}},
			Projection: projection, CreatedAt: fixture.now,
		},
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintBackupPolicy() error = %v", err)
	}
	if fixture.store.revision != before {
		t.Fatalf("preparation revision = %d, want unchanged %d", fixture.store.revision, before)
	}
	if !prepared.RequiresInitialKey() || prepared.Projection().Sources[0].ID != sourceID {
		t.Fatalf("prepared projection = %#v", prepared.Projection())
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.SupplyInitialKey(testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
		Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-age-identity"),
	}); err != nil {
		t.Fatalf("SupplyInitialKey() error = %v", err)
	}
	projection.Backup = prepared.Projection()
	publication, err := testblueprintplanning.PrepareBlueprintBackupPolicyPublication(
		testblueprintplanning.TaskIdentity{ID: taskID, Target: fixture.environment.Record.ID},
		projection, prepared,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintBackupPolicyPublication() error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(publication)
	if fixture.store.revision != before {
		t.Fatalf("final preparation revision = %d, want unchanged %d", fixture.store.revision, before)
	}
	transaction, err := fixture.store.Transact(ctx, publication.Conditions(), publication.Mutations())
	if err != nil || !transaction.Succeeded {
		t.Fatalf("Blueprint Backup transaction = %#v, %v", transaction, err)
	}
	stored, err := testbackupsources.GetBackupSource(ctx, fixture.store, sourceID)
	if err != nil || stored.Record.TargetID != volumeID {
		t.Fatalf("GetBackupSource() = %#v, %v", stored, err)
	}
	prepared.Clear()

	retryTaskID := ids.NewAt(ids.KindTask, fixture.now, 3004)
	projection.RevisionID = retryTaskID
	retry, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(
		ctx,
		testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: fixture.environment.Record.ID, TaskID: retryTaskID,
			ReadRevision: fixture.store.revision, Enabled: true, Frequency: "0 2 * * *", Keep: 7,
			Encryption: "age", ConnectorName: fixture.connector.Record.Connector.Name,
			Sources: []testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{{
				CandidateID: ids.NewAt(ids.KindBackupSource, fixture.now, 3999),
				Kind:        core.BackupSourceVolume, TargetID: volumeID,
			}},
			Projection: projection, CreatedAt: fixture.now.Add(time.Second),
		},
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintBackupPolicy(retry) error = %v", err)
	}
	defer retry.Clear()
	if retry.RequiresInitialKey() || retry.Projection().Sources[0].ID != sourceID {
		t.Fatalf("retry projection = %#v", retry.Projection())
	}
}

// Rationale: the supported maximum must publish readable, correctly scoped
// sources within storage limits, rather than merely producing a write inventory.
func TestBlueprintBackupMaximumSourcesPublishAndResolve(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	existing, err := testbackupsources.ListBackupSources(t.Context(), fixture.store, fixture.environment.Record.ID,
		testkeyvalue.PageRequest{Limit: 100})
	if err != nil || existing.NextCursor != "" {
		t.Fatalf("read existing Backup sources: %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, fixture.now, 4001)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: fixture.environment.Record.ID,
		RevisionID:    taskID,
	}
	input := testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
		EnvironmentID: fixture.environment.Record.ID, TaskID: taskID,
		ReadRevision: fixture.store.revision, Enabled: true, Frequency: "0 3 * * *", Keep: 3,
		Encryption: "none", ConnectorName: fixture.connector.Record.Connector.Name,
		CreatedAt: fixture.now,
	}
	for index := range testbackuppolicy.MaximumBackupPolicySources {
		volumeID := ids.NewAt(ids.KindVolume, fixture.now, int64(4100+index))
		projection.Volumes = append(projection.Volumes, testenvironmentprojection.EnvironmentVolumeIdentity{
			ID: volumeID, Slug: "volume-" + string(rune('a'+index)), Key: "volume-" + string(rune('a'+index)),
		})
		input.Sources = append(input.Sources, testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{
			CandidateID: ids.NewAt(ids.KindBackupSource, fixture.now, int64(4200+index)),
			Kind:        core.BackupSourceVolume, TargetID: volumeID,
		})
	}
	input.Projection = projection
	prepared, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(context.Background(), input)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintBackupPolicy(maximum) error = %v", err)
	}
	defer prepared.Clear()
	projection.Backup = prepared.Projection()
	publication, err := testblueprintplanning.PrepareBlueprintBackupPolicyPublication(
		testblueprintplanning.TaskIdentity{ID: taskID, Target: fixture.environment.Record.ID},
		projection, prepared,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintBackupPolicyPublication(maximum) error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(publication)
	budget, err := fixture.store.MeasureTransaction(t.Context(), publication.Conditions(), publication.Mutations())
	if err != nil || !budget.Fits() {
		t.Fatalf("maximum Backup source publication exceeds ordinary storage bounds: %v", err)
	}
	result, err := fixture.store.Transact(t.Context(), publication.Conditions(), publication.Mutations())
	if err != nil || !result.Succeeded {
		t.Fatalf("publish maximum Backup sources: %v", err)
	}
	listed, err := testbackupsources.ListBackupSources(t.Context(), fixture.store, input.EnvironmentID,
		testkeyvalue.PageRequest{Limit: 100})
	if err != nil || len(listed.Items) != len(existing.Items)+len(input.Sources) || listed.NextCursor != "" {
		t.Fatalf("maximum Backup source catalog is incomplete: %v", err)
	}
	for _, selected := range input.Sources {
		stored, err := testbackupsources.GetBackupSource(t.Context(), fixture.store, selected.CandidateID)
		if err != nil || stored.Record.EnvironmentID != input.EnvironmentID ||
			stored.Record.Kind != selected.Kind || stored.Record.TargetID != selected.TargetID {
			t.Fatalf("published Backup source changed identity or target: %v", err)
		}
	}
}
