package etcd

import (
	"context"
	"testing"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type environmentBlueprintKeyBufferCaptureStore struct {
	hierarchyStore
	key      string
	captured [][]byte
}

func (store *environmentBlueprintKeyBufferCaptureStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	result, err := store.hierarchyStore.GetMany(ctx, request)
	if err != nil || result == nil {
		return result, err
	}
	for _, value := range result.Values {
		if value != nil && value.Key == store.key {
			store.captured = append(store.captured, value.Value)
		}
	}
	return result, nil
}

func TestEnvironmentBlueprintBackupPreparationClearsRawExistingAgeKeyBuffers(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		ctx, testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 02:00:00",
			Keep:          7,
			Encryption:    "age",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
				Kind:     core.BackupSourceConfig,
				TargetID: fixture.environment.Record.ID,
			}},
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	defer prepared.Destroy()
	prepared, err = fixture.repository.SupplyBackupPolicyInitialKey(
		ctx,
		prepared, testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
			Recipient:  identity.Recipient().String(),
			Ciphertext: []byte("controller-sealed-existing-age-identity"),
		},
	)
	if err != nil {
		t.Fatalf("SupplyBackupPolicyInitialKey() error = %v", err)
	}
	prepared, err = prepared.FinalizeSchedule(fixture.now)
	if err != nil {
		t.Fatalf("FinalizeSchedule() error = %v", err)
	}
	if _, err = fixture.repository.ReplaceBackupPolicyProtected(
		ctx,
		prepared,
		backupPolicyReplacementMarker(
			fixture.environment.Record.ID,
			"blueprint-key-clear-seed-0001",
		),
	); err != nil {
		t.Fatalf("ReplaceBackupPolicyProtected() error = %v", err)
	}

	captured := &environmentBlueprintKeyBufferCaptureStore{
		hierarchyStore: fixture.store,
		key:            testbackuppolicy.BackupKeyValueKey(fixture.environment.Record.ID),
	}
	repository, err := newBackupPolicyRepository(captured)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, fixture.now, 7900)
	retained, err := repository.PrepareEnvironmentBlueprintBackupPolicy(
		ctx, testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: fixture.environment.Record.ID,
			TaskID:        taskID,
			ReadRevision:  fixture.store.revision,
			Retain:        true,
			Projection: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: fixture.environment.Record.ID,
				RevisionID:    taskID,
			},
			CreatedAt: fixture.now,
		},
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentBlueprintBackupPolicy(retain) error = %v", err)
	}
	defer retained.Clear()
	if len(captured.captured) == 0 {
		t.Fatal("Blueprint preparation did not read the existing encrypted age key")
	}
	for index, value := range captured.captured {
		for _, octet := range value {
			if octet != 0 {
				t.Fatalf("raw encrypted age-key buffer %d was not cleared", index)
			}
		}
	}
}
