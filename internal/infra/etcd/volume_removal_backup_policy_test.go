package etcd

import (
	"context"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: removal prepares a replacement without deleting historical source
// records or publishing a policy independently of the desired revision.
func TestVolumeRemovalBackupPolicyPreparationPreservesHistory(t *testing.T) {
	for _, test := range []struct {
		name               string
		remaining, enabled bool
	}{
		{"enabled last source", false, true}, {"enabled remaining source", true, true},
		{"disabled last source", false, false}, {"disabled remaining source", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			remaining, remainsEnabled := test.remaining, test.remaining && test.enabled
			fixture := newBackupPolicyReplacementFixture(t, true)
			ctx := context.Background()
			volume := fixture.sources[0].Source.Record
			selections := []BackupPolicySourceSelection{{Kind: core.BackupSourceVolume, TargetID: volume.TargetID}}
			if remaining {
				selections = append(selections, BackupPolicySourceSelection{
					Kind: core.BackupSourceConfig, TargetID: fixture.environment.Record.ID,
				})
			}
			prepared, err := fixture.repository.PrepareBackupPolicyReplacement(ctx, BackupPolicyReplacementInput{
				EnvironmentID: fixture.environment.Record.ID, Enabled: test.enabled,
				Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "age",
				ConnectorID: fixture.connector.Record.Connector.ID, Sources: selections,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Destroy()
			if prepared.RequiresInitialKey() {
				identity, keyErr := age.GenerateX25519Identity()
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				prepared, err = fixture.repository.SupplyBackupPolicyInitialKey(ctx, prepared,
					BackupPolicyInitialKeyMaterial{
						Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-test-identity"),
					})
				if err != nil {
					t.Fatal(err)
				}
			}
			prepared, err = prepared.FinalizeSchedule(fixture.now)
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.repository.ReplaceBackupPolicyProtected(ctx, prepared,
				backupPolicyReplacementMarker(fixture.environment.Record.ID, "volume-policy-removal-seed-0001"))
			if err != nil || result.kind != idempotencyTransactionApplied {
				t.Fatalf("seed policy: %v", err)
			}
			before := fixture.store.revision
			removal, err := fixture.repository.PrepareVolumeRemovalBackupPolicy(ctx,
				fixture.environment.Record.ID, volume.TargetID, before)
			if err != nil {
				t.Fatal(err)
			}
			publication, err := prepareVolumeRemovalBackupPolicyPublication(removal, fixture.now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer clearBackupRuntimeMutations(publication.mutations)
			projection := removal.Projection()
			if !equalEnvironmentBlueprintBackupPolicy(projection, publication.projection) {
				t.Fatal("staged decisions differ from publication decisions")
			}
			projection.Keep = 1
			if len(projection.Sources) != 0 {
				projection.Sources[0].ID = "changed"
			}
			if !equalEnvironmentBlueprintBackupPolicy(removal.Projection(), publication.projection) {
				t.Fatal("caller changed immutable removal preparation")
			}
			if fixture.store.revision != before || publication.projection.Enabled != remainsEnabled ||
				publication.projection.Keep != 7 || publication.projection.Encryption != "age" ||
				publication.projection.ConnectorID != fixture.connector.Record.Connector.ID {
				t.Fatal("preparation changed storage or lost retained policy configuration")
			}
			wantSources := 0
			if remaining {
				wantSources = 1
			}
			if len(publication.projection.Sources) != wantSources {
				t.Fatal("replacement retained the removed Volume selection")
			}
			if err := validateEnvironmentBlueprintBackupPolicy(fixture.environment.Record.ID, publication.projection); err != nil {
				t.Fatalf("last-source replacement cannot be staged as desired state: %v", err)
			}
			if !remaining {
				enabled := CloneEnvironmentBlueprintBackupPolicy(publication.projection)
				enabled.Enabled = true
				if err := validateEnvironmentBlueprintBackupPolicy(fixture.environment.Record.ID, enabled); err == nil {
					t.Fatal("enabled empty-source desired policy accepted")
				}
			}
			if len(publication.conditions) != len(selections)+4 {
				t.Fatal("policy fragment no longer has one primary compare per source plus four fixed fences")
			}
			for _, mutation := range publication.mutations {
				if mutation.Type == MutationDelete && mutation.Key != backupPolicyConnectorReferenceKey(
					fixture.connector.Record.Connector.ID, fixture.environment.Record.ID) {
					t.Fatal("replacement deletes historical authority")
				}
				if mutation.Key == environmentCoordinationKey(fixture.environment.Record.ID) {
					coordination, err := decodeEnvironmentCoordinationRecord(mutation.Value)
					if err != nil || !coordination.ScheduleClockFloor.Equal(fixture.now.Add(time.Hour)) ||
						(coordination.CurrentBackupScheduleState != nil) != remainsEnabled {
						t.Fatalf("replacement lost scheduling boundary: %v", err)
					}
				}
			}
			backward, err := prepareVolumeRemovalBackupPolicyPublication(removal, fixture.now.Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer clearBackupRuntimeMutations(backward.mutations)
			for _, mutation := range backward.mutations {
				if mutation.Key == environmentCoordinationKey(fixture.environment.Record.ID) {
					coordination, err := decodeEnvironmentCoordinationRecord(mutation.Value)
					if err != nil || !coordination.ScheduleClockFloor.Equal(fixture.now) {
						t.Fatalf("removal moved the scheduling floor backward: %v", err)
					}
				}
			}
			keys := []string{
				backupPolicyKey(fixture.environment.Record.ID),
				environmentCoordinationKey(fixture.environment.Record.ID),
				environmentMutationEpochKey(fixture.environment.Record.ID),
				backupSourceKey(volume.ID),
			}
			if test.enabled {
				keys = append(
					keys,
					backupPolicyConnectorReferenceKey(
						fixture.connector.Record.Connector.ID,
						fixture.environment.Record.ID,
					),
				)
			}
			for _, key := range keys {
				assertVolumePolicyPreparationRejectsRace(t, fixture, volume.TargetID, key)
			}
		})
	}
}

func assertVolumePolicyPreparationRejectsRace(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	volumeID, key string,
) {
	t.Helper()
	ctx := context.Background()
	prepared, err := fixture.repository.PrepareVolumeRemovalBackupPolicy(ctx,
		fixture.environment.Record.ID, volumeID, fixture.store.revision)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := prepareVolumeRemovalBackupPolicyPublication(prepared, fixture.now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer clearBackupRuntimeMutations(publication.mutations)
	read, err := fixture.store.Get(ctx, key)
	if err != nil || read.Entry == nil {
		t.Fatalf("read race fence: %v", err)
	}
	defer clear(read.Entry.Value)
	raced, err := fixture.store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: read.Entry.Value}})
	if err != nil || !raced.Succeeded {
		t.Fatalf("inject race: %v", err)
	}
	// This exercises only the prepared fragment's CAS. The production DELETE
	// journey separately requires one combined desired/operation/Task commit.
	result, err := fixture.store.Transact(ctx, publication.conditions, publication.mutations)
	if err != nil || result.Succeeded || fixture.store.revision != raced.Revision {
		t.Fatalf("stale policy preparation committed after %s changed: %v", key, err)
	}
}

// Rationale: the policy fragment must leave room for the desired and removal
// owners in ADR0051's final transaction at the public maximum selection size.
func TestVolumeRemovalBackupPolicyMaximumFragmentBudget(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	taskID := ids.NewAt(ids.KindTask, fixture.now, 4001)
	projection := EnvironmentComposeProjection{EnvironmentID: fixture.environment.Record.ID, RevisionID: taskID}
	input := EnvironmentBlueprintBackupPolicyInput{
		EnvironmentID: fixture.environment.Record.ID, TaskID: taskID,
		ReadRevision: fixture.store.revision, Enabled: true, Frequency: "*-*-* 03:00:00", Keep: 3,
		Encryption: "none", ConnectorName: fixture.connector.Record.Connector.Name, CreatedAt: fixture.now,
	}
	for index := range MaximumBackupPolicySources {
		volumeID := ids.NewAt(ids.KindVolume, fixture.now, int64(4100+index))
		projection.Volumes = append(projection.Volumes, EnvironmentVolumeIdentity{
			ID: volumeID, Slug: "volume-" + string(rune('a'+index)), Key: "volume-" + string(rune('a'+index)),
		})
		input.Sources = append(input.Sources, EnvironmentBlueprintBackupPolicySourceInput{
			CandidateID: ids.NewAt(ids.KindBackupSource, fixture.now, int64(4200+index)),
			Kind:        core.BackupSourceVolume, TargetID: volumeID,
		})
	}
	input.Projection = projection
	seed, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Clear()
	projection.Backup = seed.Projection()
	seedPublication, err := prepareBlueprintBackupPolicyPublication(
		TaskRecord{
			ID:     taskID,
			Target: fixture.environment.Record.ID,
		},
		projection,
		BlueprintAttachTaskPreparation{},
		seed,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearPreparedBlueprintBackupPolicyPublication(seedPublication)
	// Seed only this owner's records in the MVCC fixture, not a product Apply.
	seeded, err := fixture.store.Transact(ctx, seedPublication.conditions, seedPublication.mutations)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed maximum policy records: %v", err)
	}
	prepared, err := fixture.repository.PrepareVolumeRemovalBackupPolicy(ctx,
		fixture.environment.Record.ID, input.Sources[0].TargetID, seeded.Revision)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := prepareVolumeRemovalBackupPolicyPublication(prepared, fixture.now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer clearBackupRuntimeMutations(publication.mutations)
	if len(publication.conditions) != 16 || len(publication.mutations) != 2 ||
		len(publication.projection.Sources) != MaximumBackupPolicySources-1 || !publication.projection.Enabled {
		t.Fatal("maximum policy removal fragment changed its bounded shape")
	}
	sizer := &store{root: "/groundplane"}
	bytes, err := sizer.transactionSize(publication.conditions, publication.mutations)
	if err != nil || bytes > 16*1024 {
		t.Fatalf("maximum policy removal fragment exceeds 16 KiB: bytes=%d error=%v", bytes, err)
	}
	t.Logf(
		"maximum policy fragment: %d comparisons, %d mutations, %d protobuf bytes",
		len(publication.conditions),
		len(publication.mutations),
		bytes,
	)
}

func TestVolumeRemovalBackupPolicyAbsentAndMalformedPreparation(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	volumeID := ids.NewAt(ids.KindVolume, fixture.now, 4300)
	before := fixture.store.revision
	prepared, err := fixture.repository.PrepareVolumeRemovalBackupPolicy(ctx,
		fixture.environment.Record.ID, volumeID, before)
	if err != nil || prepared.Projection() != nil {
		t.Fatalf("absent policy preparation: %v", err)
	}
	publication, err := prepareVolumeRemovalBackupPolicyPublication(prepared, fixture.now)
	if err != nil || publication.projection != nil || len(publication.mutations) != 0 ||
		len(publication.conditions) != 3 {
		t.Fatalf("absent policy was created by preparation: %v", err)
	}
	for _, request := range []struct {
		environmentID string
		volumeID      string
		revision      int64
	}{
		{"invalid", volumeID, before},
		{fixture.environment.Record.ID, "invalid", before},
		{fixture.environment.Record.ID, volumeID, 0},
	} {
		if _, err := fixture.repository.PrepareVolumeRemovalBackupPolicy(ctx,
			request.environmentID, request.volumeID, request.revision); err == nil {
			t.Fatal("malformed preparation identity accepted")
		}
	}
	if _, err := prepareVolumeRemovalBackupPolicyPublication(VolumeRemovalBackupPolicyPreparation{}, fixture.now); err == nil {
		t.Fatal("unprepared policy publication accepted")
	}
	if _, err := prepareVolumeRemovalBackupPolicyPublication(prepared, time.Time{}); err == nil {
		t.Fatal("missing publication time accepted")
	}
	if fixture.store.revision != before {
		t.Fatal("read-only preparation changed storage")
	}
}
