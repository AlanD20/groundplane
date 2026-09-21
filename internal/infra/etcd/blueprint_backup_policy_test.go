package etcd

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBlueprintBackupRetainFencesConnectorOnComposedFinalPublication(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	direct, err := fixture.repository.PrepareBackupPolicyReplacement(ctx, testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID, Enabled: false,
		Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "age",
		ConnectorID: fixture.connector.Record.Connector.ID,
		Sources: []testbackuppolicy.BackupPolicySourceSelection{{
			Kind: core.BackupSourceConfig, TargetID: fixture.environment.Record.ID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Destroy()
	direct, err = direct.FinalizeSchedule(fixture.now)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := fixture.repository.ReplaceBackupPolicyProtected(
		ctx, direct, backupPolicyReplacementMarker(fixture.environment.Record.ID, "blueprint-backup-retain-seed-0001"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := seeded.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("seed policy = %v/%v/%v", outcome, conflict, classifyErr)
	}

	task := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 5100)
	projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, task, 1)
	projection.DesiredZones[0].Desired.Subnet = "10.240.0.0/25"
	beforePreparation := fixture.store.revision
	prepared, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(
		ctx, testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: fixture.environment.Record.ID, TaskID: task.ID,
			ReadRevision: beforePreparation, Retain: true, Projection: projection, CreatedAt: task.CreatedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	if prepared.IsZero() || fixture.store.revision != beforePreparation || prepared.RequiresInitialKey() {
		t.Fatalf("retained preparation = zero:%t revision:%d/%d initial-key:%t",
			prepared.IsZero(), fixture.store.revision, beforePreparation, prepared.RequiresInitialKey())
	}
	projection.Backup = prepared.Projection()
	marker := environmentBlueprintTestMarker(task, fixture.environment.Record.ID)
	hierarchy, err := newEnvironmentBlueprintRepository(fixture.store, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	claim := stageEnvironmentBlueprintForPublicationTest(
		t, hierarchy.HierarchyRepository, 0,
		environmentBlueprintTestRevision(fixture.environment.Record.ID, task, "services: {}\n"),
		projection, marker,
	)
	connectorValue, err := testconnectors.EncodeRecord(fixture.connector.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(connectorValue)
	race, err := fixture.store.Transact(ctx, []testkeyvalue.Condition{{
		Key: testconnectors.RecordKey(fixture.connector.Record.Connector.ID), ModRevision: fixture.connector.Revision,
	}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(fixture.connector.Record.Connector.ID), Value: connectorValue,
	}})
	if err != nil || !race.Succeeded {
		t.Fatalf("Connector race = %#v, %v", race, err)
	}
	result, err := hierarchy.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.MustParsePrefix("10.240.0.0/24"),
		fixture.environment.Record.NetworkPool,
		fixture.project,
		fixture.environment,
		0,
		claim,
		testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: fixture.environment.Record.ID,
			RevisionID:    task.ID,
		},
		projection,
		environmentBlueprintTestZoneChanges(t, hierarchy.HierarchyRepository, projection),
		environmentBlueprintTestServiceChanges(t, hierarchy.HierarchyRepository, projection),
		environmentBlueprintTestRouteChanges(
			t,
			hierarchy.HierarchyRepository,
			projection,
		),
		testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
		testcomponentplanning.ComponentTaskPreparation{},
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		prepared,
		BlueprintScriptPublication{},
		BlueprintReleasePublication{},
		BlueprintRequirementGate{},
		task,
		marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict == nil ||
		outcome != IdempotencyKnownConflict {
		t.Fatalf("raced publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	for _, key := range []string{testblueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID), testtaskjournal.TaskStorageKey(task.ID), testtaskjournal.TaskQueueKey(task.Executor, task.ID), testbackuppolicy.BackupKeyKey(fixture.environment.Record.ID), testbackuppolicy.BackupKeyValueKey(fixture.environment.Record.ID)} {
		value, getErr := fixture.store.Get(ctx, key)
		if getErr != nil || value.Entry != nil {
			t.Fatalf("raced publication exposed %q = %#v, %v", key, value, getErr)
		}
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if value, getErr := fixture.store.Get(ctx, markerKey); getErr != nil || value.Entry != nil {
		t.Fatalf("raced publication marker = %#v, %v", value, getErr)
	}
}

func TestBlueprintBackupCanonicalValidationRejectsDisabledConfigWithoutAge(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID, Enabled: false,
			Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "none",
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

func TestBlueprintBackupCandidateVolumePublishesWithFinalAuthority(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	ctx := context.Background()
	task := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 5200)
	projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, task, 1)
	projection.DesiredZones[0].Desired.Subnet = "10.240.0.0/25"
	volumeID := projection.Volumes[0].ID
	sourceID := ids.NewAt(ids.KindBackupSource, fixture.now, 5201)
	beforePreparation := fixture.store.revision
	prepared, err := fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(
		ctx, testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: fixture.environment.Record.ID, TaskID: task.ID,
			ReadRevision: beforePreparation, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7,
			Encryption: "none", ConnectorName: fixture.connector.Record.Connector.Name,
			Sources: []testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{{
				CandidateID: sourceID, Kind: core.BackupSourceVolume, TargetID: volumeID,
			}},
			Projection: projection, CreatedAt: task.CreatedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	if fixture.store.revision != beforePreparation {
		t.Fatalf("Backup preparation wrote revision %d, want %d", fixture.store.revision, beforePreparation)
	}
	projection.Backup = prepared.Projection()
	marker := environmentBlueprintTestMarker(task, fixture.environment.Record.ID)
	hierarchy, err := newEnvironmentBlueprintRepository(fixture.store, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	claim := stageEnvironmentBlueprintForPublicationTest(
		t, hierarchy.HierarchyRepository, 0,
		environmentBlueprintTestRevision(fixture.environment.Record.ID, task, "services: {}\n"),
		projection, marker,
	)
	result, err := hierarchy.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.MustParsePrefix("10.240.0.0/24"),
		fixture.environment.Record.NetworkPool,
		fixture.project,
		fixture.environment,
		0,
		claim,
		testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: fixture.environment.Record.ID,
			RevisionID:    task.ID,
		},
		projection,
		environmentBlueprintTestZoneChanges(t, hierarchy.HierarchyRepository, projection),
		environmentBlueprintTestServiceChanges(t, hierarchy.HierarchyRepository, projection),
		environmentBlueprintTestRouteChanges(
			t,
			hierarchy.HierarchyRepository,
			projection,
		),
		testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
		testcomponentplanning.ComponentTaskPreparation{},
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		prepared,
		BlueprintScriptPublication{},
		BlueprintReleasePublication{},
		BlueprintRequirementGate{},
		task,
		marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("candidate Volume publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	head, found, err := hierarchy.GetEnvironmentBlueprintHead(ctx, fixture.environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != task.ID {
		t.Fatalf("candidate Volume head = %#v/%t/%v", head, found, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID), testbackuppolicy.BackupSourceKey(sourceID), testbackuppolicy.BackupSourceEnvironmentKey(fixture.environment.Record.ID, sourceID), testbackuppolicy.BackupSourceIdentityKey(fixture.environment.Record.ID, core.BackupSourceVolume, volumeID), testbackuppolicy.BackupPolicyConnectorReferenceKey(fixture.connector.Record.Connector.ID, fixture.environment.Record.ID), testtaskjournal.TaskStorageKey(task.ID), testtaskjournal.TaskQueueKey(task.Executor, task.ID), markerKey} {
		value, getErr := fixture.store.Get(ctx, key)
		if getErr != nil || value.Entry == nil || value.Entry.ModRevision != head.Revision {
			t.Fatalf("candidate Volume authority %q = %#v, %v; want revision %d", key, value, getErr, head.Revision)
		}
	}
}

// Rationale: Blueprint preparation must be read-only, while final publication
// creates the missing source triple and first age key in one transaction.
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
			ReadRevision: before, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7,
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
		projection, testblueprintplanning.BlueprintAttachTaskPreparation{}, prepared,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintBackupPolicyPublication() error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(publication)
	if fixture.store.revision != before {
		t.Fatalf("final preparation revision = %d, want unchanged %d", fixture.store.revision, before)
	}
	createdSourceMutations := 0
	for _, mutation := range publication.Mutations() {
		if mutation.Key == testbackuppolicy.BackupSourceKey(sourceID) ||
			mutation.Key == testbackuppolicy.BackupSourceEnvironmentKey(fixture.environment.Record.ID, sourceID) ||
			mutation.Key == testbackuppolicy.BackupSourceIdentityKey(
				fixture.environment.Record.ID,
				core.BackupSourceVolume,
				volumeID,
			) {
			createdSourceMutations++
		}
	}
	if createdSourceMutations != 3 {
		t.Fatalf("source triple mutations = %d, want 3", createdSourceMutations)
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
			ReadRevision: fixture.store.revision, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7,
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

// Rationale: the public maximum must reserve exactly three durable writes for
// each newly introduced stable source and remain within the Blueprint arm cap.
func TestBlueprintBackupMaximumSourcesProducesTwelveDurableTriples(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, false)
	taskID := ids.NewAt(ids.KindTask, fixture.now, 4001)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: fixture.environment.Record.ID,
		RevisionID:    taskID,
	}
	input := testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
		EnvironmentID: fixture.environment.Record.ID, TaskID: taskID,
		ReadRevision: fixture.store.revision, Enabled: true, Frequency: "*-*-* 03:00:00", Keep: 3,
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
		projection, testblueprintplanning.BlueprintAttachTaskPreparation{}, prepared,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintBackupPolicyPublication(maximum) error = %v", err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(publication)
	wanted := make(map[string]struct{}, testbackuppolicy.MaximumBackupPolicySources*3)
	for _, source := range input.Sources {
		wanted[testbackuppolicy.BackupSourceKey(source.CandidateID)] = struct{}{}
		wanted[testbackuppolicy.BackupSourceEnvironmentKey(input.EnvironmentID, source.CandidateID)] = struct{}{}
		wanted[testbackuppolicy.BackupSourceIdentityKey(input.EnvironmentID, source.Kind, source.TargetID)] = struct{}{}
	}
	triples := 0
	for _, mutation := range publication.Mutations() {
		if _, found := wanted[mutation.Key]; found {
			triples++
		}
	}
	if triples != testbackuppolicy.MaximumBackupPolicySources*3 {
		t.Fatalf("source mutations = %d, want %d", triples, testbackuppolicy.MaximumBackupPolicySources*3)
	}
	if err := validateEnvironmentBlueprintTransactionBudget(publication.Conditions(), publication.Mutations()); err != nil {
		t.Fatalf("maximum Backup publication exceeds unified envelope: %v", err)
	}
}
