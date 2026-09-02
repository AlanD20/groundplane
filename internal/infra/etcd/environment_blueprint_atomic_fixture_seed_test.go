package etcd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func seedEnvironmentBlueprintBackingScope(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	seed int64,
) environmentBlueprintBackingScope {
	t.Helper()
	ctx := context.Background()
	hierarchy, err := newHierarchyRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID:   ids.NewAt(ids.KindProject, fixture.now, seed),
		Slug: fmt.Sprintf("backing-%d", seed),
		Name: "Backing",
		Kind: ProjectKindBacking,
	})
	if err != nil {
		t.Fatalf("CreateProject(backing) error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, fixture.now, seed+1)
	environmentRecord, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		project.Record,
		environmentID,
		"main",
		fmt.Sprintf("10.%d.0.0/16", 100+(seed%100)),
		ids.NewAt(ids.KindTask, fixture.now, seed+2),
		fixture.now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment(backing) error = %v", err)
	}
	environmentValue, err := encodeEnvironment(environmentRecord)
	if err != nil {
		t.Fatalf("encodeEnvironment(backing) error = %v", err)
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environmentID,
	})
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord(backing) error = %v", err)
	}
	scriptSetValue, err := encodeScriptSetGeneration(ScriptSetGenerationRecord{
		EnvironmentID: environmentID,
		GenerationID:  environmentID,
	})
	if err != nil {
		t.Fatalf("encodeScriptSetGeneration(backing) error = %v", err)
	}
	created, err := fixture.store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(project.Record.ID, "main"), Value: []byte(environmentID)},
		{Type: MutationPut, Key: environmentOwnerKey(project.Record.ID, environmentID), Value: []byte(environmentID)},
		{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue},
		{Type: MutationPut, Key: scriptSetActiveKey(environmentID), Value: scriptSetValue},
	})
	clear(environmentValue)
	clear(epochValue)
	clear(scriptSetValue)
	if err != nil || !created.Succeeded {
		t.Fatalf("seed backing Environment = %#v, %v", created, err)
	}
	environment := Versioned[EnvironmentRecord]{
		Record: environmentRecord, Revision: created.Revision, ReadRevision: created.Revision,
	}
	service := seedDesiredServiceFixture(
		t,
		ctx,
		fixture.store,
		environment.Record.ID,
		core.Service{
			ID:          ids.NewAt(ids.KindService, fixture.now, seed+3),
			Name:        "postgres",
			Image:       "postgres:16-alpine",
			Adapter:     "postgres:16",
			FactsPrefix: "pg16_",
		},
		ids.NewAt(ids.KindNetwork, fixture.now, seed+4),
		seed+5,
		true,
		true,
	).Service
	return environmentBlueprintBackingScope{
		project:     project,
		environment: environment,
		service:     service,
	}
}

func environmentBlueprintMaximumAttachCandidate(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	task TaskRecord,
	scope environmentBlueprintBackingScope,
	consumerServiceID string,
	seed int64,
) (AttachRecord, AttachEncryptedFacts, []Versioned[AttachRecord]) {
	t.Helper()
	grants := make([]string, MaximumAttachGrants)
	retained := make([]Versioned[AttachRecord], MaximumAttachGrants)
	factSets := []AttachFactSetMetadata{{Facts: []AttachFactDefinition{{Key: "pg16_DATABASE"}}}}
	for index := range grants {
		retained[index] = environmentBlueprintRetainedGrantTarget(
			t, fixture, scope, consumerServiceID, seed+int64(index)+10,
		)
		grants[index] = retained[index].Record.ID
		factSets = append(factSets, AttachFactSetMetadata{
			GrantAttachID: grants[index],
			Facts:         []AttachFactDefinition{{Key: "pg16_DATABASE"}},
		})
	}
	attachID := ids.NewAt(ids.KindAttach, fixture.now, seed)
	record, err := NewPendingAttachRecord(
		attachID,
		fixture.environment.Record.ID,
		fmt.Sprintf("candidate-%d", seed),
		scope.project.Record.ID,
		scope.environment.Record.ID,
		scope.service.Record.Desired.ID,
		scope.service.Record.BackingNetworkID,
		consumerServiceID,
		attachID,
		grants,
		factSets,
		task.ID,
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(candidate) error = %v", err)
	}
	facts, err := NewAttachEncryptedFacts(
		attachID, 1, "age-x25519", "sha256", []byte(fmt.Sprintf("encrypted-facts-%d", seed)),
	)
	if err != nil {
		t.Fatalf("NewAttachEncryptedFacts() error = %v", err)
	}
	return record, facts, retained
}

func environmentBlueprintRetainedGrantTarget(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	scope environmentBlueprintBackingScope,
	consumerServiceID string,
	seed int64,
) Versioned[AttachRecord] {
	t.Helper()
	attachID := ids.NewAt(ids.KindAttach, fixture.now, seed)
	taskID := ids.NewAt(ids.KindTask, fixture.now, seed+1000)
	record, err := NewPendingAttachRecord(
		attachID,
		fixture.environment.Record.ID,
		fmt.Sprintf("retained-%d", seed),
		scope.project.Record.ID,
		scope.environment.Record.ID,
		scope.service.Record.Desired.ID,
		scope.service.Record.BackingNetworkID,
		consumerServiceID,
		attachID,
		nil,
		[]AttachFactSetMetadata{{Facts: []AttachFactDefinition{{Key: "pg16_DATABASE"}}}},
		taskID,
		fixture.now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(retained target) error = %v", err)
	}
	record, err = MarkAttachProvisioning(record, taskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning(retained target) error = %v", err)
	}
	record, err = CompleteAttachProvisioning(record, taskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning(retained target) error = %v", err)
	}
	value, err := encodeAttachRecord(record)
	if err != nil {
		t.Fatalf("encodeAttachRecord(retained target) error = %v", err)
	}
	result, err := fixture.store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: attachKey(record.ID), Value: value},
		{Type: MutationPut, Key: attachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachServiceKey(record.ServiceID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachBackingServiceKey(record.BackingServiceID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachBackingProjectKey(record.BackingProjectID, record.ID), Value: []byte(record.ID)},
	})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed retained Attach target = %#v, %v", result, err)
	}
	return Versioned[AttachRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}
}

func environmentBlueprintExistingBackupAttach(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	scope environmentBlueprintBackingScope,
	consumerServiceID string,
	seed int64,
) AttachRecord {
	t.Helper()
	attachID := ids.NewAt(ids.KindAttach, fixture.now, seed)
	record, err := NewPendingAttachRecord(
		attachID,
		fixture.environment.Record.ID,
		fmt.Sprintf("existing-%d", seed),
		scope.project.Record.ID,
		scope.environment.Record.ID,
		scope.service.Record.Desired.ID,
		scope.service.Record.BackingNetworkID,
		consumerServiceID,
		attachID,
		nil,
		nil,
		ids.NewAt(ids.KindTask, fixture.now, seed+100),
		fixture.now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(existing) error = %v", err)
	}
	value, err := encodeAttachRecord(record)
	if err != nil {
		t.Fatalf("encodeAttachRecord() error = %v", err)
	}
	result, err := fixture.store.Transact(
		context.Background(),
		[]Condition{
			{Key: attachKey(record.ID)},
			{Key: attachNameKey(record.EnvironmentID, record.Name)},
			{Key: attachOwnerKey(record.EnvironmentID, record.ID)},
		},
		[]Mutation{
			{Type: MutationPut, Key: attachKey(record.ID), Value: value},
			{Type: MutationPut, Key: attachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
			{Type: MutationPut, Key: attachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
		},
	)
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed existing Attach = %#v, %v", result, err)
	}
	return record
}

func seedEnvironmentBlueprintCurrentBackupPolicy(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	attachIDs []string,
) {
	t.Helper()
	ctx := context.Background()
	selections := make([]BackupPolicySourceSelection, len(attachIDs))
	for index, attachID := range attachIDs {
		if _, err := fixture.repository.EnsureBackupSource(
			ctx,
			fixture.environment,
			fixture.project,
			core.BackupSourceAttach,
			attachID,
		); err != nil {
			t.Fatalf("EnsureBackupSource(existing Attach) error = %v", err)
		}
		selections[index] = BackupPolicySourceSelection{
			Kind:     core.BackupSourceAttach,
			TargetID: attachID,
		}
	}
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		ctx,
		BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          8,
			Encryption:    "none",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources:       selections,
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement(current) error = %v", err)
	}
	defer prepared.Destroy()
	prepared, err = prepared.FinalizeSchedule(fixture.now)
	if err != nil {
		t.Fatalf("FinalizeSchedule(current) error = %v", err)
	}
	if _, err = fixture.repository.ReplaceBackupPolicyProtected(
		ctx,
		prepared,
		backupPolicyReplacementMarker(
			fixture.environment.Record.ID,
			"blueprint-shape-current-policy-0001",
		),
	); err != nil {
		t.Fatalf("ReplaceBackupPolicyProtected(current) error = %v", err)
	}
}

func ensureEnvironmentBlueprintActiveScriptSet(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
) {
	t.Helper()
	key := scriptSetActiveKey(fixture.environment.Record.ID)
	read, err := fixture.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(active Script set) error = %v", err)
	}
	if read.Entry != nil {
		return
	}
	value, err := encodeScriptSetGeneration(ScriptSetGenerationRecord{
		EnvironmentID: fixture.environment.Record.ID,
		GenerationID:  fixture.environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("encodeScriptSetGeneration() error = %v", err)
	}
	result, err := fixture.store.Transact(
		context.Background(),
		[]Condition{{Key: key}},
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed active Script set = %#v, %v", result, err)
	}
}

func environmentBlueprintStagedReleaseSources(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	task TaskRecord,
	projection EnvironmentComposeProjection,
	fixedRevision int64,
	count int,
) []ScriptSourcePreparationMember {
	t.Helper()
	if count == 0 {
		return nil
	}
	members := make([]ScriptSourcePreparationMember, count)
	for index := range members {
		serviceID := ids.NewAt(ids.KindService, fixture.now, int64(7400+index))
		key := serviceRuntimeKey(serviceID)
		value := scriptSourceServiceValue(t, fixture.environment.Record.ID, serviceID)
		digest := sha256.Sum256(value)
		members[index] = ScriptSourcePreparationMember{
			Reference: ScriptSourceReference{
				OperationID:       task.OperationID,
				ScriptExecutionID: scriptSourceReferenceExecutionID(task.CreatedAt, byte(index+50)),
				Source:            ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: serviceID},
				SourceOwnerID:     fixture.environment.Record.ID,
			},
			Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{
				SourceKey: key,
				Stage: ScriptCandidateSourceStage{
					EnvironmentID:        fixture.environment.Record.ID,
					RevisionID:           task.ID,
					RenderGeneration:     projection.RenderGeneration,
					FixedReadRevision:    fixedRevision,
					CanonicalValueSHA256: digest,
				},
				Value: value,
			}},
		}
	}
	return members
}

func prepareEnvironmentBlueprintReleaseShape(
	t *testing.T,
	store *memoryHierarchyStore,
	task *TaskRecord,
	environmentID string,
	releaseCount int,
	hookCount int,
	sourceMembers []ScriptSourcePreparationMember,
) (BlueprintReleasePublication, string) {
	t.Helper()
	ctx := context.Background()
	publicationID := scriptSourceReferenceExecutionID(task.CreatedAt, 240)
	task.Params[TaskReleasePublicationParam] = publicationID
	manifest := ReleaseStagedManifest{
		PublicationID: publicationID,
		OperationID:   task.OperationID,
		Members:       make([]ReleaseStagedMemberRef, releaseCount),
		CreatedAt:     task.CreatedAt,
	}
	for index := range manifest.Members {
		manifest.Members[index] = ReleaseStagedMemberRef{
			ReleaseID:        ids.NewAt(ids.KindDeployment, task.CreatedAt, int64(7500+index)),
			ServiceID:        ids.NewAt(ids.KindService, task.CreatedAt, int64(7600+index)),
			IntentDigest:     strings.Repeat("a", 64),
			RenderDigest:     strings.Repeat("b", 64),
			CheckpointDigest: strings.Repeat("c", 64),
		}
	}
	var err error
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatalf("blueprintCandidateManifestDigest() error = %v", err)
	}
	manifestValue, err := encodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatalf("encodeReleaseRecord(manifest) error = %v", err)
	}
	manifestResult, err := store.Transact(
		ctx,
		[]Condition{{Key: releaseManifestStagingKey(publicationID)}},
		[]Mutation{{Type: MutationPut, Key: releaseManifestStagingKey(publicationID), Value: manifestValue}},
	)
	clear(manifestValue)
	if err != nil || !manifestResult.Succeeded {
		t.Fatalf("stage Release manifest = %#v, %v", manifestResult, err)
	}

	hooks := make([]ReleaseHookExecutionPublication, hookCount)
	for index := range hooks {
		record := scriptCheckpointTestRecord(task.CreatedAt.Add(time.Duration(index+1) * time.Millisecond))
		record.OperationID = task.OperationID
		record.CurrentTaskID = task.ID
		record.PlanHash = task.PlanHash
		record.EnvironmentID = environmentID
		record.ReleaseID = manifest.Members[index%len(manifest.Members)].ReleaseID
		record.ServiceID = manifest.Members[index%len(manifest.Members)].ServiceID
		record.StepID = ids.NewAt(ids.KindStep, task.CreatedAt, int64(7700+index))
		task.Params[ReleaseHookStepExecutionParam(record.StepID)] = record.ID
		task.Steps = append(task.Steps, TaskStepRecord{Kind: TaskStepOperation, ID: record.StepID})
		hooks[index] = ReleaseHookExecutionPublication{Execution: record}
	}
	ledger := &ReleaseLedger{store: &releasePlanningTestStore{memoryHierarchyStore: store}}
	hookPrepared, err := ledger.PrepareBlueprintReleaseHooks(ctx, *task, hooks)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseHooks() error = %v", err)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	sourcePrepared, err := authority.Prepare(ctx, task.OperationID, sourceMembers)
	if err != nil {
		t.Fatalf("Prepare(Script sources) error = %v", err)
	}
	publication, err := ledger.PrepareBlueprintReleasePublication(
		ctx,
		authority,
		BlueprintReleasePublicationEvidence{
			Manifest: VersionedReleaseManifest{
				Record:       manifest,
				Revision:     manifestResult.Revision,
				ReadRevision: manifestResult.Revision,
			},
			EnvironmentID:  environmentID,
			Task:           *task,
			Hooks:          hooks,
			PublishedAt:    task.CreatedAt.Add(time.Minute),
			SourcePrepared: sourcePrepared,
			SourceMembers:  sourceMembers,
			HookPrepared:   hookPrepared,
		},
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleasePublication() error = %v", err)
	}
	return publication, publicationID
}

func environmentBlueprintVolumeIDs(projection EnvironmentComposeProjection) []string {
	ids := make([]string, len(projection.Volumes))
	for index, volume := range projection.Volumes {
		ids[index] = volume.ID
	}
	return ids
}
