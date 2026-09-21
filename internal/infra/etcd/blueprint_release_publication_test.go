package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestBlueprintReleasePublicationCarriesPreparedSourceRootAndAbandonsExactly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	prepared, err := authority.Prepare(ctx, operationID, members)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	publication, err := newBlueprintReleasePublication(
		blueprintReleasePublicationInput{
			EnvironmentID:   members[0].Reference.SourceOwnerID,
			OperationID:     operationID,
			SourceFragment:  fragment,
			SourceAuthority: authority,
			SourceMembers:   members,
		},
	)
	if err != nil {
		t.Fatalf("newBlueprintReleasePublication() error = %v", err)
	}
	if publication.IsZero() || len(publication.mutations) != len(fragment.Mutations()) {
		t.Fatalf("publication = %#v", publication)
	}
	if err := publication.Abandon(ctx); err != nil {
		t.Fatalf("Abandon() error = %v", err)
	}
	if read, readErr := store.Get(ctx, testscriptsourcereference.PreparationPrefix+operationID); readErr != nil ||
		read.Entry != nil {
		t.Fatalf("prepared source descriptor after abandon = %#v, %v", read, readErr)
	}
}

func TestBlueprintReleaseHookPublicationStoresExecutionAndSnapshot(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 13, 0, 0, 0, time.UTC)
	record := withScriptContextSnapshot(t, scriptCheckpointTestRecord(at))
	task := TaskRecord{
		ID: record.CurrentTaskID, OperationID: record.OperationID, PlanHash: record.PlanHash,
		Params: map[string]string{testreleaserender.ReleaseHookStepExecutionParam(record.StepID): record.ID},
	}
	fragment, err := prepareBlueprintReleaseHookPublicationFragment(
		task,
		[]ReleaseHookExecutionPublication{{Execution: record}},
	)
	if err != nil {
		t.Fatalf("prepareBlueprintReleaseHookPublicationFragment() error = %v", err)
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(fragment.mutations) != 2 ||
		fragment.mutations[0].Key != testscriptexecutions.ScriptExecutionKey(record.ID) ||
		fragment.mutations[1].Key != testscriptexecutions.ScriptRunnerSnapshotKey(record.SnapshotID) {
		t.Fatalf("Blueprint hook mutations = %#v", fragment.mutations)
	}
	stored, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		fragment.mutations[0].Value,
		"script-execution",
	)
	if err != nil || stored.ID != record.ID || stored.CurrentTaskID != task.ID {
		t.Fatalf("stored Blueprint Script execution = %#v, error = %v", stored, err)
	}
	snapshot, err := testrecordcodec.Decode[testscriptsourceevidence.StoredScriptRunnerSnapshot](
		fragment.mutations[1].Value,
		"script-runner-snapshot",
	)
	if err != nil || snapshot.ExecutionID != record.ID || snapshot.SnapshotID != record.SnapshotID {
		t.Fatalf("stored Blueprint runner snapshot = %#v, error = %v", snapshot, err)
	}
}

func TestBlueprintReleaseSourceMembersUsesStoredSecretEntryCiphertextDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	at := time.Date(2026, 9, 2, 13, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	releaseID := ids.NewAt(ids.KindDeployment, at, 3)
	entryID := ids.NewAt(ids.KindEnvEntry, at, 4)
	generationID := ids.NewAt(ids.KindConfig, at, 5)
	revisionID := ids.NewAt(ids.KindTask, at, 6)
	scriptID := ids.NewAt(ids.KindScript, at, 7)
	operationID := ids.NewAt(ids.KindOperation, at, 8)
	executionID := scriptSourceReferenceExecutionID(at, 9)
	snapshotID := scriptSourceReferenceExecutionID(at, 10)
	publicationID := scriptSourceReferenceExecutionID(at, 11)
	networkID := ids.NewAt(ids.KindNetwork, at, 12)
	volumeID := ids.NewAt(ids.KindVolume, at, 13)
	plaintextDigest := sha256.Sum256([]byte("resolved secret plaintext"))
	ciphertext := []byte("stored age ciphertext")
	ciphertextDigest := sha256.Sum256(ciphertext)

	generationValue, err := testentryvalues.EncodeSecret(testentryvalues.SecretGeneration{
		EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(ciphertextDigest[:]), Ciphertext: ciphertext, CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("encodeSecretEntryValueGeneration() error = %v", err)
	}
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testentryvalues.SecretKey(entryID, generationID), Value: generationValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseIntentStagingKey(publicationID, releaseID),
			Value: []byte("release intent"),
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed secret Entry generation = %#v, %v", seed, err)
	}

	service := testservices.ServiceRecord{
		EnvironmentID: environmentID,
		Desired:       core.Service{ID: serviceID},
		Runtime: core.ServiceRuntime{
			ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	serviceValue, err := testservices.EncodeServiceRuntimeRecordStorage(service)
	if err != nil {
		t.Fatalf("EncodeServiceRuntimeRecordStorage() error = %v", err)
	}
	serviceDigest := sha256.Sum256(serviceValue)
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID,
		EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: releaseID,
		ServiceSource: &agentpb.ScriptSourceAuthority{Staged: &agentpb.ScriptStagedSourceAuthority{
			EnvironmentId: environmentID, RevisionId: revisionID, RenderGeneration: 1,
			FixedReadRevision: uint64(seed.Revision), CanonicalValueSha256: serviceDigest[:],
		}},
		Networks: []*agentpb.ScriptRunnerNetwork{{NetworkId: networkID, OwnerEnvironmentId: environmentID}},
		Mounts:   []*agentpb.ScriptRunnerMount{{SourceId: volumeID}},
		EntryBindings: []*agentpb.ScriptRunnerEntryBinding{{
			EntryId: entryID, ValueGenerationId: generationID,
			Sha256: plaintextDigest[:], Secret: true,
		}},
	}
	snapshotValue, err := proto.Marshal(snapshot)
	if err != nil {
		t.Fatalf("proto.Marshal(runner snapshot) error = %v", err)
	}
	execution := testscriptexecutions.ScriptExecutionRecord{
		ID: executionID, SnapshotID: snapshotID, OperationID: operationID,
		ScriptID: scriptID, ScriptGeneration: 1, EnvironmentID: environmentID,
		ServiceID: serviceID, ReleaseID: releaseID, Snapshot: snapshotValue,
		BodySHA256: hex.EncodeToString(sha256.New().Sum(nil)),
	}
	snapshotDigest := sha256.Sum256(snapshotValue)
	execution.SnapshotSHA256 = hex.EncodeToString(snapshotDigest[:])
	storedSnapshotValue, err := encodeBlueprintReleaseHookSnapshot(execution)
	if err != nil {
		t.Fatalf("encodeBlueprintReleaseHookSnapshot() error = %v", err)
	}
	snapshotSeed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID), Value: storedSnapshotValue,
	}})
	if err != nil || !snapshotSeed.Succeeded {
		t.Fatalf("seed runner snapshot = %#v, %v", snapshotSeed, err)
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
	})
	members, err := (releaseLedgerFixture(t, store)).BlueprintReleaseSourceMembers(
		ctx, testreleases.VersionedReleaseManifest{
			Record:       testreleases.ReleaseStagedManifest{PublicationID: publicationID, OperationID: operationID},
			ReadRevision: seed.Revision,
		}, []ReleaseHookExecutionPublication{{
			Sources: testscriptsourcequeries.ScriptExecutionSources{
				Revision: seed.Revision,
				Service:  testkeyvalue.Versioned[testservices.ServiceRecord]{Record: service},
				Script: testkeyvalue.Versioned[testscripts.Record]{Record: testscripts.Record{
					ScriptSetGeneration: revisionID,
				}},
				DesiredProjection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
					Record: projection,
				},
			},
			Execution: execution, SnapshotRevision: snapshotSeed.Revision,
		}},
	)
	if err != nil {
		t.Fatalf("BlueprintReleaseSourceMembers() error = %v", err)
	}
	var entryMember, networkMember, volumeMember testscriptsourceevidence.ScriptSourcePreparationMember
	for _, member := range members {
		switch member.Reference.Source.Kind {
		case testscriptsourcereference.SourceEntryValue:
			entryMember = member
		case testscriptsourcereference.SourceNetwork:
			networkMember = member
		case testscriptsourcereference.SourceVolume:
			volumeMember = member
		}
	}
	if entryMember.Reference.SourceDigest != hex.EncodeToString(ciphertextDigest[:]) {
		t.Fatalf("secret Entry source digest = %q, want ciphertext digest", entryMember.Reference.SourceDigest)
	}
	for _, member := range []testscriptsourceevidence.ScriptSourcePreparationMember{networkMember, volumeMember} {
		if member.Evidence.Existing == nil || member.Evidence.Staged != nil ||
			member.Evidence.Existing.SourceKey != testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID) ||
			member.Reference.SourceModRevision != snapshotSeed.Revision ||
			member.Reference.SourceDigest != execution.SnapshotSHA256 {
			t.Fatalf("runner snapshot source member = %#v", member)
		}
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatalf("newScriptSourceReferenceAuthority() error = %v", err)
	}
	if _, err = authority.Prepare(ctx, operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{
		entryMember, networkMember, volumeMember,
	}); err != nil {
		t.Fatalf("Prepare(secret Entry and runner snapshot sources) error = %v", err)
	}
	if !bytes.Equal(snapshot.EntryBindings[0].Sha256, plaintextDigest[:]) {
		t.Fatalf("runner binding digest = %x, want plaintext digest", snapshot.EntryBindings[0].Sha256)
	}
}

// Rationale: distinct Blueprint hook executions retain their own immutable runner-snapshot evidence when they share
// one logical Network and Volume.
func TestBlueprintReleaseSourceMembersAcceptsSharedNetworkAndVolumeSnapshots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	at := time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	releaseID := ids.NewAt(ids.KindDeployment, at, 3)
	revisionID := ids.NewAt(ids.KindTask, at, 4)
	scriptID := ids.NewAt(ids.KindScript, at, 5)
	operationID := ids.NewAt(ids.KindOperation, at, 6)
	publicationID := scriptSourceReferenceExecutionID(at, 7)
	networkID := ids.NewAt(ids.KindNetwork, at, 8)
	volumeID := ids.NewAt(ids.KindVolume, at, 9)
	service := testservices.ServiceRecord{
		EnvironmentID: environmentID,
		Desired:       core.Service{ID: serviceID},
		Runtime: core.ServiceRuntime{
			ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	serviceValue, err := testservices.EncodeServiceRuntimeRecordStorage(service)
	if err != nil {
		t.Fatalf("EncodeServiceRuntimeRecordStorage() error = %v", err)
	}
	serviceDigest := sha256.Sum256(serviceValue)
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseIntentStagingKey(publicationID, releaseID), Value: []byte("release intent"),
	}})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Release intent = %#v, %v", seed, err)
	}
	makeHook := func(idSeed byte) ReleaseHookExecutionPublication {
		t.Helper()
		executionID := scriptSourceReferenceExecutionID(at, idSeed)
		snapshotID := scriptSourceReferenceExecutionID(at, idSeed+1)
		snapshot := &agentpb.ResolvedRunnerSnapshot{
			SnapshotId: snapshotID, ScriptExecutionId: executionID,
			EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: releaseID,
			ServiceSource: &agentpb.ScriptSourceAuthority{Staged: &agentpb.ScriptStagedSourceAuthority{
				EnvironmentId: environmentID, RevisionId: revisionID, RenderGeneration: 1,
				FixedReadRevision: uint64(seed.Revision), CanonicalValueSha256: serviceDigest[:],
			}},
			Networks: []*agentpb.ScriptRunnerNetwork{{NetworkId: networkID, OwnerEnvironmentId: environmentID}},
			Mounts:   []*agentpb.ScriptRunnerMount{{SourceId: volumeID}},
		}
		snapshotValue, marshalErr := proto.Marshal(snapshot)
		if marshalErr != nil {
			t.Fatalf("proto.Marshal(runner snapshot) error = %v", marshalErr)
		}
		snapshotDigest := sha256.Sum256(snapshotValue)
		execution := testscriptexecutions.ScriptExecutionRecord{
			ID: executionID, SnapshotID: snapshotID, OperationID: operationID,
			ScriptID: scriptID, ScriptGeneration: 1, EnvironmentID: environmentID,
			ServiceID: serviceID, ReleaseID: releaseID, Snapshot: snapshotValue,
			SnapshotSHA256: hex.EncodeToString(snapshotDigest[:]),
			BodySHA256:     hex.EncodeToString(sha256.New().Sum(nil)),
		}
		storedSnapshotValue, encodeErr := encodeBlueprintReleaseHookSnapshot(execution)
		if encodeErr != nil {
			t.Fatalf("encodeBlueprintReleaseHookSnapshot() error = %v", encodeErr)
		}
		snapshotSeed, seedErr := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID), Value: storedSnapshotValue,
		}})
		if seedErr != nil || !snapshotSeed.Succeeded {
			t.Fatalf("seed runner snapshot = %#v, %v", snapshotSeed, seedErr)
		}
		return ReleaseHookExecutionPublication{
			Sources: testscriptsourcequeries.ScriptExecutionSources{
				Revision: seed.Revision,
				Service:  testkeyvalue.Versioned[testservices.ServiceRecord]{Record: service},
				Script: testkeyvalue.Versioned[testscripts.Record]{Record: testscripts.Record{
					ScriptSetGeneration: revisionID,
				}},
				BodyGeneration: testkeyvalue.Versioned[testscripts.BodyGenerationRecord]{Revision: seed.Revision},
			},
			Execution: execution, SnapshotRevision: snapshotSeed.Revision,
		}
	}
	hooks := []ReleaseHookExecutionPublication{makeHook(10), makeHook(20)}
	members, err := (releaseLedgerFixture(t, store)).BlueprintReleaseSourceMembers(
		ctx, testreleases.VersionedReleaseManifest{
			Record:       testreleases.ReleaseStagedManifest{PublicationID: publicationID, OperationID: operationID},
			ReadRevision: seed.Revision,
		}, hooks,
	)
	if err != nil {
		t.Fatalf("BlueprintReleaseSourceMembers() error = %v", err)
	}
	physicalMembers := make([]testscriptsourceevidence.ScriptSourcePreparationMember, 0, 4)
	for _, member := range members {
		if member.Reference.Source.Kind == testscriptsourcereference.SourceNetwork ||
			member.Reference.Source.Kind == testscriptsourcereference.SourceVolume {
			physicalMembers = append(physicalMembers, member)
		}
	}
	if len(physicalMembers) != 4 {
		t.Fatalf("physical source membership count = %d, want 4", len(physicalMembers))
	}
	expectedSnapshots := map[string]ReleaseHookExecutionPublication{
		hooks[0].Execution.ID: hooks[0],
		hooks[1].Execution.ID: hooks[1],
	}
	networkCount, volumeCount := 0, 0
	for _, member := range physicalMembers {
		hook := expectedSnapshots[member.Reference.ScriptExecutionID]
		if member.Evidence.Existing == nil ||
			member.Evidence.Existing.SourceKey != testscriptexecutions.ScriptRunnerSnapshotKey(
				hook.Execution.SnapshotID,
			) ||
			member.Reference.SourceModRevision != hook.SnapshotRevision ||
			member.Reference.SourceDigest != hook.Execution.SnapshotSHA256 {
			t.Fatalf("execution-specific physical source evidence = %#v", member)
		}
		switch member.Reference.Source.Kind {
		case testscriptsourcereference.SourceNetwork:
			networkCount++
			if member.Reference.Source.NetworkID != networkID {
				t.Fatalf("Network identity = %#v, want %q", member.Reference.Source, networkID)
			}
		case testscriptsourcereference.SourceVolume:
			volumeCount++
			if member.Reference.Source.VolumeID != volumeID {
				t.Fatalf("Volume identity = %#v, want %q", member.Reference.Source, volumeID)
			}
		}
	}
	if networkCount != 2 || volumeCount != 2 {
		t.Fatalf("physical source identity counts = Network %d, Volume %d, want 2 each", networkCount, volumeCount)
	}
	if hooks[0].SnapshotRevision == hooks[1].SnapshotRevision ||
		hooks[0].Execution.SnapshotSHA256 == hooks[1].Execution.SnapshotSHA256 {
		t.Fatal("runner snapshots did not retain distinct revisions and digests")
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatalf("newScriptSourceReferenceAuthority() error = %v", err)
	}
	prepared, err := authority.Prepare(ctx, operationID, physicalMembers)
	if err != nil {
		t.Fatalf("Prepare(shared Network and Volume snapshots) error = %v", err)
	}
	if prepared.MembershipCount() != 4 {
		t.Fatalf("prepared physical source membership count = %d, want 4", prepared.MembershipCount())
	}
}

func TestBlueprintStagedSourceEvidenceRejectsChangedCandidateBytes(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	expected := []byte("candidate-service-bytes")
	digest := sha256.Sum256(expected)
	authority := &agentpb.ScriptSourceAuthority{
		Staged: &agentpb.ScriptStagedSourceAuthority{
			EnvironmentId:        environmentID,
			RevisionId:           revisionID,
			RenderGeneration:     7,
			FixedReadRevision:    19,
			CanonicalValueSha256: digest[:],
		},
	}
	if _, err := blueprintStagedSourceEvidence(
		authority, testservices.ServiceRuntimeKey(ids.NewAt(ids.KindService, at, 3)), []byte("changed-service-bytes"),
		environmentID,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("blueprintStagedSourceEvidence(changed bytes) error = %v", err)
	}
}

// Rationale: final Blueprint publication owns desired state only; neither an
// applied predecessor nor applied absence may advance before terminal Task acknowledgement.
func TestBlueprintReleasePublicationLeavesAppliedProjectionUntilAcknowledgement(t *testing.T) {
	t.Parallel()
	for _, appliedPresent := range []bool{false, true} {
		appliedPresent := appliedPresent
		t.Run(map[bool]string{false: "absence", true: "predecessor"}[appliedPresent], func(t *testing.T) {
			t.Parallel()
			store, operationID, members, stage, _, _ := scriptRunnerSnapshotSourceFixture(t)
			projectionKey := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(stage.EnvironmentID)
			appliedBefore := store.valueAt(projectionKey, store.revision)
			if !appliedPresent {
				removed, removeErr := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
					Type: testkeyvalue.MutationDelete, Key: projectionKey,
				}})
				if removeErr != nil || !removed.Succeeded {
					t.Fatalf("remove applied predecessor = %#v, %v", removed, removeErr)
				}
			}
			serviceID := ids.NewAt(ids.KindService, time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), 168)
			serviceKey := testservices.ServiceRuntimeKey(serviceID)
			serviceValue := scriptSourceServiceValue(t, stage.EnvironmentID, serviceID)
			serviceStage := stage
			serviceStage.CanonicalValueSHA256 = sha256.Sum256(serviceValue)
			members = append(members, testscriptsourceevidence.ScriptSourcePreparationMember{
				Reference: testscriptsourcereference.Reference{
					OperationID: operationID, ScriptExecutionID: members[0].Reference.ScriptExecutionID,
					Source: testscriptsourcereference.SourceIdentity{
						Kind:      testscriptsourcereference.SourceService,
						ServiceID: serviceID,
					},
					SourceOwnerID: stage.EnvironmentID,
				},
				Evidence: testscriptsourceevidence.ScriptSourceEvidence{
					Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
						SourceKey: serviceKey, Stage: serviceStage, Value: serviceValue,
					},
				},
			})
			authority, err := testscriptsourcepublication.NewAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := authority.Prepare(context.Background(), operationID, members)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			sourceFragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
			if err != nil {
				t.Fatalf("FinalPublicationFragment() error = %v", err)
			}
			publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
				EnvironmentID: stage.EnvironmentID, OperationID: operationID,
				SourceFragment: sourceFragment, SourceAuthority: authority, SourceMembers: members,
			})
			if err != nil {
				t.Fatalf("newBlueprintReleasePublication() error = %v", err)
			}
			defer publication.Clear()
			if len(publication.sources.StagedRequirements()) != 1 ||
				len(publication.conditions) != 3 || len(publication.mutations) != 3 {
				t.Fatalf(
					"publication staged shape = %d requirements, %d conditions, %d mutations",
					len(
						publication.sources.StagedRequirements(),
					),
					len(publication.conditions),
					len(publication.mutations),
				)
			}
			selected := map[string]int64{}
			for _, condition := range publication.conditions {
				if condition.Key == projectionKey || condition.Key == serviceKey {
					selected[condition.Key] = condition.ModRevision
				}
			}
			if _, found := selected[projectionKey]; found || selected[serviceKey] != 0 {
				t.Fatalf("selected staged predecessors = %#v", selected)
			}
			claim := testblueprints.EnvironmentBlueprintStageClaim{
				EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
				RenderGeneration: stage.RenderGeneration,
			}
			if err = publication.sources.ValidateStagedMutations(claim, publication.mutations); err != nil {
				t.Fatalf("ValidateStagedMutations(production fragment) error = %v", err)
			}
			result, err := store.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil {
				t.Fatalf("publish Blueprint fragment error = %v", err)
			}
			if !result.Succeeded {
				t.Fatal("exact Blueprint publication did not commit")
			}
			storedApplied := store.valueAt(projectionKey, store.revision)
			if appliedPresent {
				if storedApplied == nil || !bytes.Equal(storedApplied.Value, appliedBefore.Value) ||
					storedApplied.ModRevision != appliedBefore.ModRevision {
					t.Fatalf("Blueprint publication changed applied predecessor = %#v", storedApplied)
				}
			} else if storedApplied != nil {
				t.Fatalf("Blueprint publication advanced applied absence = %#v", storedApplied)
			}
			if stored := store.valueAt(serviceKey, store.revision); stored == nil ||
				!bytes.Equal(stored.Value, serviceValue) {
				t.Fatal("Blueprint publication omitted the candidate Service")
			}
		})
	}
}
