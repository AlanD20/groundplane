package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: manual runs must reserve the full immutable runner source set,
// counting a repeated mount only once rather than pinning only the Script body.
func TestManualScriptSourceMembersPrepareExactRunnerSources(t *testing.T) {
	store, sources, execution, snapshotRevision := manualScriptReferenceFixture(t)
	members, err := (&ScriptRepository{store: store}).manualScriptSourceMembers(
		context.Background(), sources, execution, snapshotRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 6 {
		t.Fatalf("source memberships = %d, want body, snapshot, Service, Release, Network, Volume", len(members))
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := authority.Prepare(context.Background(), execution.OperationID, members)
	if err != nil || prepared.membershipCount != 6 {
		t.Fatalf("prepare complete manual sources = %#v, %v", prepared, err)
	}
	for _, member := range members {
		if member.Evidence.Existing == nil || member.Evidence.Staged != nil {
			t.Fatalf("manual source is not fixed existing evidence: %#v", member.Reference.Source)
		}
		countValue := store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision)
		if countValue == nil {
			t.Fatalf("source has no removal fence: %#v", member.Reference.Source)
		}
		count, decodeErr := decodeScriptSourceCount(countValue.Value)
		if decodeErr != nil || count.ReferencedExecutionCount != 1 {
			t.Fatalf("source execution count = %#v, %v", count, decodeErr)
		}
	}
}

// Rationale: a snapshot-backed Service count is legal only for the exact
// execution's sealed Service and Environment, not any id sharing its snapshot.
func TestManualScriptServiceSnapshotMembershipRejectsSubstitution(t *testing.T) {
	for _, change := range []string{"service", "owner", "digest"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			store, sources, execution, snapshotRevision := manualScriptReferenceFixture(t)
			members, err := (&ScriptRepository{store: store}).manualScriptSourceMembers(
				ctx,
				sources,
				execution,
				snapshotRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for index := range members {
				member := &members[index]
				if member.Reference.Source.Kind != ScriptSourceService {
					continue
				}
				found = true
				if member.Evidence.Existing.SourceKey != scriptRunnerSnapshotKey(execution.SnapshotID) ||
					member.Reference.SourceModRevision != snapshotRevision || member.Reference.SourceDigest != execution.SnapshotSHA256 {
					t.Fatal("manual Service was not pinned to its sealed snapshot")
				}
				switch change {
				case "service":
					member.Reference.Source.ServiceID = ids.New(ids.KindService)
				case "owner":
					member.Reference.SourceOwnerID = ids.New(ids.KindEnvironment)
				case "digest":
					member.Reference.SourceDigest = scriptSourceReferenceDigest("wrong snapshot")
				}
			}
			if !found {
				t.Fatal("manual Service membership absent")
			}
			authority, err := newScriptSourceReferenceAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			revision := store.revision
			if _, err := authority.Prepare(ctx, execution.OperationID, members); err == nil ||
				store.revision != revision {
				t.Fatalf("substituted Service source reserved state: %v", err)
			}
		})
	}
}

// Rationale: neither an unprepared snapshot nor substituted snapshot bytes may
// become immutable input authority for an execution that names another digest.
func TestManualScriptSourceMembersRejectUnsealedSnapshot(t *testing.T) {
	for _, malformed := range []string{"unprepared", "digest", "execution"} {
		t.Run(malformed, func(t *testing.T) {
			store, sources, execution, revision := manualScriptReferenceFixture(t)
			switch malformed {
			case "unprepared":
				revision = 0
			case "digest":
				execution.SnapshotSHA256 = scriptSourceReferenceDigest("substituted")
			case "execution":
				execution.ID = scriptSourceReferenceExecutionID(scriptSourceReferenceTestTime(), 99)
			}
			members, err := (&ScriptRepository{store: store}).manualScriptSourceMembers(
				context.Background(), sources, execution, revision,
			)
			if err == nil || len(members) != 0 {
				t.Fatalf("invalid snapshot accepted: %d members, %v", len(members), err)
			}
		})
	}
}

// Rationale: a backing-owned Network must fence its actual owner's deletion,
// and a forged consumer owner must fail even though the runner belongs there.
func TestManualScriptSourceMembersRetainBackingNetworkOwner(t *testing.T) {
	store, sources, execution, _ := manualScriptReferenceFixture(t)
	backingID := ids.NewAt(ids.KindEnvironment, scriptSourceReferenceTestTime(), 98)
	var snapshot agentpb.ResolvedRunnerSnapshot
	if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Networks[0].OwnerEnvironmentId = backingID
	var err error
	execution.Snapshot, err = proto.Marshal(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	execution.SnapshotSHA256 = scriptSourceReferenceBytesDigest(execution.Snapshot)
	value, err := encodeBlueprintReleaseHookSnapshot(execution)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: value,
	}})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed backing Network snapshot = %#v, %v", seed, err)
	}
	members, err := (&ScriptRepository{store: store}).manualScriptSourceMembers(
		context.Background(),
		sources,
		execution,
		seed.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if member.Reference.Source.Kind != ScriptSourceNetwork {
			continue
		}
		if member.Reference.SourceOwnerID != backingID {
			t.Fatalf("backing Network owner = %s, want %s", member.Reference.SourceOwnerID, backingID)
		}
		if _, err := authority.validateMembers(context.Background(), execution.OperationID, []ScriptSourcePreparationMember{member}, true); err != nil {
			t.Fatalf("exact backing Network owner rejected: %v", err)
		}
		member.Reference.SourceOwnerID = execution.EnvironmentID
		if _, err := authority.validateMembers(context.Background(), execution.OperationID, []ScriptSourcePreparationMember{member}, true); err == nil {
			t.Fatal("forged consumer ownership accepted for backing Network")
		}
	}
}

// Rationale: Entry bindings are outputs of fixed-revision resolution, never
// caller authority; the source set retains both an encrypted generation and
// its reusable Secret without using the plaintext execution digest as storage proof.
func TestManualScriptSourceMembersRetainResolvedEntryAndSecret(t *testing.T) {
	store, sources, execution, _ := manualScriptReferenceFixture(t)
	at := scriptSourceReferenceTestTime()
	entryID, generationID := ids.NewAt(ids.KindEnvEntry, at, 91), ids.NewAt(ids.KindConfig, at, 92)
	secretID, projectID := ids.NewAt(ids.KindSecret, at, 93), ids.NewAt(ids.KindProject, at, 94)
	ciphertext := []byte("test-only encrypted bytes")
	cipherDigest := scriptSourceReferenceBytesDigest(ciphertext)
	generation, err := encodeSecretEntryValueGeneration(SecretEntryValueGeneration{
		EnvironmentID: execution.EnvironmentID, EntryID: entryID, GenerationID: generationID,
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: cipherDigest, Ciphertext: ciphertext, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := NewProjectSecretRecord(secretID, projectID, "MANUAL_TEST", core.SecretKindEnvVar, "", at)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeSecretRecord(secret)
	if err != nil {
		t.Fatal(err)
	}
	secretValue, err := encodeSecretEncryptedValue(SecretEncryptedValue{
		SecretID: secretID, EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: cipherDigest, Ciphertext: ciphertext,
	})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: secretEntryValueGenerationKey(entryID, generationID), Value: generation},
		{Type: MutationPut, Key: secretRecordKey(secretID), Value: metadata},
		{Type: MutationPut, Key: secretValueKey(secretID), Value: secretValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed encrypted sources = %#v, %v", seed, err)
	}
	sources.Revision = seed.Revision
	sources.Project.Record.ID = projectID
	sources.DesiredProjection.Record.Entries = []EntryRecord{{
		EnvironmentID: execution.EnvironmentID, CurrentValueGenerationID: generationID,
		Entry: core.EnvEntry{ID: entryID, Kind: core.EntryKindEnv, Key: "MANUAL_TEST", Secret: true,
			Source: core.EntrySource{Kind: core.SourceSecretRef, SecretRef: secretID}, Exposure: []string{"all"}},
	}}
	var snapshot agentpb.ResolvedRunnerSnapshot
	if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.EntryBindings = []*agentpb.ScriptRunnerEntryBinding{{
		EntryId: entryID, ValueGenerationId: generationID, Secret: true,
		Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "MANUAL_TEST",
	}}
	execution.Snapshot, err = proto.Marshal(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	execution.SnapshotSHA256 = scriptSourceReferenceBytesDigest(execution.Snapshot)
	encoded, err := encodeBlueprintReleaseHookSnapshot(execution)
	if err != nil {
		t.Fatal(err)
	}
	preparedSnapshot, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: encoded,
	}})
	if err != nil || !preparedSnapshot.Succeeded {
		t.Fatalf("seed resolved snapshot = %#v, %v", preparedSnapshot, err)
	}
	repository := &ScriptRepository{store: store}
	members, err := repository.manualScriptSourceMembers(
		context.Background(),
		sources,
		execution,
		preparedSnapshot.Revision,
	)
	if err != nil || len(members) != 8 {
		t.Fatalf("resolved Entry and Secret source set = %d, %v", len(members), err)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(context.Background(), execution.OperationID, members); err != nil {
		t.Fatalf("reserve resolved encrypted sources: %v", err)
	}
	for _, member := range members {
		if member.Reference.Source.Kind == ScriptSourceEntryValue ||
			member.Reference.Source.Kind == ScriptSourceSecretValue {
			if member.Reference.SourceDigest != cipherDigest {
				t.Fatalf("encrypted source did not retain ciphertext authority: %#v", member.Reference.Source)
			}
		}
	}
	// A sealed snapshot cannot nominate a generation absent from its fixed desired projection.
	sources.DesiredProjection.Record.Entries = nil
	if _, err := repository.manualScriptSourceMembers(context.Background(), sources, execution, preparedSnapshot.Revision); err == nil {
		t.Fatal("caller-supplied Entry binding accepted without fixed-revision selection")
	}
}

func manualScriptReferenceFixture(
	t *testing.T,
) (*memoryHierarchyStore, ScriptExecutionSources, ScriptExecutionRecord, int64) {
	t.Helper()
	store, _, operationID, baseline := scriptSourceReferenceFixture(t)
	at := scriptSourceReferenceTestTime()
	body, service := baseline[0].Reference, baseline[1].Reference
	environmentID, executionID := body.SourceOwnerID, body.ScriptExecutionID
	releaseID := ids.NewAt(ids.KindDeployment, at, 71)
	snapshotID := scriptSourceReferenceExecutionID(at, 72)
	networkID, volumeID := ids.NewAt(ids.KindNetwork, at, 73), ids.NewAt(ids.KindVolume, at, 74)
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: service.Source.ServiceID, OperationID: operationID,
		OperationKind: domain.OperationDeploy, CandidateWorkload: releaseTestWorkloadSeal("registry.example/app:v1"),
		Tag: "v1", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureLeaveActive,
		RenderInputID: ids.NewAt(ids.KindConfig, at, 77), RenderInputDigest: scriptSourceReferenceDigest("render"),
		CreatedAt: at, Actor: "test", OriginatingTaskID: ids.NewAt(ids.KindTask, at, 78),
		Workspace: domain.Workspace{Kind: domain.WorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, at, 79),
			ProjectID: ids.NewAt(ids.KindProject, at, 80), EnvironmentID: environmentID},
	}
	releaseValue, err := encodeEnvelope("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: releaseIntentStagingKey("", releaseID), Value: releaseValue,
	}})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Release = %#v, %v", seed, err)
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: environmentID,
		ServiceId: service.Source.ServiceID, ReleaseId: releaseID,
		Networks: []*agentpb.ScriptRunnerNetwork{{NetworkId: networkID, OwnerEnvironmentId: environmentID}},
		Mounts:   []*agentpb.ScriptRunnerMount{{SourceId: volumeID}, {SourceId: volumeID}},
	}
	payload, err := proto.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	execution := ScriptExecutionRecord{
		ID: executionID, SnapshotID: snapshotID, OperationID: operationID,
		EnvironmentID: environmentID, ServiceID: service.Source.ServiceID, ReleaseID: releaseID,
		ScriptID: body.Source.ScriptID, ScriptGeneration: body.Source.BodyGeneration,
		ScriptSetGeneration: body.Source.ScriptSetGeneration, BodySHA256: body.SourceDigest,
		Snapshot: payload, SnapshotSHA256: scriptSourceReferenceBytesDigest(payload),
	}
	encoded, err := encodeBlueprintReleaseHookSnapshot(execution)
	if err != nil {
		t.Fatal(err)
	}
	snapshotSeed, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: scriptRunnerSnapshotKey(snapshotID), Value: encoded,
	}})
	if err != nil || !snapshotSeed.Succeeded {
		t.Fatalf("seed snapshot = %#v, %v", snapshotSeed, err)
	}
	sources := ScriptExecutionSources{
		Revision: seed.Revision,
		Script: Versioned[ScriptRecord]{
			Record: ScriptRecord{ScriptSetGeneration: body.Source.ScriptSetGeneration},
		},
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{Revision: body.SourceModRevision},
	}
	return store, sources, execution, snapshotSeed.Revision
}
