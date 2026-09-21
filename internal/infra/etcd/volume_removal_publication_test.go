package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the exact Task accepted by removal publication must also expose
// the standard Environment and resource authority used by the desired
// publisher and materialization-writer admission.
func TestVolumeRemovalInitialTaskDeclaresMaterializationAuthority(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer testkeyvalue.ClearMutationValues(publication.mutations)
	environmentID, declared, err := taskMaterializationEnvironment(task)
	if err != nil || !declared || environmentID != task.Owner.EnvironmentID ||
		task.Params[testtaskjournal.TaskResourceKindParam] != testtaskjournal.TaskResourceVolume {
		t.Fatalf(
			"published removal Task has no standard materialization authority: declared=%t error=%v",
			declared,
			err,
		)
	}
}

// Rationale: a retained snapshot must not authorize new Script mounts once
// removal publication owns the Volume, including before a Script Task exists.
func TestVolumeRemovalInitialOwnershipExcludesScriptPreparation(t *testing.T) {
	for _, removing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unowned control", true: "removal owns Volume"}[removing], func(t *testing.T) {
			ctx := context.Background()
			store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
			member := members[2]
			if member.Reference.Source.Kind != testscriptsourcereference.SourceVolume {
				t.Fatal("fixture has no Volume")
			}
			if removing {
				initial, task, marker := volumeRemovalPublicationFixture(t)
				runtime, _, _, err := initial.Records()
				if err != nil {
					t.Fatal(err)
				}
				runtime.EnvironmentID = member.Reference.SourceOwnerID
				runtime.VolumeID = member.Reference.Source.VolumeID
				runtime.RootLocator.ScopeID = runtime.EnvironmentID
				task.Owner.EnvironmentID, task.Target = runtime.EnvironmentID, runtime.VolumeID
				task.Params = EnvironmentVolumeRemovalTaskParams(runtime, 1)
				marker.Locator.ScopeID, marker.ReplayTarget.ID = runtime.EnvironmentID, runtime.VolumeID
				initial, err = removalrecord.PrepareInitialPublication(runtime)
				if err != nil {
					t.Fatal(err)
				}
				publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
				if err != nil {
					t.Fatal(err)
				}
				defer testkeyvalue.ClearMutationValues(publication.mutations)
				result, err := store.Transact(ctx, publication.conditions, publication.mutations)
				if err != nil || !result.Succeeded {
					t.Fatalf("publish initial records: %v", err)
				}
			}
			authority, err := testscriptsourcepublication.NewAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			_, err = authority.Prepare(
				ctx,
				operationID,
				[]testscriptsourceevidence.ScriptSourcePreparationMember{member},
			)
			if !removing {
				if err != nil {
					t.Fatalf("unowned source rejected: %v", err)
				}
				return
			}
			if !isKind(err, errs.KindStateConflict) {
				t.Fatalf("Script reserved a Volume already owned by removal: %v", err)
			}
			if count := store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), store.revision); count != nil {
				t.Fatal("rejected acquisition installed a source count")
			}
		})
	}
}

type volumeSourceOwnerRaceStore struct {
	hierarchyStore
	owner    removalrecord.Owner
	injected bool
}

func (store *volumeSourceOwnerRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if !store.injected &&
			mutation.Key == testscriptsourceevidence.ScriptSourceCountKey(volumeScriptSource(store.owner.VolumeID)) {
			value, err := removalrecord.EncodeOwner(store.owner)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			defer clear(value)
			guards := append(
				volumeScriptAbsenceConditions(
					store.owner.VolumeID,
				),
				testkeyvalue.Condition{Key: removalrecord.OwnerKey(store.owner.VolumeID)},
			)
			result, err := store.hierarchyStore.Transact(
				ctx,
				guards,
				[]testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationPut, Key: removalrecord.OwnerKey(store.owner.VolumeID), Value: value},
				},
			)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			if !result.Succeeded {
				return testkeyvalue.TransactionResult{}, errs.New(
					errs.KindInternal,
					"fixture owner race did not commit",
				)
			}
			store.injected = true
			break
		}
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

// Rationale: an owner acquired after the source read must still defeat the
// source-count transaction. Testing only a pre-existing owner misses this race.
func TestVolumeRemovalInitialOwnershipWinsSourceAcquisitionRace(t *testing.T) {
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	member := members[2]
	transactions := &volumeSourceOwnerRaceStore{hierarchyStore: store, owner: removalrecord.Owner{
		VolumeID: member.Reference.Source.VolumeID, EnvironmentID: member.Reference.SourceOwnerID,
		OperationID: ids.New(ids.KindOperation),
	}}
	authority, err := testscriptsourcepublication.NewAuthority(transactions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{member},
	)
	if !transactions.injected || !isKind(err, errs.KindStateConflict) {
		t.Fatalf("late owner did not exclude source acquisition: %v", err)
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), store.revision) != nil ||
		store.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			store.revision,
		) != nil {
		t.Fatal("losing source acquisition installed a count or membership")
	}
}

// Rationale: the owner lookup is a bounded, typed operation reference, not an
// unvalidated alternate runtime record or caller-controlled path.
func TestVolumeRemovalInitialOwnerCodecRejectsCorruption(t *testing.T) {
	_, task, _ := volumeRemovalPublicationFixture(t)
	owner := removalrecord.Owner{
		VolumeID:      task.Target,
		EnvironmentID: task.Owner.EnvironmentID,
		OperationID:   task.OperationID,
	}
	value, err := removalrecord.EncodeOwner(owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) > 1024 || len(removalrecord.OwnerKey(owner.VolumeID)) > 512 {
		t.Fatal("owner record or key exceeds its bound")
	}
	decoded, err := removalrecord.DecodeOwner(value)
	if err != nil || decoded != owner {
		t.Fatalf("owner round trip: %v", err)
	}
	for _, invalid := range [][]byte{nil, value[:len(value)-1], append(append([]byte(nil), value...), 0), make([]byte, 1025)} {
		if _, err := removalrecord.DecodeOwner(invalid); !isKind(err, errs.KindInternal) {
			t.Fatalf("malformed owner accepted: %v", err)
		}
	}
	for _, invalid := range []removalrecord.Owner{
		{EnvironmentID: owner.EnvironmentID, OperationID: owner.OperationID},
		{VolumeID: owner.VolumeID, OperationID: owner.OperationID},
		{VolumeID: owner.VolumeID, EnvironmentID: owner.EnvironmentID},
	} {
		if _, err := removalrecord.EncodeOwner(invalid); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("invalid owner identity accepted: %v", err)
		}
	}
}

// Rationale: the publisher consumes the existing runtime record contract and
// a bounded create-only fragment, not a caller-authored set of store writes.
func TestVolumeRemovalInitialPublicationFragment(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
	if err != nil {
		t.Fatalf("bind initial publication: %v", err)
	}
	defer testkeyvalue.ClearMutationValues(publication.mutations)
	if len(publication.conditions) != 2 || len(publication.mutations) != 4 {
		t.Fatalf(
			"unexpected initial fragment: %d compares, %d mutations",
			len(publication.conditions),
			len(publication.mutations),
		)
	}
	wantKeys := []string{
		removalrecord.RuntimeKey(task.OperationID), removalrecord.AttemptKey(task.OperationID, 1),
		removalrecord.ProgressKey(task.OperationID), removalrecord.OwnerKey(task.Target),
		removalrecord.PendingPathKey(task.OperationID),
	}
	if publication.conditions[0] != (testkeyvalue.Condition{Key: removalrecord.Root(task.OperationID), Prefix: true}) {
		t.Fatal("initial fragment does not require complete operation absence")
	}
	if publication.conditions[1] != (testkeyvalue.Condition{Key: removalrecord.OwnerKey(task.Target)}) {
		t.Fatal("initial fragment does not exclude a different removal owner")
	}
	owner, err := removalrecord.DecodeOwner(publication.mutations[3].Value)
	if err != nil || owner.VolumeID != task.Target || owner.EnvironmentID != task.Owner.EnvironmentID ||
		owner.OperationID != task.OperationID {
		t.Fatalf("initial ownership encoding: %v", err)
	}
	runtime, err := removalrecord.DecodeRuntime(publication.mutations[0].Value)
	if err != nil || runtime.CurrentTaskID != task.ID || runtime.Checkpoint != removalrecord.DesiredPublished {
		t.Fatalf("initial runtime encoding: %v", err)
	}
	attempt, err := removalrecord.DecodeAttempt(publication.mutations[1].Value)
	if err != nil || attempt.TaskID != task.ID || attempt.Ordinal != 1 {
		t.Fatalf("initial attempt encoding: %v", err)
	}
	progress, err := removalrecord.DecodeProgress(publication.mutations[2].Value)
	if err != nil || progress.OperationID != task.OperationID || progress.NextRequestOrdinal != 1 {
		t.Fatalf("initial progress encoding: %v", err)
	}
	for index, mutation := range publication.mutations {
		if mutation.Key != wantKeys[index] || mutation.Type != testkeyvalue.MutationPut || mutation.Prefix {
			t.Fatal("initial record fragment changed key or effect")
		}
	}
	sizer := &store{root: "/groundplane"}
	bytes, err := sizer.transactionSize(publication.conditions, publication.mutations)
	if err != nil || bytes > 8*1024 {
		t.Fatalf("initial fragment exceeds 8 KiB: bytes=%d error=%v", bytes, err)
	}
	t.Logf(
		"initial runtime fragment: %d compares, %d mutations, %d protobuf bytes",
		len(publication.conditions),
		len(publication.mutations),
		bytes,
	)
	empty := newMemoryTaskStore()
	accepted, err := empty.Transact(context.Background(), publication.conditions, publication.mutations)
	if err != nil || !accepted.Succeeded {
		t.Fatalf("empty operation publication control: %v", err)
	}
	for _, occupiedKey := range wantKeys {
		t.Run(occupiedKey, func(t *testing.T) {
			backend := newMemoryTaskStore()
			if _, err := backend.Transact(context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: occupiedKey, Value: []byte("occupied")}}); err != nil {
				t.Fatal(err)
			}
			before := backend.revision
			result, err := backend.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil || result.Succeeded || backend.revision != before {
				t.Fatalf("pre-existing removal state did not exclude publication: %v", err)
			}
		})
	}
}

// Rationale: a valid removal record set cannot authorize a different Task,
// protected intent, original response, or replay target at publication time.
func TestVolumeRemovalInitialPublicationRejectsChangedBinding(t *testing.T) {
	for name, change := range map[string]func(*TaskRecord, *testidempotency.IdempotencyMarker){
		"task id": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, 100)
		},
		"operation": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, 100)
		},
		"environment": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, task.CreatedAt, 100)
		},
		"volume": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.Target = ids.NewAt(ids.KindVolume, task.CreatedAt, 100)
		},
		"timeout":    func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) { task.TimeoutSeconds = 120 },
		"generation": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) { task.RenderGeneration++ },
		"step": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.Steps[0].ID = ids.NewAt(ids.KindStep, task.CreatedAt, 100)
		},
		"task time": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.CreatedAt = task.CreatedAt.Add(-time.Second)
		},
		"task kind": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) { task.Type = testtaskjournal.TaskUpdate },
		"wrong intent": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Intent.Ciphertext = []byte("another-protected-intent")
			digest := sha256.Sum256(marker.Intent.Ciphertext)
			marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
		},
		"wrong key": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Locator.Key = strings.Repeat("b", 128)
		},
		"wrong route":    func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) { marker.Locator.Route = "/tasks/{id}" },
		"missing target": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) { marker.ReplayTarget = nil },
		"wrong target": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.ReplayTarget.ID = ids.NewAt(ids.KindVolume, marker.CreatedAt, 100)
		},
		"response status": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) { marker.Response.Status = 200 },
		"response shape": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Response.Body = []byte(`{"task_id":"` + marker.TaskID + `","extra":true}`)
		},
		"marker time": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.UpdatedAt = marker.CreatedAt.Add(time.Second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			initial, task, marker := volumeRemovalPublicationFixture(t)
			change(&task, &marker)
			publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
			if err == nil || len(publication.conditions) != 0 || len(publication.mutations) != 0 {
				t.Fatalf("changed binding yielded a usable publication: %v", err)
			}
		})
	}
	_, task, marker := volumeRemovalPublicationFixture(t)
	if _, err := prepareVolumeRemovalInitialPublication(removalrecord.InitialPublication{}, task, marker); err == nil {
		t.Fatal("zero initial preparation accepted")
	}
}

// Rationale: valid Task presentation fields cannot substitute for the immutable
// inputs consumed on assignment. Every removal parameter is exact and closed.
func TestVolumeRemovalInitialPublicationRejectsChangedParameters(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	for key := range task.Params {
		t.Run(key, func(t *testing.T) {
			initial, task, marker := volumeRemovalPublicationFixture(t)
			task.Params[key] = "changed"
			if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
				t.Fatal("changed removal parameter accepted")
			}
			delete(task.Params, key)
			if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
				t.Fatal("missing removal parameter accepted")
			}
		})
	}
	task.Params["extra"] = "unbound"
	if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
		t.Fatal("extra removal parameter accepted")
	}
}

// Rationale: the root response is exactly the original Task acknowledgement,
// even if a caller consistently hashes a different compact JSON shape.
func TestVolumeRemovalInitialPublicationRejectsNonCanonicalRootResponse(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	runtime, _, _, err := initial.Records()
	if err != nil {
		t.Fatal(err)
	}
	marker.Response.Body = []byte(`{"task_id":"` + task.ID + `","extra":true}`)
	runtime.RootResponseSHA256 = sha256.Sum256(marker.Response.Body)
	initial, err = removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
		t.Fatal("non-canonical root response accepted with a matching digest")
	}
}

// Rationale: a reused operation id with only a completion or successor record
// is still occupied. Checking just the three initial records misses this state.
func TestVolumeRemovalInitialPublicationRejectsOrphanedOperationRecords(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer testkeyvalue.ClearMutationValues(publication.mutations)
	for _, key := range []string{
		removalrecord.CompletionKey(task.OperationID, 1), removalrecord.AttemptKey(task.OperationID, 2),
	} {
		t.Run(key, func(t *testing.T) {
			backend := newMemoryTaskStore()
			if _, err := backend.Transact(context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: []byte("occupied")}}); err != nil {
				t.Fatal(err)
			}
			before := backend.revision
			result, err := backend.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil || result.Succeeded || backend.revision != before {
				t.Fatalf("orphaned operation record did not exclude publication: %v", err)
			}
		})
	}
}

func volumeRemovalPublicationFixture(
	t *testing.T,
) (removalrecord.InitialPublication, TaskRecord, testidempotency.IdempotencyMarker) {
	t.Helper()
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Owner = testtaskjournal.TaskOwner{WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID: ids.NewAt(ids.KindTenant, now, 20), ProjectID: ids.NewAt(ids.KindProject, now, 21),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 22)}
	task.Type = testtaskjournal.TaskRemove
	task.Target = ids.NewAt(ids.KindVolume, now, 23)
	task.TimeoutSeconds = removalrecord.TimeoutSeconds
	task.IdempotencyKey = strings.Repeat("a", 128)
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = task.Owner.EnvironmentID
	marker.Locator.Method = "DELETE"
	marker.Locator.Route = "/volumes/{id}"
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetVolume,
		ID:   task.Target,
	}
	runtime := removalrecord.Runtime{
		OperationID: task.OperationID, EnvironmentID: task.Owner.EnvironmentID, VolumeID: task.Target,
		Key: strings.Repeat("a", 255), DesiredRevisionID: task.ID, DesiredGeneration: uint64(task.RenderGeneration),
		ImpactSHA256: sha256.Sum256([]byte("impact")), EvidenceManifestSHA256: sha256.Sum256([]byte("sealed manifest")),
		IntentSHA256: sha256.Sum256(marker.Intent.Ciphertext), RootResponseSHA256: sha256.Sum256(marker.Response.Body),
		RootLocator: removalrecord.ReplayLocator{
			ScopeKind: string(marker.Locator.ScopeKind),
			ScopeID:   marker.Locator.ScopeID,
			Method:    marker.Locator.Method,
			Route:     marker.Locator.Route,
			Key:       marker.Locator.Key,
		},
		OriginTaskID: task.ID, CurrentTaskID: task.ID, AttemptOrdinal: 1, StepID: task.Steps[0].ID,
		Checkpoint: removalrecord.DesiredPublished, CreatedAt: now, UpdatedAt: now,
	}
	task.Params = EnvironmentVolumeRemovalTaskParams(runtime, 1)
	initial, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return initial, task, marker
}
