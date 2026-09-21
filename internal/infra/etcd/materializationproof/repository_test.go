package materializationproof

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	coreproof "github.com/AlanD20/groundplane/internal/core/materializationproof"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the storage codec accepts only the exact canonical frame.
func TestCodecRoundTripAndCorruption(t *testing.T) {
	t.Parallel()
	proof := repositoryTestProof(t, 7, 1)
	encoded, err := encodeProof(proof)
	if err != nil {
		t.Fatalf("encodeProof: %v", err)
	}
	decoded, err := decodeProof(encoded)
	if err != nil || !reflect.DeepEqual(decoded.Record(), proof.Record()) {
		t.Fatalf("decodeProof = %#v, %v", decoded.Record(), err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := decodeProof(corrupt); !isInternal(err) {
		t.Fatalf("decodeProof(corrupt) error = %v, want internal", err)
	}
	payload, err := json.Marshal(proof.Record())
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	extended := append([]byte(`{"unknown":true,`), payload[1:]...)
	if _, err := decodeProof(framePayload(extended)); !isInternal(err) {
		t.Fatalf("decodeProof(extended) error = %v, want internal", err)
	}
}

// Rationale: a zero proof cannot cause any durable read or transaction.
func TestRepositoryRejectsZeroProofBeforeTransaction(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, err := newRepository(backend)
	if err != nil {
		t.Fatalf("newRepository: %v", err)
	}
	if _, err := repository.Publish(context.Background(), coreproof.Proof{}); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Publish error = %v, want validation", err)
	}
	if backend.transactions != 0 {
		t.Fatalf("transactions = %d, want zero", backend.transactions)
	}
}

// Rationale: publication decodes canonical applied and Task records and binds
// every claimed generation, Service, destination, and source membership.
func TestMaterializationRepositoryPublishesAgainstCanonicalSemanticAuthority(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, _ := newRepository(backend)
	proof := repositoryTestProof(t, 7, 1)
	seedSemanticAuthority(
		t,
		backend,
		proof,
		func(_ *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {},
	)
	published, err := repository.Publish(context.Background(), proof)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if published.ModRevision <= 0 || published.ReadRevision != published.ModRevision {
		t.Fatalf("published version = %#v", published)
	}
}

// Rationale: wrong applied generation, wrong owning Task semantics, and a
// mismatched materialization member must never be authorized by valid MVCC revisions.
func TestMaterializationRepositoryRejectsWrongGenerationTaskAndMembership(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testenvironmentprojection.EnvironmentComposeProjection, *base.TaskRecord)
	}{
		{
			name: "applied generation",
			mutate: func(projection *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {
				projection.RenderGeneration++
			},
		},
		{
			name: "owning Task revision",
			mutate: func(_ *testenvironmentprojection.EnvironmentComposeProjection, task *base.TaskRecord) {
				task.Params[testblueprints.EnvironmentDesiredRevisionParam] = testID(ids.KindTask, 90)
			},
		},
		{
			name: "missing desired Service",
			mutate: func(projection *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {
				projection.DesiredServices = nil
				artifact := new(agentpb.ComposeArtifact)
				if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
					panic(err)
				}
				artifact.Services = nil
				var err error
				projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
				if err != nil {
					panic(err)
				}
			},
		},
		{
			name: "Task membership destination",
			mutate: func(_ *testenvironmentprojection.EnvironmentComposeProjection, task *base.TaskRecord) {
				task.Materializations[0].Destination = "secrets/.env." + task.Target + ".other"
				task.Materializations[0].ServiceName = "other"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newMemoryStore()
			repository, _ := newRepository(backend)
			proof := repositoryTestProof(t, 8, 2)
			seedSemanticAuthority(t, backend, proof, test.mutate)
			if _, err := repository.Publish(context.Background(), proof); !isKind(err, errs.KindStateConflict) {
				t.Fatalf("Publish error = %v, want state conflict", err)
			}
		})
	}
}

// Rationale: two generated values may reuse one logical source only when they
// pin the exact same immutable Entry generation or Secret metadata revision.
func TestTaskSourcesRejectsConflictingLogicalSourceIdentity(t *testing.T) {
	t.Parallel()
	entryID := testID(ids.KindEnvEntry, 91)
	secretID := testID(ids.KindSecret, 92)
	tests := []struct {
		name   string
		values []testtaskmaterialization.GeneratedEnvironmentEntryReference
	}{
		{
			name: "Entry generation",
			values: []testtaskmaterialization.GeneratedEnvironmentEntryReference{
				{Name: "A", Value: testtaskmaterialization.EntryValueReference{
					EntryID: entryID, ValueGenerationID: testID(ids.KindConfig, 93),
					Storage: testtaskmaterialization.EntryValueStorageSecret,
				}},
				{Name: "B", Value: testtaskmaterialization.EntryValueReference{
					EntryID: entryID, ValueGenerationID: testID(ids.KindConfig, 94),
					Storage: testtaskmaterialization.EntryValueStorageSecret,
				}},
			},
		},
		{
			name: "Secret metadata",
			values: []testtaskmaterialization.GeneratedEnvironmentEntryReference{
				{Name: "A", Secret: &testtaskmaterialization.SecretValueReference{
					SecretID: secretID, Revision: 1, CiphertextSHA256: testDigest("first"),
				}},
				{Name: "B", Secret: &testtaskmaterialization.SecretValueReference{
					SecretID: secretID, Revision: 2, CiphertextSHA256: testDigest("second"),
				}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, valid := taskSources(testtaskmaterialization.Source{
				Kind: testtaskmaterialization.SourceGeneratedEnvironment,
				GeneratedEnvironment: &testtaskmaterialization.GeneratedEnvironmentValueReference{
					FormatVersion: 1,
					Values:        test.values,
				},
			})
			if valid {
				t.Fatal("taskSources accepted conflicting logical source identity")
			}
		})
	}
}

// Rationale: exact replay is immutable operation evidence and remains valid
// after mutable applied and Task authorities advance.
func TestRepositoryReplaysBeforeMutableAuthorityValidation(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, _ := newRepository(backend)
	proof := repositoryTestProof(t, 9, 3)
	seedSemanticAuthority(
		t,
		backend,
		proof,
		func(_ *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {},
	)
	first, err := repository.Publish(context.Background(), proof)
	if err != nil {
		t.Fatalf("Publish(first): %v", err)
	}
	backend.forcePut(
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(proof.EnvironmentID()),
		[]byte("advanced"),
	)
	backend.forcePut(testtaskjournal.TaskStorageKey(proof.ProducingTaskID()), []byte("advanced"))
	replay, err := repository.Publish(context.Background(), proof)
	if err != nil || replay.ModRevision != first.ModRevision || replay.ReadRevision <= first.ReadRevision {
		t.Fatalf("Publish(replay) = %#v, %v; first=%#v", replay, err, first)
	}
}

// Rationale: a concurrent exact publisher wins safely and is replayed even
// when mutable authority also advances before the losing transaction.
func TestRepositoryConcurrentExactPublicationReplaysWinner(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, _ := newRepository(backend)
	proof := repositoryTestProof(t, 10, 4)
	seedSemanticAuthority(
		t,
		backend,
		proof,
		func(_ *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {},
	)
	encoded, err := encodeProof(proof)
	if err != nil {
		t.Fatalf("encodeProof: %v", err)
	}
	backend.beforeTransact = func() {
		backend.forcePut(proofKey(proof.EnvironmentID(), proof.RenderGeneration()), encoded)
		backend.forcePut(testtaskjournal.TaskStorageKey(proof.ProducingTaskID()), []byte("advanced"))
	}
	replay, err := repository.Publish(context.Background(), proof)
	if err != nil || replay.ModRevision <= 0 {
		t.Fatalf("Publish(race) = %#v, %v", replay, err)
	}
}

// Rationale: mutable authority may advance, but immutable deletion fences
// still block exact replay and prevent resurrection.
func TestRepositoryReplayRetainsDeletionFenceRules(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, _ := newRepository(backend)
	proof := repositoryTestProof(t, 11, 5)
	seedSemanticAuthority(
		t,
		backend,
		proof,
		func(_ *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {},
	)
	if _, err := repository.Publish(context.Background(), proof); err != nil {
		t.Fatalf("Publish(first): %v", err)
	}
	backend.forcePut(
		proofGenerationDeletionFenceKey(proof.EnvironmentID(), proof.RenderGeneration()),
		[]byte("removed"),
	)
	if _, err := repository.Publish(context.Background(), proof); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("Publish(replay) error = %v, want state conflict", err)
	}
}

// Rationale: replay reconstruction remains pinned to the selected MVCC view.
func TestRepositoryLoadsOnlyTheExactFixedRevision(t *testing.T) {
	t.Parallel()
	backend := newMemoryStore()
	repository, _ := newRepository(backend)
	proof := repositoryTestProof(t, 12, 6)
	seedSemanticAuthority(
		t,
		backend,
		proof,
		func(_ *testenvironmentprojection.EnvironmentComposeProjection, _ *base.TaskRecord) {},
	)
	published, err := repository.Publish(context.Background(), proof)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	corruptRevision := backend.forcePut(proofKey(proof.EnvironmentID(), proof.RenderGeneration()), []byte("corrupt"))
	loaded, found, err := repository.LoadExact(
		context.Background(), proof.EnvironmentID(), proof.RenderGeneration(), published.ReadRevision,
	)
	if err != nil || !found || !reflect.DeepEqual(loaded.Proof.Record(), proof.Record()) {
		t.Fatalf("LoadExact(original) = %#v, %t, %v", loaded, found, err)
	}
	if _, _, err := repository.LoadExact(
		context.Background(), proof.EnvironmentID(), proof.RenderGeneration(), corruptRevision,
	); !isInternal(err) {
		t.Fatalf("LoadExact(corrupt) error = %v, want internal", err)
	}
}

func seedSemanticAuthority(
	t *testing.T,
	store *memoryStore,
	proof coreproof.Proof,
	mutate func(*testenvironmentprojection.EnvironmentComposeProjection, *base.TaskRecord),
) {
	t.Helper()
	record := proof.Record()
	member := record.Members[0]
	projection := testProjection(t, record, member)
	task := testTask(record, member)
	mutate(&projection, &task)
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("EncodeEnvironmentComposeProjectionStorage: %v", err)
	}
	taskValue, err := base.EncodeTaskRecord(task)
	if err != nil {
		t.Fatalf("EncodeTaskStorageRecord: %v", err)
	}
	store.forcePut(
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(record.EnvironmentID),
		projectionValue,
	)
	store.forcePut(testtaskjournal.TaskStorageKey(record.ProducingTaskID), taskValue)
}

func testProjection(
	t *testing.T,
	record coreproof.Record,
	member coreproof.MemberRecord,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	canonicalYAML := []byte("services: {}\n")
	digest := sha256.Sum256(canonicalYAML)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId:          testID(ids.KindConfig, 70),
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             record.EnvironmentID,
		ProjectName:         "groundplane-test",
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Services:            []*agentpb.ComposeService{{ServiceId: member.ServiceID, ComposeName: member.ServiceName}},
	})
	if err != nil {
		t.Fatalf("Marshal ComposeArtifact: %v", err)
	}
	return testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: record.EnvironmentID, RevisionID: record.AppliedRevisionID,
		RenderGeneration: record.RenderGeneration, ComposeArtifact: artifact,
		NormalizedCompose: canonicalYAML,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: record.EnvironmentID,
			Desired: core.Service{
				ID: member.ServiceID, Name: member.ServiceName, Image: "example/service:1",
				Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack,
			},
		}},
	}
}

func testTask(record coreproof.Record, member coreproof.MemberRecord) base.TaskRecord {
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	stepID := testID(ids.KindStep, 71)
	secret := member.ReusableSecrets[0]
	return base.TaskRecord{
		ID: record.ProducingTaskID, OperationID: testID(ids.KindOperation, 72),
		Owner: testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspaceTenant, TenantID: testID(ids.KindTenant, 73),
			ProjectID: testID(ids.KindProject, 74), EnvironmentID: record.EnvironmentID,
		},
		Actor: testtaskjournal.TaskActorOperator, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: testID(ids.KindPlan, 75), PlanHash: strings.Repeat("a", 64),
		RenderGeneration: int32(
			record.RenderGeneration,
		), Type: testtaskjournal.TaskUpdate, Target: record.EnvironmentID,
		Params: map[string]string{
			testtaskjournal.TaskMaterializationEnvironmentParam: record.EnvironmentID,
			testblueprints.EnvironmentDesiredRevisionParam:      record.AppliedRevisionID,
		},
		Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
		Materializations: []testtaskmaterialization.Record{{
			StepID: stepID, MaterializationID: member.MaterializationID,
			EnvironmentID: record.EnvironmentID, Destination: member.Destination,
			ServiceID: member.ServiceID, ServiceName: member.ServiceName,
			OutputKind: testtaskmaterialization.OutputGeneratedEnvironment,
			UID:        member.UID, GID: member.GID, Mode: member.Mode,
			Length: member.Length, SHA256: member.ContentSHA256,
			Source: testtaskmaterialization.Source{
				Kind: testtaskmaterialization.SourceGeneratedEnvironment,
				GeneratedEnvironment: &testtaskmaterialization.GeneratedEnvironmentValueReference{
					FormatVersion: 1,
					Values: []testtaskmaterialization.GeneratedEnvironmentEntryReference{{
						Name: "TOKEN",
						Secret: &testtaskmaterialization.SecretValueReference{
							SecretID: secret.SecretID, Revision: secret.MetadataRevision,
							CiphertextSHA256: secret.CiphertextSHA256,
						},
					}},
				},
			},
		}},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: now, UpdatedAt: now,
	}
}

func repositoryTestProof(t *testing.T, generation uint64, taskSeed int64) coreproof.Proof {
	t.Helper()
	environmentID := testID(ids.KindEnvironment, 1)
	serviceID := testID(ids.KindService, 4)
	proof, err := coreproof.New(coreproof.Input{
		EnvironmentID: environmentID, AppliedRevisionID: testID(ids.KindTask, 2),
		RenderGeneration: generation, ProducingTaskID: testID(ids.KindTask, taskSeed+10),
		Members: []coreproof.MemberRecord{{
			MaterializationID: testID(ids.KindConfig, 3), ServiceID: serviceID, ServiceName: "api",
			Destination: "secrets/.env." + environmentID + ".api",
			OutputKind:  coreproof.OutputGeneratedEnvironment, Outcome: coreproof.OutcomePresent,
			Mode: 0o600, Length: 3, ContentSHA256: testDigest("app"),
			AbsenceOrOwnershipSHA256: testDigest("owner"),
			ReusableSecrets: []coreproof.ReusableSecretRecord{{
				SecretID: testID(ids.KindSecret, 5), MetadataRevision: 19,
				CiphertextSHA256: testDigest("ciphertext"),
			}},
		}},
	})
	if err != nil {
		t.Fatalf("coreproof.New: %v", err)
	}
	return proof
}

func framePayload(payload []byte) []byte {
	framed := make([]byte, codecHeaderBytes+len(payload))
	copy(framed, codecMagic)
	binary.BigEndian.PutUint32(framed[len(codecMagic):], uint32(len(payload)))
	digest := sha256.Sum256(payload)
	copy(framed[len(codecMagic)+4:codecHeaderBytes], digest[:])
	copy(framed[codecHeaderBytes:], payload)
	return framed
}

func testID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC), seed)
}

func testDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func isInternal(err error) bool { return isKind(err, errs.KindInternal) }

func isKind(err error, want errs.Kind) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == want
}

type memoryStore struct {
	revision       int64
	transactions   int
	history        map[string][]memoryValue
	beforeTransact func()
}

type memoryValue struct {
	revision int64
	value    []byte
	deleted  bool
}

func newMemoryStore() *memoryStore { return &memoryStore{history: make(map[string][]memoryValue)} }

func (store *memoryStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	result := &testkeyvalue.GetManyResult{
		Values: make(
			[]*testkeyvalue.KeyValue,
			len(request.Keys),
		), ReadRevision: revision, ResponseRevision: store.revision,
	}
	for index, key := range request.Keys {
		if value, found := store.at(key, revision); found {
			result.Values[index] = &testkeyvalue.KeyValue{
				Key: key, Value: append([]byte(nil), value.value...), Version: 1, ModRevision: value.revision,
			}
		}
	}
	return result, nil
}

func (store *memoryStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.transactions++
	if store.beforeTransact != nil {
		hook := store.beforeTransact
		store.beforeTransact = nil
		hook()
	}
	failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
	matched := true
	for index, condition := range conditions {
		value, found := store.at(condition.Key, store.revision)
		actual := int64(0)
		if found {
			actual = value.revision
			failureReads[index] = &testkeyvalue.KeyValue{
				Key: condition.Key, Value: append([]byte(nil), value.value...), Version: 1, ModRevision: value.revision,
			}
		}
		if condition.Prefix || actual != condition.ModRevision {
			matched = false
		}
	}
	if !matched {
		return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
	}
	store.revision++
	for _, mutation := range mutations {
		value := memoryValue{revision: store.revision, deleted: mutation.Type == testkeyvalue.MutationDelete}
		if mutation.Type == testkeyvalue.MutationPut {
			value.value = append([]byte(nil), mutation.Value...)
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], value)
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryStore) forcePut(key string, value []byte) int64 {
	store.revision++
	store.history[key] = append(store.history[key], memoryValue{
		revision: store.revision, value: append([]byte(nil), value...),
	})
	return store.revision
}

func (store *memoryStore) at(key string, revision int64) (memoryValue, bool) {
	values := store.history[key]
	for index := len(values) - 1; index >= 0; index-- {
		if values[index].revision <= revision {
			return values[index], !values[index].deleted
		}
	}
	return memoryValue{}, false
}
