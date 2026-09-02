package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseGroupCountingStore struct {
	*memoryHierarchyStore
	maxOperations int
}

func (store *releaseGroupCountingStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	operations := len(conditions) + len(mutations)
	if operations > store.maxOperations {
		store.maxOperations = operations
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type releaseGroupPublisherFixture struct {
	store       *releaseGroupCountingStore
	tasks       *TaskRepository
	project     Versioned[ProjectRecord]
	environment Versioned[EnvironmentRecord]
	group       domain.Group
	revision    int64
	serviceIDs  []string
	now         time.Time
}

// Rationale: the legal 32-member removal must stay at its reviewed real Task/idempotency envelope and below the hard transaction ceiling.
func TestReleaseGroupMaximumRemovalFitsRealTaskPublicationEnvelope(t *testing.T) {
	t.Parallel()
	fixture := newReleaseGroupPublisherFixture(t, domain.MaximumMembers)
	prepared, err := fixture.tasks.PrepareReleaseGroupRemove(
		context.Background(),
		fixture.group,
		fixture.revision,
	)
	if err != nil {
		t.Fatalf("PrepareReleaseGroupRemove() error = %v", err)
	}
	unrelatedServiceID := ids.NewAt(ids.KindService, fixture.now, 9900)
	mustReleaseGroupTransaction(t, fixture.store, []Mutation{{
		Type:  MutationPut,
		Key:   deletionTombstoneKey("service", unrelatedServiceID),
		Value: []byte(`{"phase":"requested"}`),
	}})
	fixture.store.maxOperations = 0
	task, marker := releaseGroupRemovalTaskAndMarker(
		t,
		fixture.project.Record,
		fixture.environment.Record,
		fixture.group.ID,
		fixture.now,
	)
	result, err := fixture.tasks.PublishReleaseGroupMutation(
		context.Background(),
		fixture.environment,
		fixture.project,
		prepared,
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("PublishReleaseGroupMutation() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	const expectedOperations = 67
	if fixture.store.maxOperations != expectedOperations {
		t.Fatalf(
			"real maximum-member removal envelope = %d operations, want exactly %d",
			fixture.store.maxOperations,
			expectedOperations,
		)
	}
	if fixture.store.maxOperations > maximumTransactionOperations {
		t.Fatalf(
			"real maximum-member removal envelope = %d operations, want <= %d",
			fixture.store.maxOperations,
			maximumTransactionOperations,
		)
	}
}

func TestReleaseGroupRemovalFencesExactSelectedServiceDeletion(t *testing.T) {
	t.Parallel()
	fixture := newReleaseGroupPublisherFixture(t, domain.MaximumMembers)
	prepared, err := fixture.tasks.PrepareReleaseGroupRemove(
		context.Background(),
		fixture.group,
		fixture.revision,
	)
	if err != nil {
		t.Fatalf("PrepareReleaseGroupRemove() error = %v", err)
	}
	mustReleaseGroupTransaction(t, fixture.store, []Mutation{{
		Type:  MutationPut,
		Key:   deletionTombstoneKey("service", fixture.serviceIDs[len(fixture.serviceIDs)-1]),
		Value: []byte(`{"phase":"requested"}`),
	}})
	task, marker := releaseGroupRemovalTaskAndMarker(
		t,
		fixture.project.Record,
		fixture.environment.Record,
		fixture.group.ID,
		fixture.now,
	)
	result, err := fixture.tasks.PublishReleaseGroupMutation(
		context.Background(),
		fixture.environment,
		fixture.project,
		prepared,
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("PublishReleaseGroupMutation() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict ||
		!isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("selected deletion race = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if value := fixture.store.valueAt(taskKey(task.ID), fixture.store.revision); value != nil {
		t.Fatalf("failed selected-member publication wrote Task %#v", value)
	}
}

func TestReleaseGroupBlueprintCollectionCASRejectsConcurrentInsertion(t *testing.T) {
	t.Parallel()
	fixture := newReleaseGroupPublisherFixture(t, 2)
	repository, err := newHierarchyRepository(fixture.store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	readRevision := fixture.store.revision
	prepared, err := repository.PrepareReleaseGroupBlueprintMutation(
		context.Background(),
		fixture.environment.Record.ID,
		readRevision,
		[]ReleaseGroupSnapshotEntry{{
			Group:        fixture.group,
			Revision:     fixture.revision,
			ReadRevision: readRevision,
		}},
		nil,
	)
	if err != nil {
		t.Fatalf("PrepareReleaseGroupBlueprintMutation() error = %v", err)
	}
	concurrent, err := domain.New(domain.Input{
		ID:            ids.NewAt(ids.KindReleaseGroup, fixture.now, 9800),
		EnvironmentID: fixture.environment.Record.ID,
		Name:          "concurrent",
		ServiceIDs:    fixture.serviceIDs,
	})
	if err != nil {
		t.Fatalf("domain.New(concurrent) error = %v", err)
	}
	concurrentValue, err := encodeReleaseGroup(concurrent)
	if err != nil {
		t.Fatalf("encodeReleaseGroup(concurrent) error = %v", err)
	}
	collectionValue, err := encodeReleaseGroupCollectionEpoch(
		fixture.environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("encodeReleaseGroupCollectionEpoch() error = %v", err)
	}
	mustReleaseGroupTransaction(t, fixture.store, []Mutation{
		{Type: MutationPut, Key: releaseGroupRecordKey(concurrent.ID), Value: concurrentValue},
		{Type: MutationPut, Key: releaseGroupOwnerKey(concurrent.EnvironmentID, concurrent.ID), Value: []byte(concurrent.ID)},
		{Type: MutationPut, Key: releaseGroupNameKey(concurrent.EnvironmentID, concurrent.Name), Value: []byte(concurrent.ID)},
		{Type: MutationPut, Key: releaseGroupCollectionEpochKey(concurrent.EnvironmentID), Value: collectionValue},
	})
	result, err := fixture.store.Transact(
		context.Background(),
		prepared.conditions,
		prepared.mutations,
	)
	if err != nil {
		t.Fatalf("Blueprint collection transaction error = %v", err)
	}
	if result.Succeeded {
		t.Fatal("Blueprint omission retained a concurrent insertion by committing")
	}
	for _, groupID := range []string{fixture.group.ID, concurrent.ID} {
		if value := fixture.store.valueAt(
			releaseGroupRecordKey(groupID),
			fixture.store.revision,
		); value == nil {
			t.Fatalf("collection race removed live group %s", groupID)
		}
	}
}

func TestReleaseGroupBlueprintOmissionDeletesOnlyLiveProjection(t *testing.T) {
	t.Parallel()
	fixture := newReleaseGroupPublisherFixture(t, 2)
	repository, err := newHierarchyRepository(fixture.store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	historicalKey := "/v1/records/releases/" + ids.NewAt(ids.KindDeployment, fixture.now, 9700)
	mustReleaseGroupTransaction(t, fixture.store, []Mutation{{
		Type:  MutationPut,
		Key:   historicalKey,
		Value: []byte("historical-release"),
	}})
	readRevision := fixture.store.revision
	groupValue := fixture.store.valueAt(
		releaseGroupRecordKey(fixture.group.ID),
		readRevision,
	)
	prepared, err := repository.PrepareReleaseGroupBlueprintMutation(
		context.Background(),
		fixture.environment.Record.ID,
		readRevision,
		[]ReleaseGroupSnapshotEntry{{
			Group:        fixture.group,
			Revision:     groupValue.ModRevision,
			ReadRevision: readRevision,
		}},
		nil,
	)
	if err != nil {
		t.Fatalf("PrepareReleaseGroupBlueprintMutation() error = %v", err)
	}
	result, err := fixture.store.Transact(
		context.Background(),
		prepared.conditions,
		prepared.mutations,
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("Blueprint omission transaction = %#v, %v", result, err)
	}
	if value := fixture.store.valueAt(
		releaseGroupRecordKey(fixture.group.ID),
		fixture.store.revision,
	); value != nil {
		t.Fatalf("omitted live group remains %#v", value)
	}
	if value := fixture.store.valueAt(historicalKey, fixture.store.revision); value == nil {
		t.Fatal("Blueprint omission deleted historical Release data")
	}
}

func newReleaseGroupPublisherFixture(
	t *testing.T,
	memberCount int,
) releaseGroupPublisherFixture {
	t.Helper()
	store := &releaseGroupCountingStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	now := time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC)
	serviceIDs := make([]string, memberCount)
	artifactServices := make([]*agentpb.ComposeService, memberCount)
	mutations := make([]Mutation, 0, memberCount+5)
	desiredServices := make([]EnvironmentServiceProjection, memberCount)
	for index := range serviceIDs {
		serviceID := ids.NewAt(ids.KindService, now, int64(100+index))
		serviceIDs[index] = serviceID
		name := "service-" + leftPadReleaseGroupIndex(index)
		artifactServices[index] = &agentpb.ComposeService{
			ServiceId:   serviceID,
			ComposeName: name,
		}
		desiredServices[index] = EnvironmentServiceProjection{
			EnvironmentID: environment.Record.ID,
			Desired:       core.Service{ID: serviceID, Name: name, Image: "example.invalid/" + name + ":1"},
		}
	}
	canonicalYAML := []byte("services: {}\\n")
	digest := sha256.Sum256(canonicalYAML)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:          ids.NewAt(ids.KindConfig, now, 700),
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environment.Record.ID,
		ProjectName:         "groundplane-release-group-test",
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: environment.Record.VolumeDir,
		Services:            artifactServices,
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal artifact error = %v", err)
	}
	projection := EnvironmentComposeProjection{
		EnvironmentID:     environment.Record.ID,
		RevisionID:        ids.NewAt(ids.KindTask, now, 701),
		RenderGeneration:  1,
		DesiredServices:   desiredServices,
		ComposeArtifact:   artifactValue,
		NormalizedCompose: []byte("services: {}\n"),
	}
	projectionValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	group, err := domain.New(domain.Input{
		ID:            ids.NewAt(ids.KindReleaseGroup, now, 800),
		EnvironmentID: environment.Record.ID,
		Name:          "maximum",
		ServiceIDs:    serviceIDs,
	})
	if err != nil {
		t.Fatalf("domain.New() error = %v", err)
	}
	groupValue, err := encodeReleaseGroup(group)
	if err != nil {
		t.Fatalf("encodeReleaseGroup() error = %v", err)
	}
	collectionValue, err := encodeReleaseGroupCollectionEpoch(environment.Record.ID)
	if err != nil {
		t.Fatalf("encodeReleaseGroupCollectionEpoch() error = %v", err)
	}
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: environmentComposeProjectionKey(environment.Record.ID), Value: projectionValue},
		Mutation{Type: MutationPut, Key: releaseGroupRecordKey(group.ID), Value: groupValue},
		Mutation{Type: MutationPut, Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), Value: []byte(group.ID)},
		Mutation{Type: MutationPut, Key: releaseGroupNameKey(group.EnvironmentID, group.Name), Value: []byte(group.ID)},
		Mutation{Type: MutationPut, Key: releaseGroupCollectionEpochKey(group.EnvironmentID), Value: collectionValue},
	)
	mustReleaseGroupTransaction(t, store, mutations)
	groupRecord := store.valueAt(releaseGroupRecordKey(group.ID), store.revision)
	return releaseGroupPublisherFixture{
		store:       store,
		tasks:       &TaskRepository{store: store},
		project:     project,
		environment: environment,
		group:       group,
		revision:    groupRecord.ModRevision,
		serviceIDs:  serviceIDs,
		now:         now,
	}
}

func releaseGroupRemovalTaskAndMarker(
	t *testing.T,
	project ProjectRecord,
	environment EnvironmentRecord,
	groupID string,
	now time.Time,
) (TaskRecord, IdempotencyMarker) {
	t.Helper()
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := TaskRecord{
		ID:                ids.NewAt(ids.KindTask, now, 900),
		OperationID:       ids.NewAt(ids.KindOperation, now, 901),
		IdempotencyKey:    ids.NewAt(ids.KindOperation, now, 902)[3:],
		Owner:             owner,
		Actor:             TaskActorOperator,
		Executor:          TaskExecutorController,
		PlanID:            ids.NewAt(ids.KindPlan, now, 903),
		PlanHash:          strings.Repeat("a", 64),
		RenderGeneration:  1,
		Type:              TaskRemove,
		Target:            groupID,
		Params:            map[string]string{TaskResourceKindParam: TaskResourceReleaseGroup},
		Steps:             []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 904)}},
		TimeoutSeconds:    30,
		Status:            TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	ciphertext := []byte("protected-release-group-removal")
	digest := sha256.Sum256(ciphertext)
	target := IdempotencyReplayTarget{
		Kind: IdempotencyReplayTargetReleaseGroup,
		ID:   groupID,
	}
	marker := IdempotencyMarker{
		Kind:  IdempotencyMarkerTask,
		State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment,
			ScopeID:   environment.ID,
			Method:    http.MethodDelete,
			Route:     "/release-groups/{id}",
			Key:       task.IdempotencyKey,
		},
		ReplayTarget: &target,
		Intent: ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        []byte(`{"task_id":"` + task.ID + `"}`),
		},
		TaskID:    task.ID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return task, marker
}

func mustReleaseGroupTransaction(
	t *testing.T,
	store *releaseGroupCountingStore,
	mutations []Mutation,
) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed transaction = %#v, %v", result, err)
	}
}

func leftPadReleaseGroupIndex(index int) string {
	if index < 10 {
		return "0" + string(rune('0'+index))
	}
	return string([]rune{rune('0' + index/10), rune('0' + index%10)})
}
