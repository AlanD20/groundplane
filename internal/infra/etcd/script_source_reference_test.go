package etcd

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	proto "google.golang.org/protobuf/proto"
	strings "strings"
	testing "testing"
	time "time"
)

func TestScriptSourceReferenceAuthorityPrepareSealAndActivate(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	prepared, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.IsZero() {
		t.Fatal("Prepare() returned a zero token")
	}
	descriptorValue := store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision)
	if descriptorValue == nil || descriptorValue.ModRevision <= 0 ||
		prepared.MembershipCount() != uint64(len(members)) {
		t.Fatalf("sealed descriptor = %#v, membership count = %d", descriptorValue, prepared.MembershipCount())
	}
	for _, member := range members {
		forward := store.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			store.revision,
		)
		reverse := store.valueAt(testscriptsourcereference.ReverseKey(member.Reference), store.revision)
		countValue := store.valueAt(
			testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source),
			store.revision,
		)
		if forward == nil || reverse == nil || countValue == nil || !bytes.Equal(forward.Value, reverse.Value) {
			t.Fatalf(
				"membership %s is incomplete",
				testscriptsourceevidence.ScriptSourceSuffix(member.Reference.Source),
			)
		}
		count, decodeErr := testscriptsourceevidence.DecodeScriptSourceCount(countValue.Value)
		if decodeErr != nil || count.ReferencedExecutionCount != 1 || count.Source != member.Reference.Source {
			t.Fatalf("source count = %#v, %v", count, decodeErr)
		}
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	defer fragment.Clear()
	conditions, mutations := fragment.Conditions(), fragment.Mutations()
	if len(conditions) != 2 || len(mutations) != 2 || len(fragment.StagedRequirements()) != 0 ||
		conditions[0] != (testkeyvalue.Condition{Key: testscriptsourcereference.PreparationKey(operationID), ModRevision: descriptorValue.ModRevision}) ||
		conditions[1] != (testkeyvalue.Condition{Key: testscriptsourceevidence.ScriptSourceRootKey(operationID)}) ||
		mutations[0].Key != testscriptsourceevidence.ScriptSourceRootKey(operationID) ||
		mutations[1].Type != testkeyvalue.MutationDelete || mutations[1].Key != testscriptsourcereference.PreparationKey(operationID) {
		t.Fatalf("final publication fragment = %#v / %#v", conditions, mutations)
	}
	result, err := store.Transact(context.Background(), conditions, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("activate prepared source set = %#v, %v", result, err)
	}
	rootValue := store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(operationID), store.revision)
	root, err := testscriptsourceevidence.DecodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.OperationID != operationID ||
		root.Phase != testscriptsourceevidence.ScriptOperationSourceActive ||
		root.MembershipCount != prepared.MembershipCount() ||
		root.MembershipSHA256 != prepared.MembershipSHA256() {
		t.Fatalf("active root = %#v, %v", root, err)
	}
}

func TestScriptSourceReferenceAuthorityRejectsConflictingOrForgedEvidence(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	conflict := members[0]
	conflict.Reference.SourceOwnerID = ids.NewAt(ids.KindEnvironment, scriptSourceReferenceTestTime(), 99)
	_, err := authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{members[0], conflict},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(conflicting owner) error = %v", err)
	}

	forged := members[0]
	forged.Evidence.Existing = &testscriptsourceevidence.ScriptExistingSourceEvidence{
		SourceKey: members[1].Evidence.Existing.SourceKey,
	}
	_, err = authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{forged},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(unrelated same-revision key) error = %v", err)
	}
	forged = members[0]
	forged.Reference.SourceDigest = scriptSourceReferenceDigest("wrong")
	_, err = authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{forged},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(mismatched digest) error = %v", err)
	}
	primary := testscripts.ScriptSetScriptKey(
		forged.Reference.Source.EnvironmentID,
		forged.Reference.Source.ScriptSetGeneration,
		forged.Reference.Source.ScriptID,
	)
	forged = members[0]
	forged.Evidence.Existing = &testscriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: primary}
	_, err = authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{forged},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(mutable Script primary) error = %v", err)
	}
	_ = store
}

func TestScriptSourceReferenceAuthorityBatchesAtMost16Memberships(t *testing.T) {
	base, operationID, members := scriptSourceServiceMembers(t, 17)
	store := &scriptSourceReferenceAuditStore{memoryHierarchyStore: base}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	nonempty := 0
	for _, batch := range store.batches {
		count := 0
		for _, mutation := range batch {
			if mutation.Type == testkeyvalue.MutationPut && strings.Contains(mutation.Key, "/executions/") {
				count++
			}
		}
		if count > 0 {
			nonempty++
		}
		if count > 16 {
			t.Fatalf("preparation batch memberships = %d", count)
		}
	}
	if nonempty < 2 {
		t.Fatalf("membership batches = %d, want multiple", nonempty)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer fragment.Clear()
	for _, mutation := range fragment.Mutations() {
		if strings.HasPrefix(mutation.Key, testscriptsourcereference.ForwardReferencePrefix) ||
			strings.HasPrefix(mutation.Key, testscriptsourcereference.CountPrefix) ||
			strings.Contains(mutation.Key, "/executions/") {
			t.Fatalf("activation mutates membership/count: %#v", mutation)
		}
	}
}

func TestScriptSourceReferenceAuthorityAbandonsPartialPreparationAfterRestart(t *testing.T) {
	base, operationID, members := scriptSourceServiceMembers(t, 12)
	failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: base, failAt: 3}
	authority, err := testscriptsourcepublication.NewAuthority(failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.Prepare(context.Background(), operationID, members); err == nil {
		t.Fatal("Prepare() error = nil, want injected later-batch failure")
	}
	descriptorValue := base.valueAt(testscriptsourcereference.PreparationKey(operationID), base.revision)
	preparedCount := 0
	for _, member := range members {
		if base.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			base.revision,
		) != nil {
			preparedCount++
		}
	}
	if descriptorValue == nil || preparedCount == 0 || preparedCount >= len(members) {
		t.Fatalf(
			"partial preparation = descriptor %#v, memberships %d/%d",
			descriptorValue,
			preparedCount,
			len(members),
		)
	}
	restarted, err := testscriptsourcepublication.NewAuthority(base)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := append([]testscriptsourceevidence.ScriptSourcePreparationMember(nil), members...)
	refreshed[0] = members[0]
	refreshed[0].Evidence.Existing = &testscriptsourceevidence.ScriptExistingSourceEvidence{
		SourceKey: members[1].Evidence.Existing.SourceKey,
	}
	if err = restarted.Abandon(context.Background(), operationID, refreshed); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Abandon(refreshed evidence) error = %v", err)
	}
	if err = restarted.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(restart) error = %v", err)
	}
	if err = restarted.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(exact retry) error = %v", err)
	}
	if base.valueAt(testscriptsourcereference.PreparationKey(operationID), base.revision) != nil {
		t.Fatal("abandonment retained the descriptor")
	}
	page, _ := base.Range(
		context.Background(),
		testkeyvalue.RangeRequest{
			Prefix: testscriptsourcereference.RootPrefix + operationID + "/executions/",
			Limit:  1,
		},
	)
	if len(page.Values) != 0 {
		t.Fatal("abandonment retained reverse memberships")
	}
	for _, member := range members {
		if base.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			base.revision,
		) != nil ||
			base.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), base.revision) != nil {
			t.Fatal("abandonment retained forward membership or count")
		}
	}
}

func TestScriptSourceReferenceAuthorityStagesSnapshotAndReleaseRequirements(t *testing.T) {
	store := newMemoryHierarchyStore()
	authority, _ := testscriptsourcepublication.NewAuthority(store)
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 70)
	executionID := scriptSourceReferenceExecutionID(at, 71)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 72)
	serviceID := ids.NewAt(ids.KindService, at, 73)
	snapshotID := scriptSourceReferenceExecutionID(at, 74)
	releaseID := ids.NewAt(ids.KindDeployment, at, 75)
	payload, _ := proto.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: environmentID,
	})
	snapshotDigest := scriptSourceReferenceBytesDigest(payload)
	snapshotValue, _ := testrecordcodec.Encode(
		"script-runner-snapshot", testscriptsourceevidence.StoredScriptRunnerSnapshot{
			ExecutionID: executionID,
			SnapshotID:  snapshotID,
			SHA256:      snapshotDigest,
			Payload:     payload,
		},
	)
	releaseValue, _ := testrecordcodec.Encode("release-intent", domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID, OperationID: operationID,
		OperationKind: domain.OperationDeploy, CandidateWorkload: releaseTestWorkloadSeal("registry.example/app:v1"), Tag: "v1",
		Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureLeaveActive,
		RenderInputID: ids.NewAt(ids.KindConfig, at, 77), RenderInputDigest: scriptSourceReferenceDigest("render"),
		CreatedAt: at, Actor: "test", OriginatingTaskID: ids.NewAt(ids.KindTask, at, 78),
		Workspace: domain.Workspace{
			Kind:          domain.WorkspaceTenant,
			TenantID:      ids.NewAt(ids.KindTenant, at, 79),
			ProjectID:     ids.NewAt(ids.KindProject, at, 80),
			EnvironmentID: environmentID,
		},
	})
	releaseDigest := scriptSourceReferenceBytesDigest(releaseValue)
	stage := scriptSourceReferenceStage(environmentID, ids.NewAt(ids.KindTask, at, 81), 9, 1, snapshotValue)
	releaseStage := stage
	releaseStage.CanonicalValueSHA256 = sha256.Sum256(releaseValue)
	members := []testscriptsourceevidence.ScriptSourcePreparationMember{
		{
			Reference: testscriptsourcereference.Reference{
				OperationID:       operationID,
				ScriptExecutionID: executionID,
				Source: testscriptsourcereference.SourceIdentity{
					Kind:       testscriptsourcereference.SourceRunnerSnapshot,
					SnapshotID: snapshotID,
				},
				SourceOwnerID: environmentID,
				SourceDigest:  snapshotDigest,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
					SourceKey: testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID),
					Stage:     stage,
					Value:     snapshotValue,
				},
			},
		},
		{
			Reference: testscriptsourcereference.Reference{
				OperationID:       operationID,
				ScriptExecutionID: executionID,
				Source: testscriptsourcereference.SourceIdentity{
					Kind:      testscriptsourcereference.SourceRelease,
					ReleaseID: releaseID,
				},
				SourceOwnerID: environmentID,
				SourceDigest:  releaseDigest,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
					SourceKey: testreleases.ReleaseIntentStagingKey("", releaseID),
					Stage:     releaseStage,
					Value:     releaseValue,
				},
			},
		},
	}
	prepared, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(staged) error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer fragment.Clear()
	if len(fragment.Conditions()) != 4 || len(fragment.Mutations()) != 4 ||
		len(fragment.StagedRequirements()) != 2 {
		t.Fatalf("staged fragment = %#v / %#v", fragment.Conditions(), fragment.StagedRequirements())
	}
	stagedMutations := []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID),
			Value: snapshotValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseIntentStagingKey("", releaseID), Value: releaseValue},
	}
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		EnvironmentID:    stage.EnvironmentID,
		RevisionID:       stage.RevisionID,
		RenderGeneration: stage.RenderGeneration,
	}
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); err != nil {
		t.Fatalf("ValidateStagedMutations(exact) error = %v", err)
	}
	stagedMutations[0].Value = []byte("changed")
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ValidateStagedMutations(changed) error = %v", err)
	}
	stagedMutations[0].Value = releaseValue
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ValidateStagedMutations(substituted canonical value) error = %v", err)
	}
	secretID := ids.NewAt(ids.KindSecret, at, 82)
	secret := testscriptsourceevidence.ScriptSourcePreparationMember{
		Reference: testscriptsourcereference.Reference{
			OperationID:       ids.NewAt(ids.KindOperation, at, 76),
			ScriptExecutionID: executionID,
			Source: testscriptsourcereference.SourceIdentity{
				Kind:              testscriptsourcereference.SourceSecretValue,
				SecretID:          secretID,
				ValueGenerationID: secretID,
			},
			SourceOwnerID: testscriptsourceevidence.ScriptSourcePlatformOwner,
			SourceDigest:  scriptSourceReferenceDigest("absent"),
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
				SourceKey: testsecrets.ValueKey(secretID),
				Stage:     stage,
				Value:     []byte("absent"),
			},
		},
	}
	if _, err = authority.Prepare(context.Background(), secret.Reference.OperationID, []testscriptsourceevidence.ScriptSourcePreparationMember{secret}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Prepare(non-Blueprint staged kind) error = %v", err)
	}
}

func TestScriptSourceReferenceAuthorityRejectsMixedOrInvalidStagedEvidence(t *testing.T) {
	_, authority, operationID, members := scriptSourceReferenceFixture(t)
	mixed := members[0]
	mixed.Evidence.Staged = &testscriptsourceevidence.ScriptStagedSourceEvidence{}
	if _, err := authority.Prepare(context.Background(), operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{mixed}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Prepare(mixed evidence) error = %v", err)
	}
	empty := members[0]
	empty.Evidence = testscriptsourceevidence.ScriptSourceEvidence{}
	if _, err := authority.Prepare(context.Background(), operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{empty}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Prepare(empty evidence) error = %v", err)
	}
	staged := members[0]
	staged.Reference.SourceModRevision = 0
	value := []byte("candidate")
	staged.Evidence = testscriptsourceevidence.ScriptSourceEvidence{
		Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
			SourceKey: staged.Evidence.Existing.SourceKey,
			Stage: scriptSourceReferenceStage(staged.Reference.SourceOwnerID,
				ids.NewAt(ids.KindTask, scriptSourceReferenceTestTime(), 83), 1, 0, value),
			Value: value,
		},
	}
	if _, err := authority.Prepare(context.Background(), operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{staged}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Prepare(zero fixed read revision) error = %v", err)
	}
	stagedBody := members[0]
	stagedBody.Reference.SourceModRevision = 0
	stagedBody.Evidence = testscriptsourceevidence.ScriptSourceEvidence{
		Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
			SourceKey: stagedBody.Evidence.Existing.SourceKey,
			Stage: scriptSourceReferenceStage(stagedBody.Reference.SourceOwnerID,
				ids.NewAt(ids.KindTask, scriptSourceReferenceTestTime(), 84), 1, 1, value),
			Value: value,
		},
	}
	if _, err := authority.Prepare(context.Background(), operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{stagedBody}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Prepare(staged Script body) error = %v", err)
	}
}

func TestScriptSourcePublicationRejectsCandidateOrMutationMismatch(t *testing.T) {
	store := newMemoryHierarchyStore()
	authority, _ := testscriptsourcepublication.NewAuthority(store)
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 90)
	executionID := scriptSourceReferenceExecutionID(at, 91)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 92)
	serviceID := ids.NewAt(ids.KindService, at, 93)
	revisionID := ids.NewAt(ids.KindTask, at, 94)
	value := scriptSourceServiceValue(t, environmentID, serviceID)
	stage := scriptSourceReferenceStage(environmentID, revisionID, 2, 1, value)
	member := testscriptsourceevidence.ScriptSourcePreparationMember{
		Reference: testscriptsourcereference.Reference{OperationID: operationID,
			ScriptExecutionID: executionID, Source: testscriptsourcereference.SourceIdentity{Kind: testscriptsourcereference.SourceService, ServiceID: serviceID},
			SourceOwnerID: environmentID},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Staged: &testscriptsourceevidence.ScriptStagedSourceEvidence{
				SourceKey: testservices.ServiceRuntimeKey(serviceID), Stage: stage, Value: value,
			},
		},
	}
	prepared, err := authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{member},
	)
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer fragment.Clear()
	wrongClaim := testblueprints.EnvironmentBlueprintStageClaim{EnvironmentID: environmentID,
		RevisionID: ids.NewAt(ids.KindTask, at, 95), RenderGeneration: 2}
	mutation := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(serviceID), Value: value},
	}
	if err = fragment.ValidateStagedMutations(wrongClaim, mutation); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ValidateStagedMutations(wrong candidate) error = %v", err)
	}
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		EnvironmentID:    environmentID,
		RevisionID:       revisionID,
		RenderGeneration: 2,
	}
	mutation[0].Key = testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID)
	if err = fragment.ValidateStagedMutations(claim, mutation); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ValidateStagedMutations(wrong source mutation) error = %v", err)
	}
}

func scriptSourceReferenceStage(
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	readRevision int64,
	value []byte,
) testscriptsourceevidence.ScriptCandidateSourceStage {
	return testscriptsourceevidence.ScriptCandidateSourceStage{
		EnvironmentID: environmentID, RevisionID: revisionID,
		RenderGeneration: renderGeneration, FixedReadRevision: readRevision,
		CanonicalValueSHA256: sha256.Sum256(value),
	}
}

type scriptSourceReferenceAuditStore struct {
	*memoryHierarchyStore
	batches [][]testkeyvalue.Mutation
}

func (store *scriptSourceReferenceAuditStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.batches = append(store.batches, testkeyvalue.CloneMutations(mutations))
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type scriptSourceReferenceFailureStore struct {
	*memoryHierarchyStore
	transacts int
	failAt    int
}

func (store *scriptSourceReferenceFailureStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.transacts++
	if store.transacts == store.failAt {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected source preparation failure")
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func scriptSourceReferenceFixture(
	t *testing.T,
) (*memoryHierarchyStore, *testscriptsourcepublication.Authority, string, []testscriptsourceevidence.ScriptSourcePreparationMember) {
	t.Helper()
	store := newMemoryHierarchyStore()
	authority, _ := testscriptsourcepublication.NewAuthority(store)
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 1)
	executionID := scriptSourceReferenceExecutionID(at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	scriptID := ids.NewAt(ids.KindScript, at, 5)
	serviceID := ids.NewAt(ids.KindService, at, 6)
	script, err := testscripts.NewRecord(
		environmentID,
		serviceID,
		core.Script{ID: scriptID, Slug: "prepared", ServiceName: "api", When: core.ScriptManual, Body: "exit 0"},
	)
	if err != nil {
		t.Fatal(err)
	}
	script.ScriptSetGeneration = environmentID
	scriptValue, _ := testscripts.EncodeRecord(script)
	body, _ := testscripts.NewScriptBodyGeneration(script)
	bodyValue, _ := testscripts.EncodeScriptBodyGeneration(body)
	serviceValue := scriptSourceServiceValue(t, environmentID, serviceID)
	seed, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptSetScriptKey(environmentID, environmentID, scriptID),
			Value: scriptValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptSetBodyGenerationKey(environmentID, environmentID, scriptID, 1),
			Value: bodyValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(serviceID), Value: serviceValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed sources = %#v, %v", seed, err)
	}
	members := []testscriptsourceevidence.ScriptSourcePreparationMember{
		{
			Reference: testscriptsourcereference.Reference{
				OperationID:       operationID,
				ScriptExecutionID: executionID,
				Source: testscriptsourcereference.SourceIdentity{
					Kind:                testscriptsourcereference.SourceBody,
					EnvironmentID:       environmentID,
					ScriptSetGeneration: environmentID,
					ScriptID:            scriptID,
					BodyGeneration:      1,
				},
				SourceOwnerID:     environmentID,
				SourceModRevision: seed.Revision,
				SourceDigest:      body.BodySHA256,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
					SourceKey: testscripts.ScriptSetBodyGenerationKey(environmentID, environmentID, scriptID, 1),
				},
			},
		},
		{
			Reference: testscriptsourcereference.Reference{
				OperationID:       operationID,
				ScriptExecutionID: executionID,
				Source: testscriptsourcereference.SourceIdentity{
					Kind:      testscriptsourcereference.SourceService,
					ServiceID: serviceID,
				},
				SourceOwnerID:     environmentID,
				SourceModRevision: seed.Revision,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
					SourceKey: testservices.ServiceRuntimeKey(serviceID),
				},
			},
		},
	}
	return store, authority, operationID, members
}

func scriptSourceServiceMembers(
	t *testing.T,
	count int,
) (*memoryHierarchyStore, string, []testscriptsourceevidence.ScriptSourcePreparationMember) {
	t.Helper()
	store := newMemoryHierarchyStore()
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 100)
	executionID := scriptSourceReferenceExecutionID(at, 101)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 102)
	mutations := make([]testkeyvalue.Mutation, count)
	members := make([]testscriptsourceevidence.ScriptSourcePreparationMember, count)
	for index := range members {
		serviceID := ids.NewAt(ids.KindService, at, int64(index+120))
		mutations[index] = testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testservices.ServiceRuntimeKey(serviceID),
			Value: scriptSourceServiceValue(t, environmentID, serviceID),
		}
		members[index] = testscriptsourceevidence.ScriptSourcePreparationMember{
			Reference: testscriptsourcereference.Reference{
				OperationID:       operationID,
				ScriptExecutionID: executionID,
				Source: testscriptsourcereference.SourceIdentity{
					Kind:      testscriptsourcereference.SourceService,
					ServiceID: serviceID,
				},
				SourceOwnerID: environmentID,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
					SourceKey: testservices.ServiceRuntimeKey(serviceID),
				},
			},
		}
	}
	seed, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !seed.Succeeded {
		t.Fatal(err)
	}
	for index := range members {
		members[index].Reference.SourceModRevision = seed.Revision
	}
	return store, operationID, members
}

func scriptSourceServiceValue(t *testing.T, environmentID, serviceID string) []byte {
	t.Helper()
	value, err := testservices.EncodeServiceRuntimeRecord(testservices.ServiceRuntimeRecord{
		EnvironmentID: environmentID,
		ServiceID:     serviceID,
		Runtime:       core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	},
	)
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	return value
}

func scriptSourceReferenceTestTime() time.Time {
	return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
}

func scriptSourceReferenceExecutionID(at time.Time, seed byte) string {
	return ulid.MustNew(ulid.Timestamp(at), bytes.NewReader(bytes.Repeat([]byte{seed}, 16))).String()
}

func scriptSourceReferenceDigest(value string) string {
	return scriptSourceReferenceBytesDigest([]byte(value))
}
func scriptSourceReferenceBytesDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func TestScriptSourceReferenceAuthorityAbandonRestoresScriptActiveReferencesExactlyOnce(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	body := members[:1]
	if _, err := authority.Prepare(context.Background(), operationID, body); err != nil {
		t.Fatalf("Prepare(body) error = %v", err)
	}
	primaryKey := testscripts.ScriptSetScriptKey(
		body[0].Reference.Source.EnvironmentID,
		body[0].Reference.Source.ScriptSetGeneration,
		body[0].Reference.Source.ScriptID,
	)
	preparedPrimary, err := testscripts.DecodeRecord(store.valueAt(primaryKey, store.revision).Value)
	if err != nil || preparedPrimary.ActiveReferences != 1 {
		t.Fatalf("prepared Script primary = %#v, %v", preparedPrimary, err)
	}
	if err = authority.Abandon(context.Background(), operationID, body); err != nil {
		t.Fatalf("Abandon(body) error = %v", err)
	}
	if err = authority.Abandon(context.Background(), operationID, body); err != nil {
		t.Fatalf("Abandon(body retry) error = %v", err)
	}
	restored, err := testscripts.DecodeRecord(store.valueAt(primaryKey, store.revision).Value)
	if err != nil || restored.ActiveReferences != 0 {
		t.Fatalf("restored Script primary = %#v, %v", restored, err)
	}
}

// Rationale: one immutable runner snapshot is the exact physical source for
// every logical Network and Volume membership retained by its Script execution.
func TestScriptSourceReferenceAuthorityValidatesRunnerSnapshotNetworkAndVolumeMembership(t *testing.T) {
	store, operationID, members, _, snapshotKey, snapshotValue := scriptRunnerSnapshotSourceFixture(t)
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(runner snapshot memberships) error = %v", err)
	}
	if prepared.MembershipCount() != 3 {
		t.Fatalf("prepared membership count = %d, want 3", prepared.MembershipCount())
	}
	for _, member := range members {
		countValue := store.valueAt(
			testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source),
			store.revision,
		)
		count, decodeErr := testscriptsourceevidence.DecodeScriptSourceCount(countValue.Value)
		if decodeErr != nil || count.ReferencedExecutionCount != 1 || count.Source != member.Reference.Source {
			t.Fatalf("logical source count = %#v, %v", count, decodeErr)
		}
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	defer fragment.Clear()
	if len(fragment.StagedRequirements()) != 0 ||
		len(fragment.Conditions()) != 2 || len(fragment.Mutations()) != 2 {
		t.Fatalf("runner snapshot fragment = %#v / %#v", fragment.Conditions(), fragment.StagedRequirements())
	}
	tampered := append([]byte(nil), snapshotValue...)
	tampered[len(tampered)-1] ^= 1
	if err = testscriptsourceevidence.ValidateScriptSourceRecord(snapshotKey, tampered, members[0].Reference); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateScriptSourceRecord(tampered snapshot) error = %v", err)
	}
	missing := members[0].Reference
	missing.Source = testscriptsourcereference.SourceIdentity{
		Kind:      testscriptsourcereference.SourceNetwork,
		NetworkID: ids.NewAt(ids.KindNetwork, scriptSourceReferenceTestTime(), 170),
	}
	if err = testscriptsourceevidence.ValidateScriptSourceRecord(snapshotKey, snapshotValue, missing); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateScriptSourceRecord(missing Network) error = %v", err)
	}
	if err = authority.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(runner snapshot memberships) error = %v", err)
	}
	if store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision) != nil {
		t.Fatal("abandonment retained the shared source descriptor")
	}
}

// Rationale: a known failure after the shared logical memberships commit must
// leave neither preparation authority nor any membership/count removal fence.
func TestScriptSourceReferenceAuthorityAbandonsSharedProjectionAfterKnownPrepublicationFailure(t *testing.T) {
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: store, failAt: 3}
	authority, err := testscriptsourcepublication.NewAuthority(failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.Prepare(context.Background(), operationID, members); err == nil {
		t.Fatal("Prepare(shared staged projection) error = nil, want injected seal failure")
	}
	descriptorValue := store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision)
	if descriptorValue == nil {
		t.Fatal("known failure did not retain the resumable source preparation")
	}
	preparedCount := 0
	for _, member := range members {
		if store.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			store.revision,
		) != nil {
			preparedCount++
		}
	}
	if preparedCount != len(members) {
		t.Fatalf("failed preparation memberships = %d, want %d", preparedCount, len(members))
	}
	if err = authority.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(known prepublication failure) error = %v", err)
	}
	if store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision) != nil {
		t.Fatal("known-failure cleanup retained the preparation descriptor")
	}
	for _, member := range members {
		if store.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			store.revision,
		) != nil ||
			store.valueAt(testscriptsourcereference.ReverseKey(member.Reference), store.revision) != nil ||
			store.valueAt(
				testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source),
				store.revision,
			) != nil {
			t.Fatalf(
				"known-failure cleanup retained %s authority",
				testscriptsourceevidence.ScriptSourceSuffix(member.Reference.Source),
			)
		}
	}
}

func scriptRunnerSnapshotSourceFixture(
	t *testing.T,
) (*memoryHierarchyStore, string, []testscriptsourceevidence.ScriptSourcePreparationMember, testscriptsourceevidence.ScriptCandidateSourceStage, string, []byte) {
	t.Helper()
	store := newMemoryHierarchyStore()
	at := scriptSourceReferenceTestTime()
	environmentID := ids.NewAt(ids.KindEnvironment, at, 160)
	operationID := ids.NewAt(ids.KindOperation, at, 161)
	executionID := scriptSourceReferenceExecutionID(at, 162)
	revisionID := ids.NewAt(ids.KindTask, at, 163)
	firstNetworkID := ids.NewAt(ids.KindNetwork, at, 164)
	secondNetworkID := ids.NewAt(ids.KindNetwork, at, 165)
	volumeID := ids.NewAt(ids.KindVolume, at, 166)
	snapshotID := scriptSourceReferenceExecutionID(at, 169)
	projectionKey := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID)
	predecessorValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(
		withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID:    environmentID,
			RevisionID:       ids.NewAt(ids.KindTask, at, 167),
			RenderGeneration: 1,
		}),
	)
	if err != nil {
		t.Fatalf("encode predecessor projection: %v", err)
	}
	snapshotPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: environmentID,
		Networks: []*agentpb.ScriptRunnerNetwork{
			{NetworkId: firstNetworkID, OwnerEnvironmentId: environmentID},
			{NetworkId: secondNetworkID, OwnerEnvironmentId: environmentID},
		},
		Mounts: []*agentpb.ScriptRunnerMount{{SourceId: volumeID}},
	})
	if err != nil {
		t.Fatalf("marshal runner snapshot: %v", err)
	}
	snapshotDigest := scriptSourceReferenceBytesDigest(snapshotPayload)
	snapshotValue, err := testrecordcodec.Encode(
		"script-runner-snapshot",
		testscriptsourceevidence.StoredScriptRunnerSnapshot{
			ExecutionID: executionID, SnapshotID: snapshotID,
			SHA256: snapshotDigest, Payload: snapshotPayload,
		},
	)
	clear(snapshotPayload)
	if err != nil {
		t.Fatalf("encode runner snapshot: %v", err)
	}
	seed, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: projectionKey, Value: predecessorValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID),
			Value: snapshotValue,
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed applied predecessor and runner snapshot = %#v, %v", seed, err)
	}
	stage := scriptSourceReferenceStage(environmentID, revisionID, 2, seed.Revision, snapshotValue)
	sources := []testscriptsourcereference.SourceIdentity{
		{Kind: testscriptsourcereference.SourceNetwork, NetworkID: firstNetworkID},
		{Kind: testscriptsourcereference.SourceNetwork, NetworkID: secondNetworkID},
		{Kind: testscriptsourcereference.SourceVolume, VolumeID: volumeID},
	}
	members := make([]testscriptsourceevidence.ScriptSourcePreparationMember, len(sources))
	for index, source := range sources {
		members[index] = testscriptsourceevidence.ScriptSourcePreparationMember{
			Reference: testscriptsourcereference.Reference{
				OperationID: operationID, ScriptExecutionID: executionID,
				Source: source, SourceOwnerID: environmentID,
				SourceModRevision: seed.Revision, SourceDigest: snapshotDigest,
			},
			Evidence: testscriptsourceevidence.ScriptSourceEvidence{
				Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
					SourceKey: testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID),
				},
			},
		}
	}
	return store, operationID, members, stage, testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID), snapshotValue
}
