package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
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
	descriptorValue := store.valueAt(scriptSourcePreparationKey(operationID), store.revision)
	if descriptorValue == nil || descriptorValue.ModRevision != prepared.descriptorRevision {
		t.Fatalf("sealed descriptor = %#v, token revision = %d", descriptorValue, prepared.descriptorRevision)
	}
	descriptor, err := decodeScriptSourcePreparation(descriptorValue.Value)
	if err != nil || descriptor.Phase != ScriptSourcePreparationSealed ||
		descriptor.PreparationCursor != descriptor.MembershipCount || descriptor.MembershipCount != uint64(len(members)) {
		t.Fatalf("sealed descriptor = %#v, %v", descriptor, err)
	}
	for _, member := range members {
		forward := store.valueAt(scriptSourceForwardReferenceKey(member.Reference), store.revision)
		reverse := store.valueAt(scriptSourceReverseReferenceKey(member.Reference), store.revision)
		countValue := store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision)
		if forward == nil || reverse == nil || countValue == nil || !bytes.Equal(forward.Value, reverse.Value) {
			t.Fatalf("membership %s is incomplete", scriptSourceSuffix(member.Reference.Source))
		}
		count, decodeErr := decodeScriptSourceCount(countValue.Value)
		if decodeErr != nil || count.ReferencedExecutionCount != 1 || count.Source != member.Reference.Source {
			t.Fatalf("source count = %#v, %v", count, decodeErr)
		}
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	defer fragment.Clear()
	if len(fragment.conditions) != 2 || len(fragment.mutations) != 2 || len(fragment.staged) != 0 ||
		fragment.conditions[0] != (Condition{Key: scriptSourcePreparationKey(operationID), ModRevision: prepared.descriptorRevision}) ||
		fragment.conditions[1] != (Condition{Key: scriptSourceRootKey(operationID)}) ||
		fragment.mutations[0].Key != scriptSourceRootKey(operationID) ||
		fragment.mutations[1].Type != MutationDelete || fragment.mutations[1].Key != scriptSourcePreparationKey(operationID) {
		t.Fatalf("final publication fragment = %#v / %#v", fragment.conditions, fragment.mutations)
	}
	result, err := store.Transact(context.Background(), fragment.conditions, fragment.mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("activate prepared source set = %#v, %v", result, err)
	}
	rootValue := store.valueAt(scriptSourceRootKey(operationID), store.revision)
	root, err := decodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.OperationID != operationID || root.Phase != ScriptOperationSourceActive ||
		root.MembershipCount != descriptor.MembershipCount || root.MembershipSHA256 != descriptor.MembershipSHA256 {
		t.Fatalf("active root = %#v, %v", root, err)
	}
}

func TestScriptSourceReferenceAuthorityExactRetryDoesNotIncrementCounts(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	members = append(members[:1:1], members[0])
	first, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(first) error = %v", err)
	}
	countKey := scriptSourceCountKey(members[0].Reference.Source)
	countBefore := store.valueAt(countKey, store.revision)
	forwardBefore := store.valueAt(scriptSourceForwardReferenceKey(members[0].Reference), store.revision)
	second, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(retry) error = %v", err)
	}
	countAfter := store.valueAt(countKey, store.revision)
	forwardAfter := store.valueAt(scriptSourceForwardReferenceKey(members[0].Reference), store.revision)
	if first.descriptorRevision != second.descriptorRevision || first.membershipSHA256 != second.membershipSHA256 ||
		countBefore.ModRevision != countAfter.ModRevision || countBefore.Version != countAfter.Version ||
		forwardBefore.ModRevision != forwardAfter.ModRevision || forwardBefore.Version != forwardAfter.Version {
		t.Fatal("exact retry mutated durable source references")
	}
}

func TestScriptSourceReferenceAuthorityRejectsConflictingOrForgedEvidence(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	conflict := members[0]
	conflict.Reference.SourceOwnerID = ids.NewAt(ids.KindEnvironment, scriptSourceReferenceTestTime(), 99)
	_, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{members[0], conflict})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(conflicting owner) error = %v", err)
	}

	forged := members[0]
	forged.Evidence.Existing = &ScriptExistingSourceEvidence{SourceKey: members[1].Evidence.Existing.SourceKey}
	_, err = authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{forged})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(unrelated same-revision key) error = %v", err)
	}
	forged = members[0]
	forged.Reference.SourceDigest = scriptSourceReferenceDigest("wrong")
	_, err = authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{forged})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(mismatched digest) error = %v", err)
	}
	primary := scriptSetScriptKey(forged.Reference.Source.EnvironmentID, forged.Reference.Source.ScriptSetGeneration, forged.Reference.Source.ScriptID)
	forged = members[0]
	forged.Evidence.Existing = &ScriptExistingSourceEvidence{SourceKey: primary}
	_, err = authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{forged})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(mutable Script primary) error = %v", err)
	}
	_ = store
}

func TestScriptSourceReferenceAuthorityFailedFinalCASHasNoActiveRoot(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	prepared, err := authority.Prepare(context.Background(), operationID, members[:1])
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	defer fragment.Clear()
	descriptor := store.valueAt(scriptSourcePreparationKey(operationID), store.revision)
	raced, err := store.Transact(context.Background(), []Condition{{Key: descriptor.Key, ModRevision: descriptor.ModRevision}}, []Mutation{{Type: MutationPut, Key: descriptor.Key, Value: descriptor.Value}})
	if err != nil || !raced.Succeeded {
		t.Fatalf("race descriptor = %#v, %v", raced, err)
	}
	result, err := store.Transact(context.Background(), fragment.conditions, fragment.mutations)
	if err != nil || result.Succeeded || store.valueAt(scriptSourceRootKey(operationID), store.revision) != nil {
		t.Fatalf("stale final CAS = %#v, %v; root became visible", result, err)
	}
}

func TestScriptSourceReferenceAuthorityBatchesAtMost16Memberships(t *testing.T) {
	base, operationID, members := scriptSourceServiceMembers(t, 17)
	store := &scriptSourceReferenceAuditStore{memoryHierarchyStore: base}
	authority, err := newScriptSourceReferenceAuthority(store)
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
			if mutation.Type == MutationPut && strings.Contains(mutation.Key, "/executions/") {
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
	for _, mutation := range fragment.mutations {
		if strings.HasPrefix(mutation.Key, scriptSourceForwardReferencePrefix) || strings.HasPrefix(mutation.Key, scriptSourceCountPrefix) || strings.Contains(mutation.Key, "/executions/") {
			t.Fatalf("activation mutates membership/count: %#v", mutation)
		}
	}
}

func TestScriptSourceReferenceAuthorityAbandonsPartialPreparationAfterRestart(t *testing.T) {
	base, operationID, members := scriptSourceServiceMembers(t, 12)
	failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: base, failAt: 3}
	authority, err := newScriptSourceReferenceAuthority(failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.Prepare(context.Background(), operationID, members); err == nil {
		t.Fatal("Prepare() error = nil, want injected later-batch failure")
	}
	descriptorValue := base.valueAt(scriptSourcePreparationKey(operationID), base.revision)
	descriptor, err := decodeScriptSourcePreparation(descriptorValue.Value)
	if err != nil || descriptor.PreparationCursor == 0 || descriptor.PreparationCursor >= descriptor.MembershipCount {
		t.Fatalf("partial descriptor = %#v, %v", descriptor, err)
	}
	restarted, err := newScriptSourceReferenceAuthority(base)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := append([]ScriptSourcePreparationMember(nil), members...)
	refreshed[0] = members[0]
	refreshed[0].Evidence.Existing = &ScriptExistingSourceEvidence{SourceKey: members[1].Evidence.Existing.SourceKey}
	if err = restarted.Abandon(context.Background(), operationID, refreshed); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Abandon(refreshed evidence) error = %v", err)
	}
	if err = restarted.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(restart) error = %v", err)
	}
	if err = restarted.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(exact retry) error = %v", err)
	}
	if base.valueAt(scriptSourcePreparationKey(operationID), base.revision) != nil {
		t.Fatal("abandonment retained the descriptor")
	}
	page, _ := base.Range(context.Background(), RangeRequest{Prefix: scriptSourceRootPrefix + operationID + "/executions/", Limit: 1})
	if len(page.Values) != 0 {
		t.Fatal("abandonment retained reverse memberships")
	}
	for _, member := range members[:int(descriptor.PreparationCursor)] {
		if base.valueAt(scriptSourceForwardReferenceKey(member.Reference), base.revision) != nil ||
			base.valueAt(scriptSourceCountKey(member.Reference.Source), base.revision) != nil {
			t.Fatal("abandonment retained forward membership or count")
		}
	}
}

func TestScriptSourceReferenceAuthorityStagesSnapshotAndReleaseRequirements(t *testing.T) {
	store := newMemoryHierarchyStore()
	authority, _ := newScriptSourceReferenceAuthority(store)
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
	snapshotValue, _ := encodeEnvelope("script-runner-snapshot", storedScriptRunnerSnapshot{ExecutionID: executionID, SnapshotID: snapshotID, SHA256: snapshotDigest, Payload: payload})
	releaseValue, _ := encodeEnvelope("release-intent", domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID, OperationID: operationID,
		OperationKind: domain.OperationDeploy, Image: "registry.example/app", Tag: "v1",
		Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureLeaveActive,
		RenderInputID: ids.NewAt(ids.KindConfig, at, 77), RenderInputDigest: scriptSourceReferenceDigest("render"),
		CreatedAt: at, Actor: "test", OriginatingTaskID: ids.NewAt(ids.KindTask, at, 78),
		Workspace: domain.Workspace{Kind: domain.WorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, at, 79), ProjectID: ids.NewAt(ids.KindProject, at, 80), EnvironmentID: environmentID},
	})
	releaseDigest := scriptSourceReferenceBytesDigest(releaseValue)
	stage := scriptSourceReferenceStage(environmentID, ids.NewAt(ids.KindTask, at, 81), 9, 1, snapshotValue)
	releaseStage := stage
	releaseStage.CanonicalValueSHA256 = sha256.Sum256(releaseValue)
	members := []ScriptSourcePreparationMember{
		{Reference: ScriptSourceReference{OperationID: operationID, ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceRunnerSnapshot, SnapshotID: snapshotID}, SourceOwnerID: environmentID, SourceDigest: snapshotDigest}, Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{SourceKey: scriptRunnerSnapshotKey(snapshotID), Stage: stage, Value: snapshotValue}}},
		{Reference: ScriptSourceReference{OperationID: operationID, ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceRelease, ReleaseID: releaseID}, SourceOwnerID: environmentID, SourceDigest: releaseDigest}, Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{SourceKey: releaseIntentStagingKey("", releaseID), Stage: releaseStage, Value: releaseValue}}},
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
	if len(fragment.conditions) != 4 || len(fragment.mutations) != 4 ||
		len(fragment.StagedRequirements()) != 2 {
		t.Fatalf("staged fragment = %#v / %#v", fragment.conditions, fragment.StagedRequirements())
	}
	stagedMutations := []Mutation{{Type: MutationPut, Key: scriptRunnerSnapshotKey(snapshotID), Value: snapshotValue}, {Type: MutationPut, Key: releaseIntentStagingKey("", releaseID), Value: releaseValue}}
	claim := EnvironmentBlueprintStageClaim{EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID, RenderGeneration: stage.RenderGeneration}
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); err != nil {
		t.Fatalf("ValidateStagedMutations(exact) error = %v", err)
	}
	stagedMutations[0].Value = []byte("changed")
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ValidateStagedMutations(changed) error = %v", err)
	}
	stagedMutations[0].Value = releaseValue
	if err = fragment.ValidateStagedMutations(claim, stagedMutations); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ValidateStagedMutations(substituted canonical value) error = %v", err)
	}
	secretID := ids.NewAt(ids.KindSecret, at, 82)
	secret := ScriptSourcePreparationMember{Reference: ScriptSourceReference{OperationID: ids.NewAt(ids.KindOperation, at, 76), ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceSecretValue, SecretID: secretID, ValueGenerationID: secretID}, SourceOwnerID: scriptSourcePlatformOwner, SourceDigest: scriptSourceReferenceDigest("absent")}, Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{SourceKey: secretValueKey(secretID), Stage: stage, Value: []byte("absent")}}}
	if _, err = authority.Prepare(context.Background(), secret.Reference.OperationID, []ScriptSourcePreparationMember{secret}); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(non-Blueprint staged kind) error = %v", err)
	}
}

func TestScriptSourceReferenceAuthorityRejectsMixedOrInvalidStagedEvidence(t *testing.T) {
	_, authority, operationID, members := scriptSourceReferenceFixture(t)
	mixed := members[0]
	mixed.Evidence.Staged = &ScriptStagedSourceEvidence{}
	if _, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{mixed}); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(mixed evidence) error = %v", err)
	}
	empty := members[0]
	empty.Evidence = ScriptSourceEvidence{}
	if _, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{empty}); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(empty evidence) error = %v", err)
	}
	staged := members[0]
	staged.Reference.SourceModRevision = 0
	value := []byte("candidate")
	staged.Evidence = ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{
		SourceKey: staged.Evidence.Existing.SourceKey,
		Stage: scriptSourceReferenceStage(staged.Reference.SourceOwnerID,
			ids.NewAt(ids.KindTask, scriptSourceReferenceTestTime(), 83), 1, 0, value),
		Value: value,
	}}
	if _, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{staged}); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(zero fixed read revision) error = %v", err)
	}
	stagedBody := members[0]
	stagedBody.Reference.SourceModRevision = 0
	stagedBody.Evidence = ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{
		SourceKey: stagedBody.Evidence.Existing.SourceKey,
		Stage: scriptSourceReferenceStage(stagedBody.Reference.SourceOwnerID,
			ids.NewAt(ids.KindTask, scriptSourceReferenceTestTime(), 84), 1, 1, value),
		Value: value,
	}}
	if _, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{stagedBody}); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Prepare(staged Script body) error = %v", err)
	}
}

func TestScriptSourcePublicationRejectsCandidateOrMutationMismatch(t *testing.T) {
	store := newMemoryHierarchyStore()
	authority, _ := newScriptSourceReferenceAuthority(store)
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 90)
	executionID := scriptSourceReferenceExecutionID(at, 91)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 92)
	serviceID := ids.NewAt(ids.KindService, at, 93)
	revisionID := ids.NewAt(ids.KindTask, at, 94)
	value := scriptSourceServiceValue(t, environmentID, serviceID)
	stage := scriptSourceReferenceStage(environmentID, revisionID, 2, 1, value)
	member := ScriptSourcePreparationMember{Reference: ScriptSourceReference{OperationID: operationID,
		ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: serviceID},
		SourceOwnerID: environmentID}, Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{
		SourceKey: serviceRuntimeKey(serviceID), Stage: stage, Value: value,
	}}}
	prepared, err := authority.Prepare(context.Background(), operationID, []ScriptSourcePreparationMember{member})
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer fragment.Clear()
	wrongClaim := EnvironmentBlueprintStageClaim{EnvironmentID: environmentID,
		RevisionID: ids.NewAt(ids.KindTask, at, 95), RenderGeneration: 2}
	mutation := []Mutation{{Type: MutationPut, Key: serviceRuntimeKey(serviceID), Value: value}}
	if err = fragment.ValidateStagedMutations(wrongClaim, mutation); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ValidateStagedMutations(wrong candidate) error = %v", err)
	}
	claim := EnvironmentBlueprintStageClaim{EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 2}
	mutation[0].Key = environmentComposeProjectionKey(environmentID)
	if err = fragment.ValidateStagedMutations(claim, mutation); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ValidateStagedMutations(wrong source mutation) error = %v", err)
	}
}

func scriptSourceReferenceStage(
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	readRevision int64,
	value []byte,
) ScriptCandidateSourceStage {
	return ScriptCandidateSourceStage{
		EnvironmentID: environmentID, RevisionID: revisionID,
		RenderGeneration: renderGeneration, FixedReadRevision: readRevision,
		CanonicalValueSHA256: sha256.Sum256(value),
	}
}

type scriptSourceReferenceAuditStore struct {
	*memoryHierarchyStore
	batches [][]Mutation
}

func (store *scriptSourceReferenceAuditStore) Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (TransactionResult, error) {
	store.batches = append(store.batches, cloneMutations(mutations))
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type scriptSourceReferenceFailureStore struct {
	*memoryHierarchyStore
	transacts int
	failAt    int
}

func (store *scriptSourceReferenceFailureStore) Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (TransactionResult, error) {
	store.transacts++
	if store.transacts == store.failAt {
		return TransactionResult{}, errs.New(errs.KindInternal, "injected source preparation failure")
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func scriptSourceReferenceFixture(t *testing.T) (*memoryHierarchyStore, *ScriptSourceReferenceAuthority, string, []ScriptSourcePreparationMember) {
	t.Helper()
	store := newMemoryHierarchyStore()
	authority, _ := newScriptSourceReferenceAuthority(store)
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 1)
	executionID := scriptSourceReferenceExecutionID(at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	scriptID := ids.NewAt(ids.KindScript, at, 5)
	serviceID := ids.NewAt(ids.KindService, at, 6)
	script, err := NewScriptRecord(environmentID, serviceID, core.Script{ID: scriptID, Slug: "prepared", ServiceName: "api", When: core.ScriptManual, Body: "exit 0"})
	if err != nil {
		t.Fatal(err)
	}
	script.ScriptSetGeneration = environmentID
	scriptValue, _ := encodeScriptRecord(script)
	body, _ := newScriptBodyGeneration(script)
	bodyValue, _ := encodeScriptBodyGeneration(body)
	serviceValue := scriptSourceServiceValue(t, environmentID, serviceID)
	seed, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: scriptSetScriptKey(environmentID, environmentID, scriptID), Value: scriptValue},
		{Type: MutationPut, Key: scriptSetBodyGenerationKey(environmentID, environmentID, scriptID, 1), Value: bodyValue},
		{Type: MutationPut, Key: serviceRuntimeKey(serviceID), Value: serviceValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed sources = %#v, %v", seed, err)
	}
	members := []ScriptSourcePreparationMember{
		{Reference: ScriptSourceReference{OperationID: operationID, ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceBody, EnvironmentID: environmentID, ScriptSetGeneration: environmentID, ScriptID: scriptID, BodyGeneration: 1}, SourceOwnerID: environmentID, SourceModRevision: seed.Revision, SourceDigest: body.BodySHA256}, Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: scriptSetBodyGenerationKey(environmentID, environmentID, scriptID, 1)}}},
		{Reference: ScriptSourceReference{OperationID: operationID, ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: serviceID}, SourceOwnerID: environmentID, SourceModRevision: seed.Revision}, Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: serviceRuntimeKey(serviceID)}}},
	}
	return store, authority, operationID, members
}

func scriptSourceServiceMembers(t *testing.T, count int) (*memoryHierarchyStore, string, []ScriptSourcePreparationMember) {
	t.Helper()
	store := newMemoryHierarchyStore()
	at := scriptSourceReferenceTestTime()
	operationID := ids.NewAt(ids.KindOperation, at, 100)
	executionID := scriptSourceReferenceExecutionID(at, 101)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 102)
	mutations := make([]Mutation, count)
	members := make([]ScriptSourcePreparationMember, count)
	for index := range members {
		serviceID := ids.NewAt(ids.KindService, at, int64(index+120))
		mutations[index] = Mutation{Type: MutationPut, Key: serviceRuntimeKey(serviceID), Value: scriptSourceServiceValue(t, environmentID, serviceID)}
		members[index] = ScriptSourcePreparationMember{Reference: ScriptSourceReference{OperationID: operationID, ScriptExecutionID: executionID, Source: ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: serviceID}, SourceOwnerID: environmentID}, Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: serviceRuntimeKey(serviceID)}}}
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
	value, err := encodeServiceRuntimeRecord(ServiceRuntimeRecord{EnvironmentID: environmentID, ServiceID: serviceID, Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning}})
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
	primaryKey := scriptSetScriptKey(body[0].Reference.Source.EnvironmentID, body[0].Reference.Source.ScriptSetGeneration, body[0].Reference.Source.ScriptID)
	preparedPrimary, err := decodeScriptRecord(store.valueAt(primaryKey, store.revision).Value)
	if err != nil || preparedPrimary.ActiveReferences != 1 {
		t.Fatalf("prepared Script primary = %#v, %v", preparedPrimary, err)
	}
	if err = authority.Abandon(context.Background(), operationID, body); err != nil {
		t.Fatalf("Abandon(body) error = %v", err)
	}
	if err = authority.Abandon(context.Background(), operationID, body); err != nil {
		t.Fatalf("Abandon(body retry) error = %v", err)
	}
	restored, err := decodeScriptRecord(store.valueAt(primaryKey, store.revision).Value)
	if err != nil || restored.ActiveReferences != 0 {
		t.Fatalf("restored Script primary = %#v, %v", restored, err)
	}
}

// Rationale: one immutable runner snapshot is the exact physical source for
// every logical Network and Volume membership retained by its Script execution.
func TestScriptSourceReferenceAuthorityValidatesRunnerSnapshotNetworkAndVolumeMembership(t *testing.T) {
	store, operationID, members, _, snapshotKey, snapshotValue := scriptRunnerSnapshotSourceFixture(t)
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(runner snapshot memberships) error = %v", err)
	}
	if prepared.membershipCount != 3 {
		t.Fatalf("prepared membership count = %d, want 3", prepared.membershipCount)
	}
	for _, member := range members {
		countValue := store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision)
		count, decodeErr := decodeScriptSourceCount(countValue.Value)
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
		len(fragment.conditions) != 2 || len(fragment.mutations) != 2 {
		t.Fatalf("runner snapshot fragment = %#v / %#v", fragment.conditions, fragment.StagedRequirements())
	}
	tampered := append([]byte(nil), snapshotValue...)
	tampered[len(tampered)-1] ^= 1
	if err = validateScriptSourceRecord(snapshotKey, tampered, members[0].Reference); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("validateScriptSourceRecord(tampered snapshot) error = %v", err)
	}
	missing := members[0].Reference
	missing.Source = ScriptSourceIdentity{
		Kind:      ScriptSourceNetwork,
		NetworkID: ids.NewAt(ids.KindNetwork, scriptSourceReferenceTestTime(), 170),
	}
	if err = validateScriptSourceRecord(snapshotKey, snapshotValue, missing); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("validateScriptSourceRecord(missing Network) error = %v", err)
	}
	if err = authority.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(runner snapshot memberships) error = %v", err)
	}
	if store.valueAt(scriptSourcePreparationKey(operationID), store.revision) != nil {
		t.Fatal("abandonment retained the shared source descriptor")
	}
}

// Rationale: a known failure after the shared logical memberships commit must
// leave neither preparation authority nor any membership/count removal fence.
func TestScriptSourceReferenceAuthorityAbandonsSharedProjectionAfterKnownPrepublicationFailure(t *testing.T) {
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: store, failAt: 3}
	authority, err := newScriptSourceReferenceAuthority(failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.Prepare(context.Background(), operationID, members); err == nil {
		t.Fatal("Prepare(shared staged projection) error = nil, want injected seal failure")
	}
	descriptorValue := store.valueAt(scriptSourcePreparationKey(operationID), store.revision)
	if descriptorValue == nil {
		t.Fatal("known failure did not retain the resumable source preparation")
	}
	descriptor, err := decodeScriptSourcePreparation(descriptorValue.Value)
	if err != nil || descriptor.PreparationCursor != 3 || descriptor.MembershipCount != 3 {
		t.Fatalf("failed preparation descriptor = %#v, %v", descriptor, err)
	}
	if err = authority.Abandon(context.Background(), operationID, members); err != nil {
		t.Fatalf("Abandon(known prepublication failure) error = %v", err)
	}
	if store.valueAt(scriptSourcePreparationKey(operationID), store.revision) != nil {
		t.Fatal("known-failure cleanup retained the preparation descriptor")
	}
	for _, member := range members {
		if store.valueAt(scriptSourceForwardReferenceKey(member.Reference), store.revision) != nil ||
			store.valueAt(scriptSourceReverseReferenceKey(member.Reference), store.revision) != nil ||
			store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision) != nil {
			t.Fatalf("known-failure cleanup retained %s authority", scriptSourceSuffix(member.Reference.Source))
		}
	}
}

func scriptRunnerSnapshotSourceFixture(
	t *testing.T,
) (*memoryHierarchyStore, string, []ScriptSourcePreparationMember, ScriptCandidateSourceStage, string, []byte) {
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
	projectionKey := environmentComposeProjectionKey(environmentID)
	predecessorValue, err := encodeEnvironmentComposeProjection(withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    environmentID,
		RevisionID:       ids.NewAt(ids.KindTask, at, 167),
		RenderGeneration: 1,
	}))
	if err != nil {
		t.Fatalf("encode predecessor projection: %v", err)
	}
	snapshotPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: environmentID,
		Networks: []*agentpb.ScriptRunnerNetwork{
			{NetworkId: firstNetworkID},
			{NetworkId: secondNetworkID},
		},
		Mounts: []*agentpb.ScriptRunnerMount{{SourceId: volumeID}},
	})
	if err != nil {
		t.Fatalf("marshal runner snapshot: %v", err)
	}
	snapshotDigest := scriptSourceReferenceBytesDigest(snapshotPayload)
	snapshotValue, err := encodeEnvelope("script-runner-snapshot", storedScriptRunnerSnapshot{
		ExecutionID: executionID, SnapshotID: snapshotID,
		SHA256: snapshotDigest, Payload: snapshotPayload,
	})
	clear(snapshotPayload)
	if err != nil {
		t.Fatalf("encode runner snapshot: %v", err)
	}
	seed, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: projectionKey, Value: predecessorValue},
		{Type: MutationPut, Key: scriptRunnerSnapshotKey(snapshotID), Value: snapshotValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed applied predecessor and runner snapshot = %#v, %v", seed, err)
	}
	stage := scriptSourceReferenceStage(environmentID, revisionID, 2, seed.Revision, snapshotValue)
	sources := []ScriptSourceIdentity{
		{Kind: ScriptSourceNetwork, NetworkID: firstNetworkID},
		{Kind: ScriptSourceNetwork, NetworkID: secondNetworkID},
		{Kind: ScriptSourceVolume, VolumeID: volumeID},
	}
	members := make([]ScriptSourcePreparationMember, len(sources))
	for index, source := range sources {
		members[index] = ScriptSourcePreparationMember{
			Reference: ScriptSourceReference{
				OperationID: operationID, ScriptExecutionID: executionID,
				Source: source, SourceOwnerID: environmentID,
				SourceModRevision: seed.Revision, SourceDigest: snapshotDigest,
			},
			Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
				SourceKey: scriptRunnerSnapshotKey(snapshotID),
			}},
		}
	}
	return store, operationID, members, stage, scriptRunnerSnapshotKey(snapshotID), snapshotValue
}
