package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBlueprintTaggedSuccessPersistsResolvedImageEvidence(t *testing.T) {
	ctx := context.Background()
	store := &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 2, 16, 0, 0, 0, time.UTC)
	publicationID := ids.NewULID()
	operationID := ids.NewAt(ids.KindOperation, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	planID := ids.NewAt(ids.KindPlan, now, 3)
	tenantID := ids.NewAt(ids.KindTenant, now, 4)
	projectID := ids.NewAt(ids.KindProject, now, 5)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 6)
	serviceID := ids.NewAt(ids.KindService, now, 7)
	releaseID := ids.NewAt(ids.KindDeployment, now, 8)
	artifactID := ids.NewAt(ids.KindConfig, now, 9)
	stepID := ids.NewAt(ids.KindStep, now, 10)
	agentID := ids.NewAt(ids.KindAgent, now, 11)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 12)
	planHash := strings.Repeat("a", 64)
	requested := "docker.io/library/nginx:stable"
	immutable := "docker.io/library/nginx@sha256:" + strings.Repeat("b", 64)
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID,
		OperationID: operationID, OperationKind: domain.OperationBlueprintApply,
		GroupOperationID: operationID, GroupMemberOrdinal: 1,
		Image: requested, Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureLeaveActive, RenderInputID: artifactID,
		RenderInputDigest: strings.Repeat("e", 64), CreatedAt: now,
		Actor: "operator", OriginatingTaskID: taskID,
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		},
	}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent() error = %v", err)
	}
	checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: now}
	intentDigest, _ := domain.Digest(intent)
	checkpointDigest, _ := domain.Digest(checkpoint)
	manifest := ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID,
		Members: []ReleaseStagedMemberRef{{
			ReleaseID: releaseID, ServiceID: serviceID, IntentDigest: intentDigest,
			RenderDigest: intent.RenderInputDigest, CheckpointDigest: checkpointDigest,
		}},
		Digest: strings.Repeat("f", 64), CreatedAt: now,
	}
	marker := ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID,
		ManifestDigest: manifest.Digest, PublishedAt: now,
	}
	sealedResult, err := executionplan.SealExecutionStepResult(&agentpb.ExecutionStepResult{
		OperationId: operationID, PlanHash: bytes.Repeat([]byte{0xaa}, 32), StepId: stepID,
		Result: &agentpb.ExecutionStepResult_ProcedureServiceImage{ProcedureServiceImage: &agentpb.ProcedureServiceImageResult{
			ServiceId: serviceID, ReleaseId: releaseID, RequestedReference: requested,
			ImmutableReference: immutable, ImageDigest: bytes.Repeat([]byte{0xbb}, 32),
			LocalImageId: "sha256:" + strings.Repeat("c", 64),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := newExecutionStepResultRecord(sealedResult, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string][]byte)
	for key, record := range map[string]struct {
		typeName string
		value    any
	}{
		releasePublicationKey(publicationID):                  {"release-publication", marker},
		releaseManifestStagingKey(publicationID):              {"release-staged-manifest", manifest},
		releaseIntentStagingKey(publicationID, releaseID):     {"release-intent", intent},
		releaseCheckpointStagingKey(publicationID, releaseID): {"release-checkpoint", checkpoint},
	} {
		value, encodeErr := encodeReleaseRecord(record.typeName, record.value)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		values[key] = value
	}
	resolvedValue, err := encodeEnvelope("execution-step-result", resolved)
	if err != nil {
		t.Fatal(err)
	}
	indexValue, err := json.Marshal(releaseServiceIndexValue{Schema: 1, PublicationID: publicationID})
	if err != nil {
		t.Fatal(err)
	}
	mutations := make([]Mutation, 0, len(values)+3)
	for key, value := range values {
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
	}
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: releaseServiceIndexKey(environmentID, serviceID, releaseID), Value: indexValue},
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1}`)},
		Mutation{Type: MutationPut, Key: executionStepResultKey(operationID, planHash, stepID), Value: resolvedValue},
	)
	seeded, err := store.Transact(ctx, nil, mutations)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Blueprint Release = %#v, %v", seeded, err)
	}
	if _, err := resolved.Proto(); err != nil {
		t.Fatalf("resolved record Proto() error = %v", err)
	}
	journal, err := repository.blueprintReleaseResolvedRecords(ctx, operationID, planHash, seeded.Revision)
	if err != nil || len(journal) != 1 {
		t.Fatalf("resolved journal = %#v, %v", journal, err)
	}
	startedAt := now.Add(time.Second)
	task := TaskRecord{
		ID: taskID, OperationID: operationID,
		Owner:    TaskOwner{WorkspaceType: TaskWorkspaceTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID},
		Executor: TaskExecutorAgent, PlanID: planID, PlanHash: planHash, RenderGeneration: 1,
		Type: TaskUpdate, Target: environmentID, Params: map[string]string{TaskReleasePublicationParam: publicationID},
		Steps: []TaskStepRecord{{ID: stepID}}, StartedAt: &startedAt,
	}
	assignment := TaskAssignmentRecord{AssignmentID: assignmentID}
	processed, err := repository.finalizeReleaseTaskBatch(
		ctx, task, assignment, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		agentID, now.Add(2*time.Second), seeded.Revision,
	)
	if err != nil || !processed {
		t.Fatalf("finalize Blueprint Release = %t, %v", processed, err)
	}
	currentRevision, err := store.GetMany(ctx, GetManyRequest{Keys: []string{releaseTerminalKey(releaseID)}})
	if err != nil || currentRevision.Values[0] == nil {
		t.Fatalf("terminal read = %#v, %v", currentRevision, err)
	}
	replayed, err := repository.finalizeReleaseTaskBatch(
		ctx, task, assignment, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		agentID, now.Add(2*time.Second), currentRevision.ReadRevision,
	)
	if err != nil || replayed {
		t.Fatalf("terminal replay = %t, %v", replayed, err)
	}
	ledger := &ReleaseLedger{store: store}
	view, err := ledger.ResolveCurrentSuccessful(ctx, environmentID, serviceID, 0)
	if err != nil {
		t.Fatalf("ResolveCurrentSuccessful() error = %v", err)
	}
	if view.Intent.Image != requested || view.Intent.Digest != "" || view.ResolvedImage == nil ||
		view.ResolvedImage.ImmutableReference != immutable || view.ResolvedImage.Digest != strings.Repeat("b", 64) {
		t.Fatalf("successful Blueprint Release = %#v", view)
	}
	public, err := ledger.Get(ctx, releaseID)
	if err != nil || public.Terminal == nil || public.Terminal.ResolvedImage == nil ||
		public.Terminal.ResolvedImage.ImmutableReference != immutable {
		t.Fatalf("public Blueprint Release = %#v, %v", public, err)
	}
	retentionRead, err := store.GetMany(ctx, GetManyRequest{Keys: []string{releaseRetentionKey(releaseID)}})
	if err != nil || retentionRead.Values[0] == nil {
		t.Fatalf("retention read = %#v, %v", retentionRead, err)
	}
	retention, err := decodeReleaseRecord[domain.RollbackMaterial](retentionRead.Values[0].Value, "release-retention")
	references := strings.Join(retention.References, "\n")
	if err != nil || strings.Contains(references, requested) || !strings.Contains(references, immutable) {
		t.Fatalf("rollback material = %#v, %v", retention, err)
	}
}
