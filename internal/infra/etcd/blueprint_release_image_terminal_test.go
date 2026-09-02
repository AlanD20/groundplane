package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBlueprintCandidateSuccessAtomicallyPromotesResolvedImageAndPreservesDesiredHead(t *testing.T) {
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
	priorArtifactID := ids.NewAt(ids.KindConfig, now, 14)
	stepID := ids.NewAt(ids.KindStep, now, 10)
	agentID := ids.NewAt(ids.KindAgent, now, 11)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 12)
	planHash := strings.Repeat("a", 64)
	requested := "docker.io/library/nginx:stable"
	immutable := "docker.io/library/nginx@sha256:" + strings.Repeat("b", 64)
	projection := EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 1,
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID: serviceID, Name: "api", Image: requested, Strategy: core.StrategyRecreate,
			},
		}},
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	projection = withTestEnvironmentComposeArtifact(projection)
	render := ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID,
		PriorArtifactID: priorArtifactID, ServiceID: serviceID, ServiceName: "api",
		Image: requested, PriorImage: "docker.io/library/nginx:previous",
		Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		TenantID: tenantID, TenantSlug: "tenant", ProjectID: projectID, ProjectSlug: "project",
		EnvironmentID: environmentID, EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
		Projection:          projection, ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	rawRender, err := EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	renderDigest, err := domain.Digest(rawRender)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID,
		OperationID: operationID, OperationKind: domain.OperationBlueprintApply,
		GroupOperationID: operationID, GroupMemberOrdinal: 1,
		Image: requested, Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: artifactID,
		RenderInputDigest: renderDigest, CreatedAt: now,
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
		CreatedAt: now,
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
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
		releasePublicationKey(publicationID):                   {"release-publication", marker},
		releaseManifestStagingKey(publicationID):               {"release-staged-manifest", manifest},
		releaseIntentStagingKey(publicationID, releaseID):      {"release-intent", intent},
		releaseRenderInputStagingKey(publicationID, releaseID): {"release-render-input", json.RawMessage(rawRender)},
		releaseCheckpointStagingKey(publicationID, releaseID):  {"release-checkpoint", checkpoint},
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
	headValue, err := encodeTaskReference(taskID)
	if err != nil {
		t.Fatal(err)
	}
	sealValue, err := encodeEnvironmentBlueprintSeal(EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: taskID,
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1,
		ProjectionSchema: 1,
		AuditChunks:      1, AuditBytes: 1, AuditSHA256: sha256.Sum256([]byte("audit")),
		ProjectionChunks: 1, ProjectionBytes: 1,
		ProjectionSHA256:    sha256.Sum256([]byte("projection")),
		ProjectionResources: 1, BaselineHeadRevision: 0,
		DependencyDigest: sha256.Sum256([]byte("dependencies")),
	})
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: releaseServiceIndexKey(environmentID, serviceID, releaseID), Value: indexValue},
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1}`)},
		Mutation{Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: headValue},
		Mutation{Type: MutationPut, Key: environmentBlueprintRootKey(environmentID, taskID), Value: sealValue},
		Mutation{Type: MutationPut, Key: executionStepResultKey(operationID, planHash, stepID), Value: resolvedValue},
	)
	seeded, err := store.Transact(ctx, nil, mutations)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Blueprint Release = %#v, %v", seeded, err)
	}
	driftStore := &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()}
	driftRepository, err := newTaskRepository(driftStore)
	if err != nil {
		t.Fatal(err)
	}
	driftSeeded, err := driftStore.Transact(ctx, nil, mutations)
	if err != nil || !driftSeeded.Succeeded {
		t.Fatalf("seed Blueprint retry drift fixture = %#v, %v", driftSeeded, err)
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
		Type: TaskUpdate, Target: environmentID, Params: map[string]string{
			TaskReleasePublicationParam:         publicationID,
			TaskMaterializationEnvironmentParam: environmentID,
			EnvironmentDesiredRevisionParam:     taskID,
		},
		Steps: []TaskStepRecord{{Kind: TaskStepOperation, ID: stepID}}, StartedAt: &startedAt,
	}
	failureResult := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, FailedStepID: stepID,
		RecreateEvidence: []TaskRecreateEvidence{{
			ServiceID: serviceID, ReleaseID: "baseline", ArtifactID: priorArtifactID,
			Target: string(domain.WorkloadSingleton), Compensated: true,
		}},
	}
	failureChange, err := repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, TaskAssignmentRecord{AssignmentID: assignmentID}, TaskStatusFailed,
		failureResult,
		agentID, now.Add(2*time.Second), seeded.Revision,
	)
	if err != nil || !failureChange.applies || len(failureChange.mutations) != 0 {
		t.Fatalf("proven Blueprint failure contribution = %#v, %v", failureChange, err)
	}
	failureTransaction, err := store.Transact(ctx, failureChange.conditions, failureChange.mutations)
	failureChange.clear()
	if err != nil || !failureTransaction.Succeeded {
		t.Fatalf("commit Blueprint failure proof = %#v, %v", failureTransaction, err)
	}
	unpublished, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseTerminalKey(releaseID), releaseProjectionKey(serviceID),
		environmentBlueprintHeadKey(environmentID),
	}})
	if err != nil || unpublished.Values[0] != nil || unpublished.Values[1] != nil {
		t.Fatalf("failed Blueprint candidate was published = %#v, %v", unpublished, err)
	}
	if desired, decodeErr := decodeTaskReference(unpublished.Values[2].Value); decodeErr != nil || desired != taskID {
		t.Fatalf("failed Blueprint desired head = %q, %v", desired, decodeErr)
	}
	_, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, TaskAssignmentRecord{AssignmentID: assignmentID}, TaskStatusFailed,
		TaskResultRecord{
			Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
			FailedStepID: stepID, ReconciliationRequired: true,
			RecreateEvidence: failureResult.RecreateEvidence,
		},
		agentID, now.Add(2*time.Second), failureTransaction.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindReleaseRecoveryRequired, "")) {
		t.Fatalf("unproven Blueprint failure error = %v", err)
	}
	failedAt := now.Add(2 * time.Second)
	failed := cloneTaskRecord(task)
	failed.Status = TaskStatusFailed
	failed.FinishedAt = &failedAt
	failed.Result = cloneTaskResult(&failureResult)
	retry := cloneTaskRecord(failed)
	retry.ID = ids.NewAt(ids.KindTask, now.Add(3*time.Second), 13)
	retry.RetryOf = failed.ID
	retry.Status = TaskStatusPending
	retry.CreatedAt = now.Add(3 * time.Second)
	retry.FinishedAt = nil
	retry.Result = nil
	driftRetryChange, err := driftRepository.prepareBlueprintCandidateRetry(ctx, failed, retry, driftSeeded.Revision)
	if err != nil || !driftRetryChange.applies {
		t.Fatalf("prepare Blueprint retry drift fixture = %#v, %v", driftRetryChange, err)
	}
	driftRetryTransaction, err := driftStore.Transact(ctx, driftRetryChange.conditions, driftRetryChange.mutations)
	driftRetryChange.clear()
	if err != nil || !driftRetryTransaction.Succeeded {
		t.Fatalf("commit Blueprint retry drift fixture = %#v, %v", driftRetryTransaction, err)
	}
	driftEpoch, err := driftStore.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1,"drift":true}`),
	}})
	if err != nil || !driftEpoch.Succeeded {
		t.Fatalf("advance Blueprint retry epoch = %#v, %v", driftEpoch, err)
	}
	driftTask := cloneTaskRecord(retry)
	driftTask.Status = TaskStatusRunning
	driftTask.StartedAt = &startedAt
	_, err = driftRepository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, driftTask, TaskAssignmentRecord{AssignmentID: assignmentID}, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		agentID, now.Add(4*time.Second), driftEpoch.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Blueprint retry with intervening epoch error = %v", err)
	}
	retryChange, err := repository.prepareBlueprintCandidateRetry(ctx, failed, retry, failureTransaction.Revision)
	if err != nil || !retryChange.applies {
		t.Fatalf("prepare exact Blueprint retry = %#v, %v", retryChange, err)
	}
	retryTransaction, err := store.Transact(ctx, retryChange.conditions, retryChange.mutations)
	retryChange.clear()
	if err != nil || !retryTransaction.Succeeded {
		t.Fatalf("commit exact Blueprint retry = %#v, %v", retryTransaction, err)
	}
	retainedEvidence, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		executionStepResultKey(operationID, planHash, stepID),
	}})
	if err != nil || retainedEvidence.Values[0] == nil ||
		retainedEvidence.Values[0].ModRevision == retryTransaction.Revision {
		t.Fatalf("resolved image evidence changed during retry = %#v, %v", retainedEvidence, err)
	}
	task = retry
	task.Status = TaskStatusRunning
	task.StartedAt = &startedAt
	assignment := TaskAssignmentRecord{AssignmentID: assignmentID}
	change, err := repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, assignment, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		agentID, now.Add(4*time.Second), retryTransaction.Revision,
	)
	if err != nil || !change.applies {
		t.Fatalf("prepare Blueprint candidate terminal = %#v, %v", change, err)
	}
	defer change.clear()
	appliedValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	change.mutations = append(change.mutations, Mutation{
		Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: appliedValue,
	})
	transaction, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("commit Blueprint candidate terminal = %#v, %v", transaction, err)
	}
	currentRevision, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseTerminalKey(releaseID), releaseProjectionKey(serviceID),
		environmentBlueprintHeadKey(environmentID),
	}})
	if err != nil || currentRevision.Values[0] == nil || currentRevision.Values[1] == nil ||
		currentRevision.Values[0].ModRevision != transaction.Revision ||
		currentRevision.Values[1].ModRevision != transaction.Revision {
		t.Fatalf("atomic terminal read = %#v, %v", currentRevision, err)
	}
	if desired, decodeErr := decodeTaskReference(currentRevision.Values[2].Value); decodeErr != nil || desired != taskID ||
		currentRevision.Values[2].ModRevision == transaction.Revision {
		t.Fatalf("desired head moved during promotion = %#v, %v", currentRevision.Values[2], decodeErr)
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
	finishedAt := now.Add(4 * time.Second)
	task.Status = TaskStatusCompleted
	task.FinishedAt = &finishedAt
	task.Result = &TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, TaskStatusCompleted, transaction.Revision,
	); err != nil {
		t.Fatalf("exact Blueprint terminal replay error = %v", err)
	}
	corruptManifest := manifest
	corruptManifest.Digest = strings.Repeat("0", 64)
	corruptManifestValue, err := encodeReleaseRecord("release-staged-manifest", corruptManifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDrift, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: releaseManifestStagingKey(publicationID), Value: corruptManifestValue,
	}})
	if err != nil || !manifestDrift.Succeeded {
		t.Fatalf("drift retained Blueprint manifest = %#v, %v", manifestDrift, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, TaskStatusCompleted, manifestDrift.Revision,
	); err == nil {
		t.Fatal("Blueprint terminal replay accepted drifted retained manifest")
	}
	manifestValue, err := encodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestRestored, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: releaseManifestStagingKey(publicationID), Value: manifestValue,
	}})
	if err != nil || !manifestRestored.Succeeded {
		t.Fatalf("restore retained Blueprint manifest = %#v, %v", manifestRestored, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, TaskStatusCompleted, manifestRestored.Revision,
	); err != nil {
		t.Fatalf("restored Blueprint terminal replay error = %v", err)
	}
	stateKeys := []string{
		environmentBlueprintHeadKey(environmentID),
		environmentComposeProjectionKey(environmentID),
		releaseProjectionKey(serviceID),
	}
	assertReadOnlyReplay := func(replayTask TaskRecord, status TaskStatus, revision int64) {
		t.Helper()
		before, getErr := store.GetMany(ctx, GetManyRequest{Keys: stateKeys, Revision: revision})
		if getErr != nil || before == nil || len(before.Values) != len(stateKeys) {
			t.Fatalf("read successor state before replay = %#v, %v", before, getErr)
		}
		if replayErr := repository.validateBlueprintCandidateTerminalReplay(
			ctx, replayTask, status, revision,
		); replayErr != nil {
			t.Fatalf("delayed exact Blueprint terminal replay error = %v", replayErr)
		}
		after, getErr := store.GetMany(ctx, GetManyRequest{Keys: stateKeys})
		if getErr != nil || after == nil || len(after.Values) != len(stateKeys) {
			t.Fatalf("read successor state after replay = %#v, %v", after, getErr)
		}
		for index := range stateKeys {
			if before.Values[index] == nil || after.Values[index] == nil ||
				before.Values[index].ModRevision != after.Values[index].ModRevision ||
				string(before.Values[index].Value) != string(after.Values[index].Value) {
				t.Fatalf("delayed replay mutated %q: before=%#v after=%#v", stateKeys[index], before.Values[index], after.Values[index])
			}
		}
	}

	successorTaskID := ids.NewAt(ids.KindTask, now.Add(5*time.Second), 15)
	successorHeadValue, err := encodeTaskReference(successorTaskID)
	if err != nil {
		t.Fatal(err)
	}
	successorSealValue, err := encodeEnvironmentBlueprintSeal(EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: successorTaskID,
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 2,
		ProjectionSchema: 1,
		AuditChunks:      1, AuditBytes: 1, AuditSHA256: sha256.Sum256([]byte("successor-audit")),
		ProjectionChunks: 1, ProjectionBytes: 1,
		ProjectionSHA256:    sha256.Sum256([]byte("successor-projection")),
		ProjectionResources: 1, BaselineHeadRevision: manifestRestored.Revision,
		DependencyDigest: sha256.Sum256([]byte("successor-dependencies")),
	})
	if err != nil {
		t.Fatal(err)
	}
	successorHead, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentBlueprintRootKey(environmentID, successorTaskID), Value: successorSealValue},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: successorHeadValue},
	})
	if err != nil || !successorHead.Succeeded {
		t.Fatalf("publish successor Blueprint head = %#v, %v", successorHead, err)
	}
	assertReadOnlyReplay(task, TaskStatusCompleted, successorHead.Revision)

	successorProjection := projection
	successorProjection.RevisionID = successorTaskID
	successorProjection.RenderGeneration = 2
	successorProjectionValue, err := encodeEnvironmentComposeProjection(successorProjection)
	if err != nil {
		t.Fatal(err)
	}
	successorApplied, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: successorProjectionValue,
	}})
	if err != nil || !successorApplied.Succeeded {
		t.Fatalf("advance successor applied projection = %#v, %v", successorApplied, err)
	}
	assertReadOnlyReplay(task, TaskStatusCompleted, successorApplied.Revision)
	assertReadOnlyReplay(failed, TaskStatusFailed, successorApplied.Revision)
	mismatchedFailure := cloneTaskRecord(failed)
	mismatchedFailure.Result.RecreateEvidence[0].ArtifactID = artifactID
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, mismatchedFailure, TaskStatusFailed, successorApplied.Revision,
	); err == nil {
		t.Fatal("delayed failed Blueprint replay accepted mismatched compensation evidence")
	}

	terminalRead, err := store.GetMany(ctx, GetManyRequest{Keys: []string{releaseTerminalKey(releaseID)}})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := decodeReleaseRecord[domain.TerminalSummary](
		terminalRead.Values[0].Value, "release-terminal-summary",
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.AttemptIDs = []string{task.ID}
	corruptTerminal, err := encodeReleaseRecord("release-terminal-summary", terminal)
	if err != nil {
		t.Fatal(err)
	}
	drifted, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: releaseTerminalKey(releaseID), Value: corruptTerminal,
	}})
	if err != nil || !drifted.Succeeded {
		t.Fatalf("drift terminal lineage = %#v, %v", drifted, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, TaskStatusCompleted, drifted.Revision,
	); err == nil {
		t.Fatal("Blueprint terminal replay accepted drifted attempt lineage")
	}
}
