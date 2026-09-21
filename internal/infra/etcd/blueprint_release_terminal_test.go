package etcd

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	json "encoding/json"
	errors "errors"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	strings "strings"
	testing "testing"
	time "time"
)

// Rationale: a claimed Blueprint attempt owns the Environment mutation epoch through its
// materialization writer, while proven terminalization and retry transfer that authority atomically.
func TestBlueprintCandidateSuccessAtomicallyPromotesSealedWorkloadAndPreservesDesiredHead(t *testing.T) {
	ctx := context.Background()
	store := &blueprintEpochMutationRejectingStore{
		releaseRenderInputTestStore: &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()},
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	repository.blueprintTerminalStore = store
	now := time.Date(2026, 9, 2, 16, 0, 0, 0, time.UTC)
	publicationID := ids.NewULID()
	operationID := ids.NewAt(ids.KindOperation, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	planID := ids.NewAt(ids.KindPlan, now, 3)
	tenantID := ids.NewAt(ids.KindTenant, now, 4)
	projectID := ids.NewAt(ids.KindProject, now, 5)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 6)
	store.epochKey = testhierarchy.EnvironmentMutationEpochKey(environmentID)
	predecessorTaskID := ids.NewAt(ids.KindTask, now, 16)
	serviceID := ids.NewAt(ids.KindService, now, 7)
	releaseID := ids.NewAt(ids.KindDeployment, now, 8)
	artifactID := ids.NewAt(ids.KindConfig, now, 9)
	stepID := ids.NewAt(ids.KindStep, now, 10)
	probeStepID := ids.NewAt(ids.KindStep, now, 17)
	compensateStepID := ids.NewAt(ids.KindStep, now, 18)
	priorReleaseID := ids.NewAt(ids.KindDeployment, now, 15)
	agentID := ids.NewAt(ids.KindAgent, now, 11)
	planHash := strings.Repeat("a", 64)
	requested := "docker.io/library/nginx:stable"
	priorWorkload := releaseTestPriorWorkload("docker.io/library/nginx:previous")
	localImageID := releaseTestWorkloadSeal(requested).LocalImageID
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 3,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID: serviceID, Name: "api", Image: requested, Strategy: core.StrategyRecreate,
			},
		}},
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	projection = withTestEnvironmentComposeArtifact(projection)
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionChunkValue, err := testblueprints.EncodeEnvironmentBlueprintChunk(
		testblueprints.EnvironmentBlueprintChunk{
			Family:        testblueprints.EnvironmentBlueprintChunkProjection,
			Sequence:      0,
			LogicalOffset: 0,
			LogicalLength: uint32(len(projectionValue)),
			Digest:        sha256.Sum256(projectionValue),
			Data:          projectionValue,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenantValue, err := testhierarchy.EncodeTenant(
		testhierarchy.TenantRecord{ID: tenantID, Slug: "tenant", Name: "Tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	projectValue, err := testhierarchy.EncodeProject(testhierarchy.ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "project", Name: "Project", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatal(err)
	}
	environmentValue, err := testhierarchy.EncodeEnvironment(testhierarchy.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady, CreateTaskID: predecessorTaskID, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	hierarchySeed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.TenantKey(tenantID), Value: tenantValue},
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.ProjectKey(projectID), Value: projectValue},
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environmentID), Value: environmentValue},
	})
	if err != nil || !hierarchySeed.Succeeded {
		t.Fatalf("seed Blueprint hierarchy = %#v, %v", hierarchySeed, err)
	}
	predecessorProjection, priorArtifactID := blueprintServingPredecessorFixture(
		t, projection, predecessorTaskID, priorReleaseID, priorWorkload,
	)
	predecessorValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(predecessorProjection)
	if err != nil {
		t.Fatal(err)
	}
	appliedSeed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), Value: predecessorValue,
	}})
	if err != nil || !appliedSeed.Succeeded {
		t.Fatalf("seed applied predecessor = %#v, %v", appliedSeed, err)
	}
	predecessorHeadValue, err := testidempotency.EncodeTaskReference(predecessorTaskID)
	if err != nil {
		t.Fatal(err)
	}
	desiredBaseline, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), Value: predecessorHeadValue,
	}})
	if err != nil || !desiredBaseline.Succeeded || desiredBaseline.Revision == appliedSeed.Revision {
		t.Fatalf("seed distinct desired baseline = %#v, %v", desiredBaseline, err)
	}
	render := testreleaserender.ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID,
		PriorArtifactID: priorArtifactID, ServiceID: serviceID, ServiceName: "api",
		CandidateWorkload: releaseTestWorkloadSeal(requested), PriorWorkload: priorWorkload,
		Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		TenantID: tenantID, TenantSlug: "tenant", ProjectID: projectID, ProjectSlug: "project",
		EnvironmentID: environmentID, EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
		Projection:          projection, ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	rawRender, err := testreleaserender.EncodeReleaseRenderInput(render)
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
		CandidateWorkload: releaseTestWorkloadSeal(requested), Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: artifactID,
		PriorServingReleaseID: priorReleaseID, PriorSuccessfulReleaseID: priorReleaseID,
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
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID,
		Members: []testreleases.ReleaseStagedMemberRef{{
			ReleaseID: releaseID, ServiceID: serviceID, IntentDigest: intentDigest,
			RenderDigest: intent.RenderInputDigest, CheckpointDigest: checkpointDigest,
		}},
		CreatedAt: now,
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
			ForwardStepIDs: []string{stepID},
			ServingPredecessor: &executionplan.ServingPredecessorInput{
				ProbeStepID:      probeStepID,
				CompensateStepID: compensateStepID,
			},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: "gp-" + environmentID, ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
				Services: []executionplan.CandidateServiceIdentity{{ServiceID: serviceID, ReleaseID: releaseID}},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	procedureBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(procedure)
	if err != nil {
		t.Fatal(err)
	}
	marker := testreleases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID,
		ManifestDigest: manifest.Digest, PublishedAt: now,
		ExecutedComposeArtifact: blueprintExecutedSingletonFixture(
			t, predecessorProjection.ComposeArtifact, artifactID, releaseID, releaseTestWorkloadSeal(requested),
		),
		CandidateReleaseDescriptor: executionplan.CandidateReleaseDescriptor{
			PlanID: planID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, ProcedureBytes: procedureBytes,
		},
	}
	marker.BlueprintRuntimes = []executionplan.BlueprintRuntimeInput{{ServiceID: serviceID, ReleaseID: releaseID,
		Target: "singleton"}}
	values := make(map[string][]byte)
	for key, record := range map[string]struct {
		typeName string
		value    any
	}{testreleases.ReleasePublicationKey(publicationID): {"release-publication", marker}, testreleases.ReleaseManifestStagingKey(publicationID): {"release-staged-manifest", manifest}, testreleases.ReleaseIntentStagingKey(publicationID, releaseID): {"release-intent", intent}, testreleases.ReleaseRenderInputStagingKey(publicationID, releaseID): {"release-render-input", json.RawMessage(rawRender)}, testreleases.ReleaseCheckpointStagingKey(publicationID, releaseID): {"release-checkpoint", checkpoint}, testreleases.ReleaseProjectionKey(serviceID): {"service-release-projection", domain.ServiceProjection{
		EnvironmentID: environmentID, ServiceID: serviceID,
		ServingReleaseID: priorReleaseID, CurrentSuccessfulReleaseID: priorReleaseID, Revision: 1,
	}},
	} {
		value, encodeErr := testreleases.EncodeReleaseRecord(record.typeName, record.value)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		values[key] = value
	}
	indexValue, err := json.Marshal(testreleases.ReleaseServiceIndexValue{Schema: 1, PublicationID: publicationID})
	if err != nil {
		t.Fatal(err)
	}
	mutations := make([]testkeyvalue.Mutation, 0, len(values)+3)
	for key, value := range values {
		mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: value})
	}
	headValue, err := testidempotency.EncodeTaskReference(taskID)
	if err != nil {
		t.Fatal(err)
	}
	sealValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: taskID,
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 3,
		ProjectionSchema: 1,
		AuditChunks:      1, AuditBytes: 1, AuditSHA256: sha256.Sum256([]byte("audit")),
		ProjectionChunks: 1, ProjectionBytes: uint64(len(projectionValue)),
		ProjectionSHA256:    sha256.Sum256(projectionValue),
		ProjectionResources: 1, BaselineHeadRevision: desiredBaseline.Revision,
		DependencyDigest: sha256.Sum256([]byte("dependencies")),
	})
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environmentID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations, testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseServiceIndexKey(environmentID, serviceID, releaseID),
			Value: indexValue,
		}, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(environmentID), Value: epochValue}, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headValue}, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintRootKey(environmentID, taskID), Value: sealValue}, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut,
			Key: testblueprints.EnvironmentBlueprintChunkKeyFor(
				environmentID,
				taskID,
				testblueprints.EnvironmentBlueprintChunkProjection,
				0,
			),
			Value: projectionChunkValue,
		},
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
	driftHierarchySeed, err := driftStore.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.TenantKey(tenantID), Value: tenantValue},
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.ProjectKey(projectID), Value: projectValue},
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environmentID), Value: environmentValue},
	})
	if err != nil || !driftHierarchySeed.Succeeded {
		t.Fatalf("seed drift Blueprint hierarchy = %#v, %v", driftHierarchySeed, err)
	}
	driftAppliedSeed, err := driftStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), Value: predecessorValue,
	}})
	if err != nil || !driftAppliedSeed.Succeeded || driftAppliedSeed.Revision != appliedSeed.Revision {
		t.Fatalf("seed drift applied predecessor = %#v, %v", driftAppliedSeed, err)
	}
	driftDesiredBaseline, err := driftStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), Value: predecessorHeadValue,
	}})
	if err != nil || !driftDesiredBaseline.Succeeded || driftDesiredBaseline.Revision != desiredBaseline.Revision {
		t.Fatalf("seed drift desired baseline = %#v, %v", driftDesiredBaseline, err)
	}
	driftSeeded, err := driftStore.Transact(ctx, nil, mutations)
	if err != nil || !driftSeeded.Succeeded {
		t.Fatalf("seed Blueprint retry drift fixture = %#v, %v", driftSeeded, err)
	}
	task := newTaskRecord(
		taskID,
		operationID, testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
			TenantID:      tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		}, testtaskjournal.TaskActorOperator, testtaskjournal.TaskUpdate, environmentID,
		120,
		now,
	)
	task.IdempotencyKey = "blueprint-candidate-original"
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.PlanID = planID
	task.PlanHash = planHash
	task.RenderGeneration = 3
	task.Params = map[string]string{
		testreleaserender.TaskReleasePublicationParam:       publicationID,
		testtaskjournal.TaskComposeArtifactParam:            artifactID,
		testtaskjournal.TaskMaterializationEnvironmentParam: environmentID,
		testblueprints.EnvironmentDesiredRevisionParam:      taskID,
	}
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: stepID},
		{Kind: testtaskjournal.TaskStepOperation, ID: probeStepID},
		{Kind: testtaskjournal.TaskStepOperation, ID: compensateStepID},
	}
	pendingTask := cloneTaskRecord(task)
	seedBlueprintRequirementTask(t, store.memoryTaskStore, task)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 1, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("claim original Blueprint Task = %#v, %t, %v", claim, found, err)
	}
	task = claim.Task.Record
	assignment := claim.Assignment.Record
	writerKey := testtaskjournal.TaskMaterializationWriterKey(environmentID)
	claimAuthority, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		writerKey, testhierarchy.EnvironmentMutationEpochKey(environmentID),
	}})
	if err != nil || claimAuthority.Values[0] == nil || claimAuthority.Values[1] == nil ||
		claimAuthority.Values[0].ModRevision != claim.Assignment.Revision ||
		claimAuthority.Values[1].ModRevision != claim.Assignment.Revision {
		t.Fatalf("original Blueprint claim authority = %#v, %v", claimAuthority, err)
	}
	writer, err := decodeTaskMaterializationWriter(claimAuthority.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	failureResult := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, FailedStepID: stepID, ExecutionEpoch: 1,
		RecreateEvidence: []testtaskjournal.TaskRecreateEvidence{{
			ServiceID: serviceID, ReleaseID: priorReleaseID, ArtifactID: priorArtifactID,
			Target: string(domain.WorkloadSingleton), Compensated: true,
		}},
	}
	_, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, writer, assignment, testtaskjournal.TaskStatusFailed, testtaskjournal.TaskResultRecord{
			Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			FailedStepID: stepID, ReconciliationRequired: true,
			RecreateEvidence: failureResult.RecreateEvidence,
		}, agentID, now.Add(2*time.Second), claim.Assignment.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindReleaseRecoveryRequired, "")) {
		t.Fatalf("unproven Blueprint failure error = %v", err)
	}
	unproven, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), writerKey,
				blueprintCandidateAttemptAuthorityKey(
					task.ID,
				), testhierarchy.EnvironmentMutationEpochKey(environmentID),
			},
		},
	)
	if err != nil || unproven.Values[0] == nil || unproven.Values[0].ModRevision != appliedSeed.Revision ||
		unproven.Values[1] == nil || unproven.Values[1].ModRevision != claim.Assignment.Revision ||
		unproven.Values[2] != nil || unproven.Values[3] == nil ||
		unproven.Values[3].ModRevision != claim.Assignment.Revision {
		t.Fatalf("unproven Blueprint failure wrote state = %#v, %v", unproven, err)
	}
	failedVersion, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID,
		testtaskjournal.TaskStatusFailed,
		failureResult,
		now.Add(2*time.Second),
	)
	if err != nil || failedVersion.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("acknowledge proven Blueprint failure = %#v, %v", failedVersion, err)
	}
	if store.lastEpochMutationCount != 1 {
		t.Fatalf("original Blueprint failure epoch mutation count = %d", store.lastEpochMutationCount)
	}
	failureRevision := failedVersion.Revision
	failureAuthority, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		blueprintCandidateAttemptAuthorityKey(
			task.ID,
		), testhierarchy.EnvironmentMutationEpochKey(environmentID), writerKey,
	}})
	if err != nil || failureAuthority.Values[0] == nil || failureAuthority.Values[1] == nil ||
		failureAuthority.Values[0].ModRevision != failureRevision ||
		failureAuthority.Values[1].ModRevision != failureRevision || failureAuthority.Values[2] != nil {
		t.Fatalf("original Blueprint terminal authority = %#v, %v", failureAuthority, err)
	}
	unpublished, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testreleases.ReleaseTerminalKey(releaseID),
				testreleases.ReleaseProjectionKey(serviceID),
				testblueprints.EnvironmentBlueprintHeadKey(environmentID),
			},
		},
	)
	if err != nil || unpublished.Values[0] != nil || unpublished.Values[1] == nil {
		t.Fatalf("failed Blueprint candidate was published = %#v, %v", unpublished, err)
	}
	predecessorReleaseProjection, err := testreleases.DecodeReleaseRecord[domain.ServiceProjection](
		unpublished.Values[1].Value, "service-release-projection",
	)
	if err != nil || predecessorReleaseProjection.ServingReleaseID != priorReleaseID ||
		predecessorReleaseProjection.CurrentSuccessfulReleaseID != priorReleaseID {
		t.Fatalf("failed Blueprint predecessor projection = %#v, %v", predecessorReleaseProjection, err)
	}
	if desired, decodeErr := testidempotency.DecodeTaskReference(unpublished.Values[2].Value); decodeErr != nil ||
		desired != taskID {
		t.Fatalf("failed Blueprint desired head = %q, %v", desired, decodeErr)
	}
	failedAt := now.Add(2 * time.Second)
	failed := failedVersion.Record
	retry, err := CloneRetryTask(
		failed,
		ids.NewAt(ids.KindTask, now.Add(3*time.Second), 13), testtaskjournal.TaskActorOperator, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	seedBlueprintRequirementTask(t, driftStore.memoryTaskStore, pendingTask)
	driftClaim, found, err := driftRepository.ClaimNextTask(ctx, agentID, 2, now.Add(time.Second))
	if err != nil || !found || driftClaim.Task.Record.ID != pendingTask.ID {
		t.Fatalf("claim drift original Blueprint Task = %#v, %t, %v", driftClaim, found, err)
	}
	driftWriterRead, err := driftStore.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{writerKey}})
	if err != nil || driftWriterRead.Values[0] == nil ||
		driftWriterRead.Values[0].ModRevision != driftClaim.Assignment.Revision {
		t.Fatalf("drift original Blueprint writer = %#v, %v", driftWriterRead, err)
	}
	driftWriter, err := decodeTaskMaterializationWriter(driftWriterRead.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	driftFailureChange, err := driftRepository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx,
		driftClaim.Task.Record,
		driftWriter,
		driftClaim.Assignment.Record,
		testtaskjournal.TaskStatusFailed,
		failureResult,
		agentID,
		now.Add(2*time.Second),
		driftClaim.Assignment.Revision,
	)
	if err != nil {
		t.Fatalf("prepare drift Blueprint failure = %v", err)
	}
	driftFailureChange.mutations = append(
		driftFailureChange.mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: writerKey},
	)
	driftFailureTransaction, err := driftStore.Transact(
		ctx, driftFailureChange.conditions, driftFailureChange.mutations,
	)
	driftFailureChange.clear()
	if err != nil || !driftFailureTransaction.Succeeded {
		t.Fatalf("commit drift Blueprint failure = %#v, %v", driftFailureTransaction, err)
	}
	driftFailed := cloneTaskRecord(driftClaim.Task.Record)
	driftFailed.Status = testtaskjournal.TaskStatusFailed
	driftFailed.FinishedAt = &failedAt
	driftFailed.Result = testtaskjournal.CloneTaskResult(&failureResult)
	driftRetryChange, err := driftRepository.prepareBlueprintCandidateRetry(
		ctx, driftFailed, retry, driftFailureTransaction.Revision,
	)
	if err != nil || !driftRetryChange.applies {
		t.Fatalf("prepare Blueprint retry drift fixture = %#v, %v", driftRetryChange, err)
	}
	driftRetryTransaction, err := driftStore.Transact(ctx, driftRetryChange.conditions, driftRetryChange.mutations)
	driftRetryChange.clear()
	if err != nil || !driftRetryTransaction.Succeeded {
		t.Fatalf("commit Blueprint retry drift fixture = %#v, %v", driftRetryTransaction, err)
	}
	seedBlueprintRequirementTask(t, driftStore.memoryTaskStore, retry)
	driftRetryClaim, found, err := driftRepository.ClaimNextTask(ctx, agentID, 3, now.Add(4*time.Second))
	if err != nil || !found || driftRetryClaim.Task.Record.ID != retry.ID {
		t.Fatalf("claim drift Blueprint retry = %#v, %t, %v", driftRetryClaim, found, err)
	}
	driftRetryAuthority, err := driftStore.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		writerKey, testhierarchy.EnvironmentMutationEpochKey(environmentID),
	}})
	if err != nil || driftRetryAuthority.Values[0] == nil || driftRetryAuthority.Values[1] == nil ||
		driftRetryAuthority.Values[0].ModRevision != driftRetryClaim.Assignment.Revision ||
		driftRetryAuthority.Values[1].ModRevision != driftRetryClaim.Assignment.Revision {
		t.Fatalf("drift Blueprint retry claim authority = %#v, %v", driftRetryAuthority, err)
	}
	driftRetryWriter, err := decodeTaskMaterializationWriter(driftRetryAuthority.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	driftEpoch, err := driftStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1,"drift":true}`),
	}})
	if err != nil || !driftEpoch.Succeeded {
		t.Fatalf("advance Blueprint retry epoch = %#v, %v", driftEpoch, err)
	}
	_, err = driftRepository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx,
		driftRetryClaim.Task.Record,
		driftRetryWriter,
		driftRetryClaim.Assignment.Record,
		testtaskjournal.TaskStatusCompleted,
		testtaskjournal.TaskResultRecord{
			Kind:       testtaskjournal.TaskResultCompose,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
		},
		agentID,
		now.Add(4*time.Second),
		driftEpoch.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Blueprint retry with intervening epoch error = %v", err)
	}
	retryChange, err := repository.prepareBlueprintCandidateRetry(ctx, failed, retry, failureRevision)
	if err != nil || !retryChange.applies {
		t.Fatalf("prepare exact Blueprint retry = %#v, %v", retryChange, err)
	}
	retryTransaction, err := store.Transact(ctx, retryChange.conditions, retryChange.mutations)
	retryChange.clear()
	if err != nil || !retryTransaction.Succeeded {
		t.Fatalf("commit exact Blueprint retry = %#v, %v", retryTransaction, err)
	}
	retryAuthorityRead, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		blueprintCandidateAttemptAuthorityKey(retry.ID), testhierarchy.EnvironmentMutationEpochKey(environmentID),
	}})
	if err != nil || retryAuthorityRead.Values[0] == nil || retryAuthorityRead.Values[1] == nil ||
		retryAuthorityRead.Values[0].ModRevision != retryTransaction.Revision ||
		retryAuthorityRead.Values[1].ModRevision != retryTransaction.Revision {
		t.Fatalf("read retry applied authority = %#v, %v", retryAuthorityRead, err)
	}
	retryAuthority, err := testrecordcodec.Decode[blueprintCandidateAttemptAuthorityRecord](
		retryAuthorityRead.Values[0].Value, "blueprint-candidate-attempt-authority",
	)
	if err != nil || writer.BlueprintAppliedPredecessor == nil ||
		retryAuthority.AppliedPredecessor != *writer.BlueprintAppliedPredecessor {
		t.Fatalf("retry applied authority = %#v, %v", retryAuthority, err)
	}
	retainedEvidence, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{Keys: []string{testreleases.ReleaseIntentStagingKey(publicationID, releaseID)}},
	)
	if err != nil || retainedEvidence.Values[0] == nil ||
		retainedEvidence.Values[0].ModRevision == retryTransaction.Revision {
		t.Fatalf("sealed Release intent changed during retry = %#v, %v", retainedEvidence, err)
	}
	seedBlueprintRequirementTask(t, store.memoryTaskStore, retry)
	retryClaim, found, err := repository.ClaimNextTask(ctx, agentID, 4, now.Add(4*time.Second))
	if err != nil || !found || retryClaim.Task.Record.ID != retry.ID {
		t.Fatalf("claim exact Blueprint retry = %#v, %t, %v", retryClaim, found, err)
	}
	task = retryClaim.Task.Record
	assignment = retryClaim.Assignment.Record
	retryClaimAuthority, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		writerKey, testhierarchy.EnvironmentMutationEpochKey(environmentID), blueprintCandidateAttemptAuthorityKey(task.ID),
	}})
	if err != nil || retryClaimAuthority.Values[0] == nil || retryClaimAuthority.Values[1] == nil ||
		retryClaimAuthority.Values[2] == nil ||
		retryClaimAuthority.Values[0].ModRevision != retryClaim.Assignment.Revision ||
		retryClaimAuthority.Values[1].ModRevision != retryClaim.Assignment.Revision {
		t.Fatalf("exact Blueprint retry claim authority = %#v, %v", retryClaimAuthority, err)
	}
	retryWriter, err := decodeTaskMaterializationWriter(retryClaimAuthority.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	retryAuthorityRevision := retryClaimAuthority.Values[2].ModRevision
	_, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, retryWriter, assignment, testtaskjournal.TaskStatusFailed, testtaskjournal.TaskResultRecord{
			Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			FailedStepID: stepID, ReconciliationRequired: true,
			RecreateEvidence: failureResult.RecreateEvidence,
		}, agentID, now.Add(5*time.Second), retryClaim.Assignment.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindReleaseRecoveryRequired, "")) {
		t.Fatalf("unproven Blueprint retry failure error = %v", err)
	}
	retryUnproven, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		writerKey, testhierarchy.EnvironmentMutationEpochKey(environmentID), blueprintCandidateAttemptAuthorityKey(task.ID),
	}})
	if err != nil || retryUnproven.Values[0] == nil || retryUnproven.Values[1] == nil ||
		retryUnproven.Values[2] == nil ||
		retryUnproven.Values[0].ModRevision != retryClaim.Assignment.Revision ||
		retryUnproven.Values[1].ModRevision != retryClaim.Assignment.Revision ||
		retryUnproven.Values[2].ModRevision != retryAuthorityRevision {
		t.Fatalf("unproven Blueprint retry failure wrote state = %#v, %v", retryUnproven, err)
	}
	completedResult := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: 1,
	}
	completedVersion, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		4,
		task.ID,
		assignment.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		completedResult,
		now.Add(5*time.Second),
	)
	if err != nil || completedVersion.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("acknowledge Blueprint retry success = %#v, %v", completedVersion, err)
	}
	if store.lastEpochMutationCount != 1 {
		t.Fatalf("Blueprint retry success epoch mutation count = %d", store.lastEpochMutationCount)
	}
	terminalRevision := completedVersion.Revision
	currentRevision, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testreleases.ReleaseTerminalKey(releaseID),
				testreleases.ReleaseProjectionKey(serviceID),
				testblueprints.EnvironmentBlueprintHeadKey(environmentID),
				testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
			},
		},
	)
	if err != nil || currentRevision.Values[0] == nil || currentRevision.Values[1] == nil ||
		currentRevision.Values[3] == nil ||
		currentRevision.Values[0].ModRevision != terminalRevision ||
		currentRevision.Values[1].ModRevision != terminalRevision ||
		currentRevision.Values[3].ModRevision != terminalRevision {
		t.Fatalf("atomic terminal read = %#v, %v", currentRevision, err)
	}
	applied, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(currentRevision.Values[3].Value)
	if err != nil || !bytes.Equal(applied.ComposeArtifact, marker.ExecutedComposeArtifact) {
		t.Fatalf("applied Blueprint artifact differs from executed candidate: %v", err)
	}
	if desired, decodeErr := testidempotency.DecodeTaskReference(currentRevision.Values[2].Value); decodeErr != nil ||
		desired != taskID ||
		currentRevision.Values[2].ModRevision == terminalRevision {
		t.Fatalf("desired head moved during promotion = %#v, %v", currentRevision.Values[2], decodeErr)
	}
	ledger := releaseLedgerFixture(t, store)
	view, err := ledger.ResolveCurrentSuccessful(ctx, environmentID, serviceID, 0)
	if err != nil {
		t.Fatalf("ResolveCurrentSuccessful() error = %v", err)
	}
	if view.Intent.CandidateWorkload != render.CandidateWorkload {
		t.Fatalf("successful Blueprint Release = %#v", view)
	}
	public, err := ledger.Get(ctx, releaseID)
	if err != nil || public.Terminal == nil || public.Intent.CandidateWorkload != render.CandidateWorkload {
		t.Fatalf("public Blueprint Release = %#v, %v", public, err)
	}
	retentionRead, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{Keys: []string{testreleases.ReleaseRetentionKey(releaseID)}},
	)
	if err != nil || retentionRead.Values[0] == nil {
		t.Fatalf("retention read = %#v, %v", retentionRead, err)
	}
	retention, err := testreleases.DecodeReleaseRecord[domain.RollbackMaterial](
		retentionRead.Values[0].Value,
		"release-retention",
	)
	references := strings.Join(retention.References, "\n")
	if err != nil || strings.Contains(references, requested) || !strings.Contains(references, localImageID) {
		t.Fatalf("rollback material = %#v, %v", retention, err)
	}
	task = completedVersion.Record
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, testtaskjournal.TaskStatusCompleted, terminalRevision,
	); err != nil {
		t.Fatalf("exact Blueprint terminal replay error = %v", err)
	}
	corruptManifest := manifest
	corruptManifest.Digest = strings.Repeat("0", 64)
	corruptManifestValue, err := testreleases.EncodeReleaseRecord("release-staged-manifest", corruptManifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDrift, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseManifestStagingKey(publicationID), Value: corruptManifestValue,
	}})
	if err != nil || !manifestDrift.Succeeded {
		t.Fatalf("drift retained Blueprint manifest = %#v, %v", manifestDrift, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, testtaskjournal.TaskStatusCompleted, manifestDrift.Revision,
	); err == nil {
		t.Fatal("Blueprint terminal replay accepted drifted retained manifest")
	}
	manifestValue, err := testreleases.EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestRestored, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseManifestStagingKey(publicationID), Value: manifestValue,
	}})
	if err != nil || !manifestRestored.Succeeded {
		t.Fatalf("restore retained Blueprint manifest = %#v, %v", manifestRestored, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, testtaskjournal.TaskStatusCompleted, manifestRestored.Revision,
	); err != nil {
		t.Fatalf("restored Blueprint terminal replay error = %v", err)
	}
	stateKeys := []string{
		testblueprints.EnvironmentBlueprintHeadKey(environmentID),
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
		testreleases.ReleaseProjectionKey(serviceID),
	}
	assertReadOnlyReplay := func(replayTask TaskRecord, status testtaskjournal.TaskStatus, revision int64) {
		t.Helper()
		before, getErr := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: stateKeys, Revision: revision})
		if getErr != nil || before == nil || len(before.Values) != len(stateKeys) {
			t.Fatalf("read successor state before replay = %#v, %v", before, getErr)
		}
		if replayErr := repository.validateBlueprintCandidateTerminalReplay(
			ctx, replayTask, status, revision,
		); replayErr != nil {
			t.Fatalf("delayed exact Blueprint terminal replay error = %v", replayErr)
		}
		after, getErr := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: stateKeys})
		if getErr != nil || after == nil || len(after.Values) != len(stateKeys) {
			t.Fatalf("read successor state after replay = %#v, %v", after, getErr)
		}
		for index := range stateKeys {
			if before.Values[index] == nil || after.Values[index] == nil ||
				before.Values[index].ModRevision != after.Values[index].ModRevision ||
				string(before.Values[index].Value) != string(after.Values[index].Value) {
				t.Fatalf(
					"delayed replay mutated %q: before=%#v after=%#v",
					stateKeys[index],
					before.Values[index],
					after.Values[index],
				)
			}
		}
	}

	successorTaskID := ids.NewAt(ids.KindTask, now.Add(5*time.Second), 15)
	successorHeadValue, err := testidempotency.EncodeTaskReference(successorTaskID)
	if err != nil {
		t.Fatal(err)
	}
	successorSealValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: successorTaskID,
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 4,
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
	successorHead, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintRootKey(environmentID, successorTaskID),
			Value: successorSealValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintHeadKey(environmentID),
			Value: successorHeadValue,
		},
	})
	if err != nil || !successorHead.Succeeded {
		t.Fatalf("publish successor Blueprint head = %#v, %v", successorHead, err)
	}
	assertReadOnlyReplay(task, testtaskjournal.TaskStatusCompleted, successorHead.Revision)

	successorProjection := projection
	successorProjection.RevisionID = successorTaskID
	successorProjection.RenderGeneration = 4
	successorProjectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(
		successorProjection,
	)
	if err != nil {
		t.Fatal(err)
	}
	successorApplied, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), Value: successorProjectionValue,
	}})
	if err != nil || !successorApplied.Succeeded {
		t.Fatalf("advance successor applied projection = %#v, %v", successorApplied, err)
	}
	assertReadOnlyReplay(task, testtaskjournal.TaskStatusCompleted, successorApplied.Revision)
	assertReadOnlyReplay(failed, testtaskjournal.TaskStatusFailed, successorApplied.Revision)
	mismatchedFailure := cloneTaskRecord(failed)
	mismatchedFailure.Result.RecreateEvidence[0].ArtifactID = artifactID
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, mismatchedFailure, testtaskjournal.TaskStatusFailed, successorApplied.Revision,
	); err == nil {
		t.Fatal("delayed failed Blueprint replay accepted mismatched compensation evidence")
	}

	terminalRead, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{Keys: []string{testreleases.ReleaseTerminalKey(releaseID)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := testreleases.DecodeReleaseRecord[domain.TerminalSummary](
		terminalRead.Values[0].Value, "release-terminal-summary",
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.AttemptIDs = []string{task.ID}
	corruptTerminal, err := testreleases.EncodeReleaseRecord("release-terminal-summary", terminal)
	if err != nil {
		t.Fatal(err)
	}
	drifted, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseTerminalKey(releaseID), Value: corruptTerminal,
	}})
	if err != nil || !drifted.Succeeded {
		t.Fatalf("drift terminal lineage = %#v, %v", drifted, err)
	}
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, testtaskjournal.TaskStatusCompleted, drifted.Revision,
	); err == nil {
		t.Fatal("Blueprint terminal replay accepted drifted attempt lineage")
	}
}
