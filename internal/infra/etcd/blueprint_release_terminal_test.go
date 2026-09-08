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
	"google.golang.org/protobuf/proto"
)

// Rationale: an older pending reconciliation may lawfully advance the applied projection and
// Environment epoch after candidate publication, while an unrelated epoch rewrite remains drift.
func TestBlueprintCandidateClaimEpochAcceptsOnlyValidatedAppliedPredecessor(t *testing.T) {
	for _, test := range []struct {
		name                      string
		advanceAppliedPredecessor bool
		wantClaim                 bool
	}{
		{name: "applied predecessor and epoch", advanceAppliedPredecessor: true, wantClaim: true},
		{name: "epoch only", wantClaim: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)
			tenantID := ids.NewAt(ids.KindTenant, now, 801)
			projectID := ids.NewAt(ids.KindProject, now, 802)
			environmentID := ids.NewAt(ids.KindEnvironment, now, 803)
			initialTaskID := ids.NewAt(ids.KindTask, now, 804)
			intermediateTaskID := ids.NewAt(ids.KindTask, now, 805)
			publicationID := ids.NewULID()

			initialProjection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: initialTaskID, RenderGeneration: 1,
			})
			initialValue, err := encodeEnvironmentComposeProjection(initialProjection)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := store.Transact(ctx, nil, []Mutation{{
				Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: initialValue,
			}})
			if err != nil || !initial.Succeeded {
				t.Fatalf("seed lower applied projection = %#v, %v", initial, err)
			}

			task := materializationLifecycleTask(now.Add(time.Second), environmentID, 3)
			task.Owner = TaskOwner{
				WorkspaceType: TaskWorkspaceTenant, TenantID: tenantID,
				ProjectID: projectID, EnvironmentID: environmentID,
			}
			task.Params[TaskReleasePublicationParam] = publicationID
			task.Params[EnvironmentDesiredRevisionParam] = task.ID
			artifactID := ids.NewAt(ids.KindConfig, now, 807)
			serviceID := ids.NewAt(ids.KindService, now, 808)
			releaseID := ids.NewAt(ids.KindDeployment, now, 809)
			probeStepID := ids.NewAt(ids.KindStep, now, 810)
			compensateStepID := ids.NewAt(ids.KindStep, now, 811)
			task.Params[TaskComposeArtifactParam] = artifactID
			task.Steps = append(task.Steps,
				TaskStepRecord{Kind: TaskStepOperation, ID: probeStepID},
				TaskStepRecord{Kind: TaskStepOperation, ID: compensateStepID},
			)
			procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
				Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
				Members: []executionplan.CandidateReleaseMemberInput{{
					ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
					ForwardStepIDs: []string{task.Steps[0].ID},
					ServingPredecessor: &executionplan.ServingPredecessorInput{
						ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
					},
					CandidateAbsence: &executionplan.CandidateAbsenceInput{
						ComposeProjectName: "gp-" + environmentID,
						ProbeStepID:        probeStepID, CompensateStepID: compensateStepID,
						Services: []executionplan.CandidateServiceIdentity{
							{ServiceID: serviceID, ReleaseID: releaseID},
						},
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
			descriptor := executionplan.CandidateReleaseDescriptor{
				PlanID: task.PlanID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
				Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, ProcedureBytes: procedureBytes,
			}
			manifest := ReleaseStagedManifest{
				PublicationID: publicationID, OperationID: task.OperationID, CreatedAt: now,
				Members: []ReleaseStagedMemberRef{{
					ReleaseID: releaseID, ServiceID: serviceID,
					IntentDigest: strings.Repeat("b", 64), RenderDigest: strings.Repeat("c", 64),
					CheckpointDigest: strings.Repeat("d", 64),
				}},
			}
			manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			seedBlueprintRequirementTask(t, store, task)

			markerValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
				PublicationID: publicationID, OperationID: task.OperationID,
				ManifestDigest: manifest.Digest, CandidateReleaseDescriptor: descriptor, PublishedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			manifestValue, err := encodeReleaseRecord("release-staged-manifest", manifest)
			if err != nil {
				t.Fatal(err)
			}
			epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
				EnvironmentID: environmentID,
			})
			if err != nil {
				t.Fatal(err)
			}
			published, err := store.Transact(ctx, nil, []Mutation{
				{Type: MutationPut, Key: releasePublicationKey(publicationID), Value: markerValue},
				{Type: MutationPut, Key: releaseManifestStagingKey(publicationID), Value: manifestValue},
				{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue},
			})
			if err != nil || !published.Succeeded {
				t.Fatalf("publish pending Blueprint candidate = %#v, %v", published, err)
			}

			intermediateProjection := initialProjection
			intermediateProjection.RevisionID = intermediateTaskID
			intermediateProjection.RenderGeneration = 2
			intermediateValue, err := encodeEnvironmentComposeProjection(intermediateProjection)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []Mutation{{
				Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue,
			}}
			if test.advanceAppliedPredecessor {
				mutations = append(mutations, Mutation{
					Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: intermediateValue,
				})
			}
			advanced, err := store.Transact(ctx, nil, mutations)
			if err != nil || !advanced.Succeeded || advanced.Revision == published.Revision {
				t.Fatalf("advance intermediate applied state = %#v, %v", advanced, err)
			}

			claim, found, claimErr := repository.ClaimNextTask(
				ctx, ids.NewAt(ids.KindAgent, now, 806), 1, now.Add(2*time.Second),
			)
			if !test.wantClaim {
				if claimErr == nil || found || !errors.Is(claimErr, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("claim after epoch-only rewrite = %#v, %t, %v", claim, found, claimErr)
				}
				pending, getErr := repository.GetTask(ctx, task.ID)
				if getErr != nil || pending.Record.Status != TaskStatusPending {
					t.Fatalf("rejected Blueprint claim = %#v, %v", pending, getErr)
				}
				return
			}
			if claimErr != nil || !found || claim.Task.Record.ID != task.ID {
				t.Fatalf("claim after applied predecessor advance = %#v, %t, %v", claim, found, claimErr)
			}
			authority, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
				taskMaterializationWriterKey(environmentID),
				environmentMutationEpochKey(environmentID),
				environmentComposeProjectionKey(environmentID),
			}})
			if err != nil || authority.Values[0] == nil || authority.Values[1] == nil || authority.Values[2] == nil ||
				authority.Values[0].ModRevision != claim.Assignment.Revision ||
				authority.Values[1].ModRevision != claim.Assignment.Revision ||
				authority.Values[2].ModRevision != advanced.Revision {
				t.Fatalf("claimed Blueprint authority = %#v, %v", authority, err)
			}
			writer, err := decodeTaskMaterializationWriter(authority.Values[0].Value)
			if err != nil || writer.BlueprintAppliedPredecessor == nil ||
				*writer.BlueprintAppliedPredecessor != (taskMaterializationAppliedPredecessor{
					Present: true, KeyRevision: advanced.Revision,
					RevisionID: intermediateTaskID, RenderGeneration: 2,
				}) {
				t.Fatalf("claimed applied predecessor writer = %#v, %v", writer, err)
			}
		})
	}
}

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
	store.epochKey = environmentMutationEpochKey(environmentID)
	predecessorTaskID := ids.NewAt(ids.KindTask, now, 16)
	serviceID := ids.NewAt(ids.KindService, now, 7)
	releaseID := ids.NewAt(ids.KindDeployment, now, 8)
	artifactID := ids.NewAt(ids.KindConfig, now, 9)
	priorArtifactID := ids.NewAt(ids.KindConfig, now, 14)
	stepID := ids.NewAt(ids.KindStep, now, 10)
	probeStepID := ids.NewAt(ids.KindStep, now, 17)
	compensateStepID := ids.NewAt(ids.KindStep, now, 18)
	priorReleaseID := ids.NewAt(ids.KindDeployment, now, 15)
	agentID := ids.NewAt(ids.KindAgent, now, 11)
	planHash := strings.Repeat("a", 64)
	requested := "docker.io/library/nginx:stable"
	priorWorkload := releaseTestPriorWorkload("docker.io/library/nginx:previous")
	localImageID := releaseTestWorkloadSeal(requested).LocalImageID
	projection := EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 3,
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID: serviceID, Name: "api", Image: requested, Strategy: core.StrategyRecreate,
			},
		}},
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	projection = withTestEnvironmentComposeArtifact(projection)
	projectionValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionChunkValue, err := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
		Family:        EnvironmentBlueprintChunkProjection,
		Sequence:      0,
		LogicalOffset: 0,
		LogicalLength: uint32(len(projectionValue)),
		Digest:        sha256.Sum256(projectionValue),
		Data:          projectionValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantValue, err := encodeTenant(TenantRecord{ID: tenantID, Slug: "tenant", Name: "Tenant"})
	if err != nil {
		t.Fatal(err)
	}
	projectValue, err := encodeProject(ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "project", Name: "Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatal(err)
	}
	environmentValue, err := encodeEnvironment(EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady, CreateTaskID: predecessorTaskID, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	hierarchySeed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: tenantKey(tenantID), Value: tenantValue},
		{Type: MutationPut, Key: projectKey(projectID), Value: projectValue},
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
	})
	if err != nil || !hierarchySeed.Succeeded {
		t.Fatalf("seed Blueprint hierarchy = %#v, %v", hierarchySeed, err)
	}
	predecessorProjection, priorArtifactID := blueprintServingPredecessorFixture(
		t, projection, predecessorTaskID, priorReleaseID, priorWorkload,
	)
	predecessorValue, err := encodeEnvironmentComposeProjection(predecessorProjection)
	if err != nil {
		t.Fatal(err)
	}
	appliedSeed, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: predecessorValue,
	}})
	if err != nil || !appliedSeed.Succeeded {
		t.Fatalf("seed applied predecessor = %#v, %v", appliedSeed, err)
	}
	predecessorHeadValue, err := encodeTaskReference(predecessorTaskID)
	if err != nil {
		t.Fatal(err)
	}
	desiredBaseline, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: predecessorHeadValue,
	}})
	if err != nil || !desiredBaseline.Succeeded || desiredBaseline.Revision == appliedSeed.Revision {
		t.Fatalf("seed distinct desired baseline = %#v, %v", desiredBaseline, err)
	}
	render := ReleaseRenderInput{
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
	marker := ReleasePublicationMarker{
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
		releaseProjectionKey(serviceID): {"service-release-projection", domain.ServiceProjection{
			EnvironmentID: environmentID, ServiceID: serviceID,
			ServingReleaseID: priorReleaseID, CurrentSuccessfulReleaseID: priorReleaseID, Revision: 1,
		}},
	} {
		value, encodeErr := encodeReleaseRecord(record.typeName, record.value)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		values[key] = value
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
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 3,
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
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environmentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations,
		Mutation{
			Type:  MutationPut,
			Key:   releaseServiceIndexKey(environmentID, serviceID, releaseID),
			Value: indexValue,
		},
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue},
		Mutation{Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: headValue},
		Mutation{Type: MutationPut, Key: environmentBlueprintRootKey(environmentID, taskID), Value: sealValue},
		Mutation{
			Type:  MutationPut,
			Key:   environmentBlueprintChunkKeyFor(environmentID, taskID, EnvironmentBlueprintChunkProjection, 0),
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
	driftHierarchySeed, err := driftStore.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: tenantKey(tenantID), Value: tenantValue},
		{Type: MutationPut, Key: projectKey(projectID), Value: projectValue},
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
	})
	if err != nil || !driftHierarchySeed.Succeeded {
		t.Fatalf("seed drift Blueprint hierarchy = %#v, %v", driftHierarchySeed, err)
	}
	driftAppliedSeed, err := driftStore.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(environmentID), Value: predecessorValue,
	}})
	if err != nil || !driftAppliedSeed.Succeeded || driftAppliedSeed.Revision != appliedSeed.Revision {
		t.Fatalf("seed drift applied predecessor = %#v, %v", driftAppliedSeed, err)
	}
	driftDesiredBaseline, err := driftStore.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: predecessorHeadValue,
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
		operationID,
		TaskOwner{
			WorkspaceType: TaskWorkspaceTenant,
			TenantID:      tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		},
		TaskActorOperator,
		TaskUpdate,
		environmentID,
		120,
		now,
	)
	task.IdempotencyKey = "blueprint-candidate-original"
	task.Executor = TaskExecutorAgent
	task.PlanID = planID
	task.PlanHash = planHash
	task.RenderGeneration = 3
	task.Params = map[string]string{
		TaskReleasePublicationParam:         publicationID,
		TaskComposeArtifactParam:            artifactID,
		TaskMaterializationEnvironmentParam: environmentID,
		EnvironmentDesiredRevisionParam:     taskID,
	}
	task.Steps = []TaskStepRecord{
		{Kind: TaskStepOperation, ID: stepID},
		{Kind: TaskStepOperation, ID: probeStepID},
		{Kind: TaskStepOperation, ID: compensateStepID},
	}
	pendingTask := cloneTaskRecord(task)
	seedBlueprintRequirementTask(t, store.memoryTaskStore, task)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 1, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("claim original Blueprint Task = %#v, %t, %v", claim, found, err)
	}
	task = claim.Task.Record
	assignment := claim.Assignment.Record
	writerKey := taskMaterializationWriterKey(environmentID)
	claimAuthority, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		writerKey, environmentMutationEpochKey(environmentID),
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
	failureResult := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, FailedStepID: stepID,
		RecreateEvidence: []TaskRecreateEvidence{{
			ServiceID: serviceID, ReleaseID: priorReleaseID, ArtifactID: priorArtifactID,
			Target: string(domain.WorkloadSingleton), Compensated: true,
		}},
	}
	_, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, task, writer, assignment, TaskStatusFailed,
		TaskResultRecord{
			Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
			FailedStepID: stepID, ReconciliationRequired: true,
			RecreateEvidence: failureResult.RecreateEvidence,
		},
		agentID, now.Add(2*time.Second), claim.Assignment.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindReleaseRecoveryRequired, "")) {
		t.Fatalf("unproven Blueprint failure error = %v", err)
	}
	unproven, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		environmentComposeProjectionKey(environmentID), writerKey,
		blueprintCandidateAttemptAuthorityKey(task.ID), environmentMutationEpochKey(environmentID),
	}})
	if err != nil || unproven.Values[0] == nil || unproven.Values[0].ModRevision != appliedSeed.Revision ||
		unproven.Values[1] == nil || unproven.Values[1].ModRevision != claim.Assignment.Revision ||
		unproven.Values[2] != nil || unproven.Values[3] == nil ||
		unproven.Values[3].ModRevision != claim.Assignment.Revision {
		t.Fatalf("unproven Blueprint failure wrote state = %#v, %v", unproven, err)
	}
	failedVersion, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, assignment.AssignmentID,
		TaskStatusFailed, failureResult, now.Add(2*time.Second),
	)
	if err != nil || failedVersion.Record.Status != TaskStatusFailed {
		t.Fatalf("acknowledge proven Blueprint failure = %#v, %v", failedVersion, err)
	}
	if store.lastEpochMutationCount != 1 {
		t.Fatalf("original Blueprint failure epoch mutation count = %d", store.lastEpochMutationCount)
	}
	failureRevision := failedVersion.Revision
	failureAuthority, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		blueprintCandidateAttemptAuthorityKey(task.ID), environmentMutationEpochKey(environmentID), writerKey,
	}})
	if err != nil || failureAuthority.Values[0] == nil || failureAuthority.Values[1] == nil ||
		failureAuthority.Values[0].ModRevision != failureRevision ||
		failureAuthority.Values[1].ModRevision != failureRevision || failureAuthority.Values[2] != nil {
		t.Fatalf("original Blueprint terminal authority = %#v, %v", failureAuthority, err)
	}
	unpublished, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseTerminalKey(releaseID), releaseProjectionKey(serviceID),
		environmentBlueprintHeadKey(environmentID),
	}})
	if err != nil || unpublished.Values[0] != nil || unpublished.Values[1] == nil {
		t.Fatalf("failed Blueprint candidate was published = %#v, %v", unpublished, err)
	}
	predecessorReleaseProjection, err := decodeReleaseRecord[domain.ServiceProjection](
		unpublished.Values[1].Value, "service-release-projection",
	)
	if err != nil || predecessorReleaseProjection.ServingReleaseID != priorReleaseID ||
		predecessorReleaseProjection.CurrentSuccessfulReleaseID != priorReleaseID {
		t.Fatalf("failed Blueprint predecessor projection = %#v, %v", predecessorReleaseProjection, err)
	}
	if desired, decodeErr := decodeTaskReference(unpublished.Values[2].Value); decodeErr != nil || desired != taskID {
		t.Fatalf("failed Blueprint desired head = %q, %v", desired, decodeErr)
	}
	failedAt := now.Add(2 * time.Second)
	failed := failedVersion.Record
	retry, err := cloneRetryTask(
		failed,
		ids.NewAt(ids.KindTask, now.Add(3*time.Second), 13),
		TaskActorOperator,
		now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	seedBlueprintRequirementTask(t, driftStore.memoryTaskStore, pendingTask)
	driftClaim, found, err := driftRepository.ClaimNextTask(ctx, agentID, 2, now.Add(time.Second))
	if err != nil || !found || driftClaim.Task.Record.ID != pendingTask.ID {
		t.Fatalf("claim drift original Blueprint Task = %#v, %t, %v", driftClaim, found, err)
	}
	driftWriterRead, err := driftStore.GetMany(ctx, GetManyRequest{Keys: []string{writerKey}})
	if err != nil || driftWriterRead.Values[0] == nil ||
		driftWriterRead.Values[0].ModRevision != driftClaim.Assignment.Revision {
		t.Fatalf("drift original Blueprint writer = %#v, %v", driftWriterRead, err)
	}
	driftWriter, err := decodeTaskMaterializationWriter(driftWriterRead.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	driftFailureChange, err := driftRepository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, driftClaim.Task.Record, driftWriter, driftClaim.Assignment.Record, TaskStatusFailed,
		failureResult, agentID, now.Add(2*time.Second), driftClaim.Assignment.Revision,
	)
	if err != nil {
		t.Fatalf("prepare drift Blueprint failure = %v", err)
	}
	driftFailureChange.mutations = append(
		driftFailureChange.mutations, Mutation{Type: MutationDelete, Key: writerKey},
	)
	driftFailureTransaction, err := driftStore.Transact(
		ctx, driftFailureChange.conditions, driftFailureChange.mutations,
	)
	driftFailureChange.clear()
	if err != nil || !driftFailureTransaction.Succeeded {
		t.Fatalf("commit drift Blueprint failure = %#v, %v", driftFailureTransaction, err)
	}
	driftFailed := cloneTaskRecord(driftClaim.Task.Record)
	driftFailed.Status = TaskStatusFailed
	driftFailed.FinishedAt = &failedAt
	driftFailed.Result = cloneTaskResult(&failureResult)
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
	driftRetryAuthority, err := driftStore.GetMany(ctx, GetManyRequest{Keys: []string{
		writerKey, environmentMutationEpochKey(environmentID),
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
	driftEpoch, err := driftStore.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1,"drift":true}`),
	}})
	if err != nil || !driftEpoch.Succeeded {
		t.Fatalf("advance Blueprint retry epoch = %#v, %v", driftEpoch, err)
	}
	_, err = driftRepository.prepareBlueprintCandidateTerminalAcknowledgement(
		ctx, driftRetryClaim.Task.Record, driftRetryWriter, driftRetryClaim.Assignment.Record, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		agentID, now.Add(4*time.Second), driftEpoch.Revision,
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
	retryAuthorityRead, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		blueprintCandidateAttemptAuthorityKey(retry.ID), environmentMutationEpochKey(environmentID),
	}})
	if err != nil || retryAuthorityRead.Values[0] == nil || retryAuthorityRead.Values[1] == nil ||
		retryAuthorityRead.Values[0].ModRevision != retryTransaction.Revision ||
		retryAuthorityRead.Values[1].ModRevision != retryTransaction.Revision {
		t.Fatalf("read retry applied authority = %#v, %v", retryAuthorityRead, err)
	}
	retryAuthority, err := decodeEnvelope[blueprintCandidateAttemptAuthorityRecord](
		retryAuthorityRead.Values[0].Value, "blueprint-candidate-attempt-authority",
	)
	if err != nil || writer.BlueprintAppliedPredecessor == nil ||
		retryAuthority.AppliedPredecessor != *writer.BlueprintAppliedPredecessor {
		t.Fatalf("retry applied authority = %#v, %v", retryAuthority, err)
	}
	retainedEvidence, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseIntentStagingKey(publicationID, releaseID),
	}})
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
	retryClaimAuthority, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		writerKey, environmentMutationEpochKey(environmentID), blueprintCandidateAttemptAuthorityKey(task.ID),
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
		ctx, task, retryWriter, assignment, TaskStatusFailed,
		TaskResultRecord{
			Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
			FailedStepID: stepID, ReconciliationRequired: true,
			RecreateEvidence: failureResult.RecreateEvidence,
		},
		agentID, now.Add(5*time.Second), retryClaim.Assignment.Revision,
	)
	if !errors.Is(err, errs.New(errs.KindReleaseRecoveryRequired, "")) {
		t.Fatalf("unproven Blueprint retry failure error = %v", err)
	}
	retryUnproven, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		writerKey, environmentMutationEpochKey(environmentID), blueprintCandidateAttemptAuthorityKey(task.ID),
	}})
	if err != nil || retryUnproven.Values[0] == nil || retryUnproven.Values[1] == nil ||
		retryUnproven.Values[2] == nil ||
		retryUnproven.Values[0].ModRevision != retryClaim.Assignment.Revision ||
		retryUnproven.Values[1].ModRevision != retryClaim.Assignment.Revision ||
		retryUnproven.Values[2].ModRevision != retryAuthorityRevision {
		t.Fatalf("unproven Blueprint retry failure wrote state = %#v, %v", retryUnproven, err)
	}
	completedResult := TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone}
	completedVersion, err := repository.AcknowledgeTask(
		ctx, agentID, 4, task.ID, assignment.AssignmentID,
		TaskStatusCompleted, completedResult, now.Add(5*time.Second),
	)
	if err != nil || completedVersion.Record.Status != TaskStatusCompleted {
		t.Fatalf("acknowledge Blueprint retry success = %#v, %v", completedVersion, err)
	}
	if store.lastEpochMutationCount != 1 {
		t.Fatalf("Blueprint retry success epoch mutation count = %d", store.lastEpochMutationCount)
	}
	terminalRevision := completedVersion.Revision
	currentRevision, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseTerminalKey(releaseID), releaseProjectionKey(serviceID),
		environmentBlueprintHeadKey(environmentID),
		environmentComposeProjectionKey(environmentID),
	}})
	if err != nil || currentRevision.Values[0] == nil || currentRevision.Values[1] == nil ||
		currentRevision.Values[3] == nil ||
		currentRevision.Values[0].ModRevision != terminalRevision ||
		currentRevision.Values[1].ModRevision != terminalRevision ||
		currentRevision.Values[3].ModRevision != terminalRevision {
		t.Fatalf("atomic terminal read = %#v, %v", currentRevision, err)
	}
	applied, err := decodeEnvironmentComposeProjection(currentRevision.Values[3].Value)
	if err != nil || !bytes.Equal(applied.ComposeArtifact, marker.ExecutedComposeArtifact) {
		t.Fatalf("applied Blueprint artifact differs from executed candidate: %v", err)
	}
	if desired, decodeErr := decodeTaskReference(currentRevision.Values[2].Value); decodeErr != nil ||
		desired != taskID ||
		currentRevision.Values[2].ModRevision == terminalRevision {
		t.Fatalf("desired head moved during promotion = %#v, %v", currentRevision.Values[2], decodeErr)
	}
	ledger := &ReleaseLedger{store: store}
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
	retentionRead, err := store.GetMany(ctx, GetManyRequest{Keys: []string{releaseRetentionKey(releaseID)}})
	if err != nil || retentionRead.Values[0] == nil {
		t.Fatalf("retention read = %#v, %v", retentionRead, err)
	}
	retention, err := decodeReleaseRecord[domain.RollbackMaterial](retentionRead.Values[0].Value, "release-retention")
	references := strings.Join(retention.References, "\n")
	if err != nil || strings.Contains(references, requested) || !strings.Contains(references, localImageID) {
		t.Fatalf("rollback material = %#v, %v", retention, err)
	}
	task = completedVersion.Record
	if err := repository.validateBlueprintCandidateTerminalReplay(
		ctx, task, TaskStatusCompleted, terminalRevision,
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
	successorHeadValue, err := encodeTaskReference(successorTaskID)
	if err != nil {
		t.Fatal(err)
	}
	successorSealValue, err := encodeEnvironmentBlueprintSeal(EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: successorTaskID,
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 4,
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
		{
			Type:  MutationPut,
			Key:   environmentBlueprintRootKey(environmentID, successorTaskID),
			Value: successorSealValue,
		},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: successorHeadValue},
	})
	if err != nil || !successorHead.Succeeded {
		t.Fatalf("publish successor Blueprint head = %#v, %v", successorHead, err)
	}
	assertReadOnlyReplay(task, TaskStatusCompleted, successorHead.Revision)

	successorProjection := projection
	successorProjection.RevisionID = successorTaskID
	successorProjection.RenderGeneration = 4
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
