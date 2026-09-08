package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: recovery retry is a ledger transition, not an ordinary Task
// clone. It must preserve immutable intent/render lineage and transfer the
// Environment fence atomically to exactly one fresh attempt.
func TestPrepareReleaseTaskRetryTransfersSealedLineageOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	publicationID := ids.NewULID()
	operationID := ids.NewAt(ids.KindOperation, now, 1)
	sourceTaskID := ids.NewAt(ids.KindTask, now, 2)
	retryTaskID := ids.NewAt(ids.KindTask, now.Add(time.Minute), 3)
	releaseID := ids.NewAt(ids.KindDeployment, now, 4)
	serviceID := ids.NewAt(ids.KindService, now, 5)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 6)
	tenantID := ids.NewAt(ids.KindTenant, now, 7)
	projectID := ids.NewAt(ids.KindProject, now, 8)
	planID := ids.NewAt(ids.KindPlan, now, 9)
	artifactID := ids.NewAt(ids.KindConfig, now, 10)
	priorArtifactID := ids.NewAt(ids.KindConfig, now, 12)
	hook := scriptCheckpointTestRecord(now)
	hook.CurrentTaskID, hook.OperationID = sourceTaskID, operationID
	hook.StepID, hook.ReleaseID = ids.NewAt(ids.KindStep, now, 13), releaseID
	hook.EnvironmentID, hook.ServiceID = environmentID, serviceID
	hook.PlanHash = strings.Repeat("a", 64)

	render := ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID, PriorArtifactID: priorArtifactID, ServiceID: serviceID, ServiceName: "api",
		CandidateWorkload: releaseTestWorkloadSeal(
			"registry.example/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		),
		PriorWorkload: releaseTestPriorWorkload("registry.example/api:previous"), Strategy: domain.StrategyRecreate,
		PriorStrategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton,
		PriorTarget: domain.WorkloadSingleton,
		TenantID:    tenantID, TenantSlug: "tenant", ProjectID: projectID, ProjectSlug: "project",
		EnvironmentID: environmentID, EnvironmentName: "production", AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
		Projection: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 11), RenderGeneration: 1,
			DesiredServices: []EnvironmentServiceProjection{{
				EnvironmentID: environmentID,
				Desired: core.Service{
					ID:       serviceID,
					Name:     "api",
					Image:    "registry.example/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Strategy: core.StrategyRecreate,
				},
			}},
			ServiceDependencyPlans: core.ServiceDependencyPlans{},
		},
	}
	render.Projection = withTestEnvironmentComposeArtifact(render.Projection)
	rawRender, err := EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatalf("EncodeReleaseRenderInput() error = %v", err)
	}
	renderDigest, err := domain.Digest(rawRender)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID, OperationID: operationID,
		OperationKind: domain.OperationDeploy, CandidateWorkload: render.CandidateWorkload, Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureLeaveActive, RenderInputID: artifactID, RenderInputDigest: renderDigest,
		CreatedAt: now, Actor: "operator", OriginatingTaskID: sourceTaskID,
		Workspace: domain.Workspace{
			Kind:          domain.WorkspaceTenant,
			TenantID:      tenantID,
			ProjectID:     projectID,
			EnvironmentID: environmentID,
		},
	}
	intentDigest, err := domain.Digest(intent)
	if validationErr := domain.ValidateIntent(intent); validationErr != nil {
		t.Fatalf("invalid retry intent fixture: %v", validationErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StateRecoveryRequired, UpdatedAt: now}
	checkpointDigest, err := domain.Digest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	forwardStepID := ids.NewAt(ids.KindStep, now, 14)
	probeStepID := ids.NewAt(ids.KindStep, now, 15)
	compensateStepID := ids.NewAt(ids.KindStep, now, 16)
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
			ForwardStepIDs: []string{forwardStepID},
			ServingPredecessor: &executionplan.ServingPredecessorInput{
				ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
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
		PlanID: planID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, ProcedureBytes: procedureBytes,
	}
	manifest := ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID,
		Members: []ReleaseStagedMemberRef{{
			ReleaseID: releaseID, ServiceID: serviceID, IntentDigest: intentDigest,
			RenderDigest: renderDigest, CheckpointDigest: checkpointDigest,
		}}, CreatedAt: now,
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	member := domain.GroupMember{Ordinal: 1, ServiceID: serviceID, ReleaseID: releaseID}
	records := []struct {
		key      string
		typeName string
		value    any
	}{
		{releasePublicationKey(publicationID), "release-publication", ReleasePublicationMarker{
			PublicationID: publicationID, OperationID: operationID, ManifestDigest: manifest.Digest,
			CandidateReleaseDescriptor: descriptor, PublishedAt: now,
		}},
		{releaseManifestStagingKey(publicationID), "release-staged-manifest", manifest},
		{releaseOperationKey(operationID), "release-operation", ReleaseOperationHead{
			OperationID: operationID, PublicationID: publicationID, EnvironmentID: environmentID,
			FailurePolicy: domain.OnFailureLeaveActive, State: domain.StateRecoveryRequired,
			RecoveryOutcome: domain.StateFailed, FailedMemberOrdinal: 1,
			Attempts: []domain.Attempt{
				{ID: sourceTaskID, TaskID: sourceTaskID, StartedAt: now},
			}, Members: []domain.GroupMember{member},
			LatestTaskID: sourceTaskID, ConfiguredTimeoutSeconds: 900, ComputedBudgetSeconds: 1200, CreatedAt: now, UpdatedAt: now,
		}},
		{releaseFenceSetKey(environmentID), "release-fence-set", ReleaseFenceSet{
			EnvironmentID: environmentID, Generation: 1, OperationID: operationID, AttemptTaskID: sourceTaskID,
			Members: []ReleaseFenceMember{
				{ServiceID: serviceID, CandidateReleaseID: releaseID, RenderInputDigest: renderDigest},
			},
		}},
		{releaseIntentStagingKey(publicationID, releaseID), "release-intent", intent},
		{releaseRenderInputStagingKey(publicationID, releaseID), "release-render-input", rawRender},
		{scriptExecutionKey(hook.ID), "script-execution", hook},
	}
	mutations := make([]Mutation, 0, len(records)+1)
	for _, record := range records {
		value, encodeErr := encodeReleaseRecord(record.typeName, record.value)
		if encodeErr != nil {
			t.Fatalf("encode %s error = %v", record.typeName, encodeErr)
		}
		mutations = append(mutations, Mutation{Type: MutationPut, Key: record.key, Value: value})
	}
	mutations = append(
		mutations,
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1}`)},
	)
	seeded, err := store.Transact(ctx, nil, mutations)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed release retry = %#v, %v", seeded, err)
	}
	read, err := store.GetMany(ctx, GetManyRequest{Keys: []string{releaseOperationKey(operationID)}})
	if err != nil {
		t.Fatal(err)
	}
	source := TaskRecord{
		ID: sourceTaskID, OperationID: operationID,
		Owner: TaskOwner{
			WorkspaceType: TaskWorkspaceTenant,
			TenantID:      tenantID,
			ProjectID:     projectID,
			EnvironmentID: environmentID,
		},
		Executor: TaskExecutorAgent, PlanID: planID, Type: TaskDeploy,
		PlanHash: hook.PlanHash, Params: map[string]string{
			TaskComposeArtifactParam:    artifactID,
			TaskReleasePublicationParam: publicationID, ReleaseHookStepExecutionParam(hook.StepID): hook.ID,
		}, Steps: []TaskStepRecord{
			{Kind: TaskStepOperation, ID: forwardStepID},
			{Kind: TaskStepOperation, ID: hook.StepID},
			{Kind: TaskStepOperation, ID: probeStepID},
			{Kind: TaskStepOperation, ID: compensateStepID},
		},
		Result: &TaskResultRecord{Kind: TaskResultCompose, ReconciliationRequired: true}, CreatedAt: now,
	}
	retry := cloneTaskRecord(source)
	retry.ID, retry.RetryOf, retry.CreatedAt = retryTaskID, sourceTaskID, now.Add(time.Minute)
	if _, err := validateReleaseCandidateDescriptor(descriptor, source, manifest); err != nil {
		t.Fatalf("invalid retry descriptor fixture: %v", err)
	}
	if _, err := decodeReleaseRenderInput(rawRender); err != nil {
		t.Fatalf("invalid retry render fixture: %v", err)
	}
	change, err := repository.prepareReleaseTaskRetry(ctx, source, retry, read.ReadRevision)
	if err != nil {
		t.Fatalf("prepareReleaseTaskRetry() error = %v", err)
	}
	defer change.clear()
	if !change.applies || len(change.conditions) != 8 || len(change.mutations) != 4 {
		t.Fatalf("release retry change = %#v", change)
	}
	transaction, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("apply release retry = %#v, %v", transaction, err)
	}
	replay, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || replay.Succeeded {
		t.Fatalf("replay release retry = %#v, %v", replay, err)
	}
	updated, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseOperationKey(operationID), releaseFenceSetKey(environmentID), scriptExecutionKey(hook.ID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	head, err := decodeReleaseRecord[ReleaseOperationHead](updated.Values[0].Value, "release-operation")
	if err != nil {
		t.Fatal(err)
	}
	fence, err := decodeReleaseRecord[ReleaseFenceSet](updated.Values[1].Value, "release-fence-set")
	if err != nil {
		t.Fatal(err)
	}
	if head.State != domain.StateRecovering || head.LatestTaskID != retryTaskID || len(head.Attempts) != 2 ||
		head.Attempts[1].RetryOf != sourceTaskID || fence.Generation != 2 || fence.AttemptTaskID != retryTaskID {
		t.Fatalf("retry head=%#v fence=%#v", head, fence)
	}
	transferred, err := decodeEnvelope[ScriptExecutionRecord](updated.Values[2].Value, "script-execution")
	if err != nil || transferred.CurrentTaskID != retryTaskID || !transferred.ActiveReference ||
		!transferred.UpdatedAt.Equal(retry.CreatedAt) {
		t.Fatalf("retry hook ownership = %#v, %v", transferred, err)
	}
}
