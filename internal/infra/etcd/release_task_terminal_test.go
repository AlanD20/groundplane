package etcd

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func TestReleaseTerminalizationBatchesMaximumGroupBelowTransactionCeiling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	publicationID := ids.NewULID()
	operationID := ids.New(ids.KindOperation)
	taskID := ids.New(ids.KindTask)
	planID := ids.New(ids.KindPlan)
	tenantID := ids.New(ids.KindTenant)
	projectID := ids.New(ids.KindProject)
	environmentID := ids.New(ids.KindEnvironment)
	groupID := ids.New(ids.KindReleaseGroup)
	artifactID := ids.New(ids.KindConfig)
	manifestDigest := strings.Repeat("1", 64)
	renderDigest := strings.Repeat("2", 64)

	members := make([]domain.GroupMember, domain.MaximumGroupMembers)
	fenceMembers := make([]ReleaseFenceMember, domain.MaximumGroupMembers)
	steps := make([]TaskStepRecord, domain.MaximumGroupMembers*5)
	proxyEvidence := make([]TaskProxyEvidence, domain.MaximumGroupMembers)
	mutations := make([]Mutation, 0, domain.MaximumGroupMembers*2+4)
	for index := range members {
		serviceID := ids.New(ids.KindService)
		releaseID := ids.New(ids.KindDeployment)
		ordinal := uint32(index + 1)
		members[index] = domain.GroupMember{Ordinal: ordinal, ServiceID: serviceID, ReleaseID: releaseID}
		fenceMembers[index] = ReleaseFenceMember{
			ServiceID: serviceID, CandidateReleaseID: releaseID, RenderInputDigest: renderDigest,
		}
		proxyEvidence[index] = TaskProxyEvidence{
			ServiceID: serviceID, Target: string(domain.WorkloadBlue), ProxyGeneration: 1,
			ConfigSHA256: strings.Repeat("3", 64), ReleaseID: releaseID,
		}
		intent := domain.Intent{
			ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID,
			OperationID: operationID, OperationKind: domain.OperationDeploy,
			GroupOperationID: operationID, GroupMemberOrdinal: ordinal,
			CandidateWorkload: releaseTestWorkloadSeal(
				"docker.io/library/nginx:1",
			), Tag: "1", Strategy: domain.StrategyBlueGreen, Slot: domain.SlotBlue,
			OnFailure: domain.OnFailureLeaveActive, RenderInputID: artifactID, RenderInputDigest: renderDigest,
			CreatedAt: now, Actor: "operator", OriginatingTaskID: taskID,
			Workspace: domain.Workspace{
				Kind: domain.WorkspaceTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID,
			},
		}
		if err := domain.ValidateIntent(intent); err != nil {
			t.Fatalf("ValidateIntent(%d) error = %v", index, err)
		}
		checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: now}
		intentValue, err := encodeReleaseRecord("release-intent", intent)
		if err != nil {
			t.Fatal(err)
		}
		checkpointValue, err := encodeReleaseRecord("release-checkpoint", checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		mutations = append(
			mutations,
			Mutation{Type: MutationPut, Key: releaseIntentStagingKey(publicationID, releaseID), Value: intentValue},
			Mutation{
				Type:  MutationPut,
				Key:   releaseCheckpointStagingKey(publicationID, releaseID),
				Value: checkpointValue,
			},
		)
		for step := range 5 {
			steps[index*5+step] = TaskStepRecord{Kind: TaskStepOperation, ID: ids.New(ids.KindStep)}
		}
	}
	sort.Slice(proxyEvidence, func(left int, right int) bool {
		return proxyEvidence[left].ServiceID < proxyEvidence[right].ServiceID
	})
	executor, err := domain.NewGroupExecutor(domain.GroupManifest{
		OperationID: operationID, ReleaseGroupID: groupID, EnvironmentID: environmentID,
		FailurePolicy: domain.OnFailureLeaveActive, Members: members,
		ConfiguredTimeoutSeconds: 15 * 60 * 60, ComputedBudgetSeconds: 6 * 60 * 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	progress, err := executor.Begin(taskID, now)
	if err != nil {
		t.Fatal(err)
	}
	head := ReleaseOperationHead{
		OperationID: operationID, PublicationID: publicationID, EnvironmentID: environmentID,
		ReleaseGroupID: groupID, FailurePolicy: domain.OnFailureLeaveActive, State: domain.StatePending,
		Attempts: []domain.Attempt{{ID: taskID, TaskID: taskID, StartedAt: now}}, Members: members,
		Progress: &progress, LatestTaskID: taskID, ConfiguredTimeoutSeconds: 15 * 60 * 60,
		ComputedBudgetSeconds: 6 * 60 * 60, CreatedAt: now, UpdatedAt: now,
	}
	markerValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID, ManifestDigest: manifestDigest, PublishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	headValue, err := encodeReleaseRecord("release-operation", head)
	if err != nil {
		t.Fatal(err)
	}
	fenceValue, err := encodeReleaseRecord("release-fence-set", ReleaseFenceSet{
		EnvironmentID: environmentID, Generation: 1, OperationID: operationID,
		AttemptTaskID: taskID, Group: true, Members: fenceMembers,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: releasePublicationKey(publicationID), Value: markerValue},
		Mutation{Type: MutationPut, Key: releaseOperationKey(operationID), Value: headValue},
		Mutation{Type: MutationPut, Key: releaseFenceSetKey(environmentID), Value: fenceValue},
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: []byte(`{"schema":1}`)},
	)
	if seeded, err := store.Transact(ctx, nil, mutations); err != nil || !seeded.Succeeded {
		t.Fatalf("seed release terminal fixture = %#v, %v", seeded, err)
	}

	audited := &releaseTerminalAuditStore{memoryHierarchyStore: store}
	repository, err := newTaskRepository(audited)
	if err != nil {
		t.Fatal(err)
	}
	task := TaskRecord{
		ID: taskID, OperationID: operationID,
		Owner: TaskOwner{
			WorkspaceType: TaskWorkspaceTenant,
			TenantID:      tenantID,
			ProjectID:     projectID,
			EnvironmentID: environmentID,
		},
		Executor: TaskExecutorAgent, PlanID: planID, RenderGeneration: 1,
		Type: TaskDeploy, Target: groupID, Params: map[string]string{TaskReleasePublicationParam: publicationID}, Steps: steps,
	}
	assignment := TaskAssignmentRecord{AssignmentID: ids.New(ids.KindAssignment)}
	agentID := ids.New(ids.KindAgent)
	for transaction := 0; ; transaction++ {
		read, readErr := audited.GetMany(ctx, GetManyRequest{Keys: []string{releaseOperationKey(operationID)}})
		if readErr != nil {
			t.Fatal(readErr)
		}
		processed, finalizeErr := repository.finalizeReleaseTaskBatch(
			ctx,
			task,
			assignment,
			TaskStatusCompleted,
			TaskResultRecord{
				Kind:          TaskResultCompose,
				Diagnostic:    TaskResultDiagnosticNone,
				ProxyEvidence: proxyEvidence,
			},
			agentID,
			now.Add(time.Minute),
			read.ReadRevision,
		)
		if finalizeErr != nil {
			t.Fatalf("finalizeReleaseTaskBatch(%d) error = %v", transaction, finalizeErr)
		}
		if !processed {
			if transaction != 5 {
				t.Fatalf("terminal transaction count = %d, want 5", transaction)
			}
			break
		}
	}
	if audited.maximumAggregate != 94 {
		t.Fatalf("maximum terminal aggregate operations = %d, want 94", audited.maximumAggregate)
	}
	closed, err := audited.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseOperationKey(operationID), releaseFenceSetKey(environmentID),
	}})
	if err != nil || closed.Values[0] == nil || closed.Values[1] != nil {
		t.Fatalf("closed release operation = %#v, %v", closed, err)
	}
	closedHead, err := decodeReleaseRecord[ReleaseOperationHead](closed.Values[0].Value, "release-operation")
	if err != nil || closedHead.State != domain.StateCompleted || closedHead.Progress == nil ||
		len(closedHead.Progress.Results) != domain.MaximumGroupMembers {
		t.Fatalf("closed Release head = %#v, %v", closedHead, err)
	}
}

type releaseTerminalAuditStore struct {
	*memoryHierarchyStore
	maximumAggregate int
}

func (store *releaseTerminalAuditStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	aggregate := len(conditions) + len(mutations)
	if aggregate > store.maximumAggregate {
		store.maximumAggregate = aggregate
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
