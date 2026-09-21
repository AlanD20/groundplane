package etcd

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseTerminalFixture struct {
	store      *releaseTerminalAuditStore
	task       TaskRecord
	assignment testtaskassignments.TaskAssignmentRecord
	result     testtaskjournal.TaskResultRecord
	head       testreleases.ReleaseOperationHead
	agentID    string
	now        time.Time
}

func newReleaseTerminalFixture(t *testing.T, count int) releaseTerminalFixture {
	t.Helper()
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
	planHash := strings.Repeat("a", 64)
	runtimes := make([]executionplan.CandidateRuntime, count)

	members := make([]domain.GroupMember, count)
	fenceMembers := make([]testreleases.ReleaseFenceMember, count)
	steps := make([]testtaskjournal.TaskStepRecord, count*5)
	proxyEvidence := make([]testtaskjournal.TaskProxyEvidence, count)
	mutations := make([]testkeyvalue.Mutation, 0, count*2+4)
	for index := range members {
		serviceID := ids.New(ids.KindService)
		releaseID := ids.New(ids.KindDeployment)
		ordinal := uint32(index + 1)
		members[index] = domain.GroupMember{Ordinal: ordinal, ServiceID: serviceID, ReleaseID: releaseID}
		fenceMembers[index] = testreleases.ReleaseFenceMember{
			ServiceID: serviceID, CandidateReleaseID: releaseID, RenderInputDigest: renderDigest,
		}
		proxyEvidence[index] = testtaskjournal.TaskProxyEvidence{
			ServiceID: serviceID, Target: string(domain.WorkloadBlue), ProxyGeneration: 1,
			ConfigSHA256: strings.Repeat("3", 64), ReleaseID: releaseID,
		}
		runtimes[index] = terminalRuntimeFixture(t, environmentID, serviceID, releaseID, planID, artifactID)
		proxyEvidence[index].ConfigSHA256 = runtimeFixtureHash(runtimes[index])
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
		intentValue, err := testreleases.EncodeReleaseRecord("release-intent", intent)
		if err != nil {
			t.Fatal(err)
		}
		checkpointValue, err := testreleases.EncodeReleaseRecord("release-checkpoint", checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testreleases.ReleaseIntentStagingKey(publicationID, releaseID),
				Value: intentValue,
			},
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testreleases.ReleaseCheckpointStagingKey(publicationID, releaseID),
				Value: checkpointValue,
			},
		)
		for step := range 5 {
			steps[index*5+step] = testtaskjournal.TaskStepRecord{
				Kind: testtaskjournal.TaskStepOperation,
				ID:   ids.New(ids.KindStep),
			}
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
	head := testreleases.ReleaseOperationHead{
		OperationID: operationID, PublicationID: publicationID, EnvironmentID: environmentID,
		ReleaseGroupID: groupID, FailurePolicy: domain.OnFailureLeaveActive, State: domain.StatePending,
		Attempts: []domain.Attempt{{ID: taskID, TaskID: taskID, StartedAt: now}}, Members: members,
		Progress: &progress, LatestTaskID: taskID, ConfiguredTimeoutSeconds: 15 * 60 * 60,
		ComputedBudgetSeconds: 6 * 60 * 60, CreatedAt: now, UpdatedAt: now,
	}
	markerValue, err := testreleases.EncodeReleaseRecord("release-publication", testreleases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID, ManifestDigest: manifestDigest, PublishedAt: now,
		PreparedRuntimes: runtimes,
		CandidateReleaseDescriptor: executionplan.CandidateReleaseDescriptor{
			PlanID:   planID,
			PlanHash: bytes.Repeat([]byte{0xaa}, 32),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	headValue, err := testreleases.EncodeReleaseRecord("release-operation", head)
	if err != nil {
		t.Fatal(err)
	}
	fenceValue, err := testreleases.EncodeReleaseRecord("release-fence-set", testreleases.ReleaseFenceSet{
		EnvironmentID: environmentID, Generation: 1, OperationID: operationID,
		AttemptTaskID: taskID, Group: true, Members: fenceMembers,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations,
		testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleasePublicationKey(publicationID),
			Value: markerValue,
		},
		testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseOperationKey(operationID),
			Value: headValue,
		},
		testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseFenceSetKey(environmentID),
			Value: fenceValue,
		},
		testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environmentID),
			Value: []byte(`{"schema":1}`),
		},
	)
	if seeded, err := store.Transact(ctx, nil, mutations); err != nil || !seeded.Succeeded {
		t.Fatalf("seed release terminal fixture = %#v, %v", seeded, err)
	}

	audited := &releaseTerminalAuditStore{memoryHierarchyStore: store}
	task := TaskRecord{
		ID: taskID, OperationID: operationID,
		Owner: testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
			TenantID:      tenantID,
			ProjectID:     projectID,
			EnvironmentID: environmentID,
		},
		Executor: testtaskjournal.TaskExecutorAgent, PlanID: planID, PlanHash: planHash, RenderGeneration: 1,
		Type: testtaskjournal.TaskDeploy, Target: groupID, Params: map[string]string{testreleaserender.TaskReleasePublicationParam: publicationID}, Steps: steps,
	}
	assignment := testtaskassignments.TaskAssignmentRecord{AssignmentID: ids.New(ids.KindAssignment), ExecutionEpoch: 1}
	agentID := ids.New(ids.KindAgent)

	return releaseTerminalFixture{
		store: audited, task: task, assignment: assignment, head: head, agentID: agentID, now: now,
		result: testtaskjournal.TaskResultRecord{
			Kind:           testtaskjournal.TaskResultCompose,
			Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
			ProxyEvidence:  proxyEvidence,
			ExecutionEpoch: 1,
		},
	}
}

func (f releaseTerminalFixture) finalize(
	t *testing.T,
	status testtaskjournal.TaskStatus,
	guards ...testkeyvalue.Condition,
) (bool, error) {
	t.Helper()
	repository, err := newTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{Keys: []string{testreleases.ReleaseOperationKey(f.head.OperationID)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return repository.finalizeReleaseTaskBatch(
		context.Background(),
		f.task,
		f.assignment,
		status,
		f.result,
		f.agentID,
		f.now.Add(time.Minute),
		read.ReadRevision,
		guards...)
}

// Rationale: a 32-member successful Release stores each runtime atomically with
// its terminal member, resumes after repository restart, and fits both ceilings.
func TestReleaseTerminalizationBatchesMaximumGroupBelowTransactionCeiling(t *testing.T) {
	t.Parallel()
	f := newReleaseTerminalFixture(t, domain.MaximumGroupMembers)
	for transaction := 0; ; transaction++ {
		processed, err := f.finalize(t, testtaskjournal.TaskStatusCompleted)
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			if transaction != 5 {
				t.Fatalf("terminal transaction count=%d, want 5", transaction)
			}
			break
		}
		if transaction > 5 {
			t.Fatal("terminal batching did not finish")
		}
	}
	if f.store.maximumAggregate != 92 {
		t.Fatalf("maximum operations=%d, want 92", f.store.maximumAggregate)
	}
	closed, err := f.store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testreleases.ReleaseOperationKey(f.head.OperationID),
				testreleases.ReleaseFenceSetKey(f.head.EnvironmentID),
			},
		},
	)
	if err != nil || closed.Values[0] == nil || closed.Values[1] != nil {
		t.Fatalf("closed operation=%#v, %v", closed, err)
	}
	head, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseOperationHead](
		closed.Values[0].Value,
		"release-operation",
	)
	if err != nil || head.State != domain.StateCompleted || head.Progress == nil ||
		len(head.Progress.Results) != domain.MaximumGroupMembers {
		t.Fatalf("closed head=%#v, %v", head, err)
	}
	for _, member := range head.Members {
		receipt, err := f.store.Get(context.Background(), serviceruntimerecord.Key(member.ServiceID))
		terminal, terminalErr := f.store.Get(context.Background(), testreleases.ReleaseTerminalKey(member.ReleaseID))
		projection, projectionErr := f.store.Get(
			context.Background(),
			testreleases.ReleaseProjectionKey(member.ServiceID),
		)
		if err != nil || terminalErr != nil || projectionErr != nil || receipt.Entry == nil || terminal.Entry == nil ||
			projection.Entry == nil {
			t.Fatal("missing atomic terminal output")
		}
		if receipt.Entry.ModRevision != terminal.Entry.ModRevision ||
			receipt.Entry.ModRevision != projection.Entry.ModRevision {
			t.Fatal("runtime not atomically published with owning successful member")
		}
		record, err := testreleases.DecodeReleaseRecord[serviceruntimerecord.Record](
			receipt.Entry.Value,
			"service-acknowledged-runtime",
		)
		if err != nil || serviceruntimerecord.Validate(record) != nil || record.Runtime.ReleaseID != member.ReleaseID ||
			record.Source.TaskID != f.task.ID ||
			record.Source.ExecutionEpoch != f.assignment.ExecutionEpoch {
			t.Fatalf("runtime source=%#v, %v", record.Source, err)
		}
	}
}

type releaseTerminalAuditStore struct {
	*memoryHierarchyStore
	maximumAggregate int
	maximumBytes     int
	keyPrefix        string
	raceKey          string
}

func (store *releaseTerminalAuditStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	prefix := "/proof/"
	if store.keyPrefix != "" {
		prefix = store.keyPrefix
	}
	return MeasureTransactionBudget(ctx, prefix, conditions, mutations)
}

func (store *releaseTerminalAuditStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	budget, err := store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	if !budget.Fits() {
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"test terminal transaction exceeds physical budget",
		)
	}
	if budget.Bytes > store.maximumBytes {
		store.maximumBytes = budget.Bytes
	}
	if store.raceKey != "" {
		key := store.raceKey
		store.raceKey = ""
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: []byte("concurrent runtime")}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	aggregate := len(conditions) + len(mutations)
	if aggregate > store.maximumAggregate {
		store.maximumAggregate = aggregate
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
