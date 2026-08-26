package etcd

import (
	"context"
	"net/netip"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
)

func assertRunnerAllocationRetained(
	t *testing.T,
	repository *RunnerRepository,
	want RunnerRecord,
	allocation runnerallocation.RunnerHostAllocationRecord,
) {
	t.Helper()
	current, err := repository.GetRunner(context.Background(), want.Desired.ID)
	if err != nil || current.Record.Allocation != allocation ||
		current.Record.Desired.ID != want.Desired.ID || current.Record.Desired.OwnerKind != want.Desired.OwnerKind ||
		current.Record.Desired.OwnerID != want.Desired.OwnerID || current.Record.Desired.TenantID != want.Desired.TenantID {
		t.Fatalf("retained Runner = %#v, %v", current, err)
	}
	if _, err := repository.readRunnerAllocationEvidence(
		context.Background(), current.Record, current.ReadRevision,
	); err != nil {
		t.Fatalf("readRunnerAllocationEvidence(retained) error = %v", err)
	}
}

func newRunnerRepositoryFixture(
	t *testing.T,
) (*memoryHierarchyStore, *RunnerRepository, string, string) {
	t.Helper()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, taskJournalTime(), 700)
	projectID := ids.NewAt(ids.KindProject, taskJournalTime(), 701)
	if _, err := hierarchy.CreateTenant(context.Background(), TenantRecord{
		ID: tenantID, Slug: "example", Name: "Example",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if _, err := hierarchy.CreateProject(context.Background(), ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "application", Name: "Application", Kind: ProjectKindTenant,
	}); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	repository, err := newRunnerRepository(store)
	if err != nil {
		t.Fatalf("newRunnerRepository() error = %v", err)
	}
	if err := repository.EnsureRunnerNetworkPool(context.Background(), runnerTestAllocationConfig()); err != nil {
		t.Fatalf("EnsureRunnerNetworkPool() error = %v", err)
	}
	return store, repository, tenantID, projectID
}

type runnerTransactionAuditStore struct {
	*memoryHierarchyStore
	maximumOperations   int
	allocationRaceKey   string
	allocationRaceValue []byte
	allocationRaceDone  bool
	terminalRunnerID    string
	terminalRaceKey     string
	terminalRaceDone    bool
}

func (store *runnerTransactionAuditStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	result, err := store.memoryHierarchyStore.GetMany(ctx, request)
	if err != nil || store.allocationRaceDone || store.allocationRaceKey == "" ||
		!containsHierarchyKey(request.Keys, store.allocationRaceKey) {
		return result, err
	}
	store.allocationRaceDone = true
	_, err = store.memoryHierarchyStore.Transact(ctx, []Condition{{Key: store.allocationRaceKey}}, []Mutation{{
		Type: MutationPut, Key: store.allocationRaceKey, Value: store.allocationRaceValue,
	}})
	return result, err
}

func (store *runnerTransactionAuditStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	operations := len(conditions) + len(mutations)
	if operations > store.maximumOperations {
		store.maximumOperations = operations
	}
	if !store.terminalRaceDone && store.terminalRaceKey != "" &&
		runnerMutationsTerminalize(mutations, store.terminalRunnerID) {
		store.terminalRaceDone = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
			Type: MutationDelete, Key: store.terminalRaceKey,
		}}); err != nil {
			return TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func runnerMutationsTerminalize(mutations []Mutation, runnerID string) bool {
	for _, mutation := range mutations {
		if mutation.Type != MutationPut || mutation.Key != runnerLifecycleKey(runnerID) {
			continue
		}
		record, err := decodeRunnerLifecycleRecord(mutation.Value)
		if err == nil && record.ProvisioningState != RunnerProvisioningProvisioning {
			return true
		}
	}
	return false
}

func seedRunnerHostSlots(t *testing.T, store *memoryHierarchyStore, slots uint32) {
	t.Helper()
	mutations := make([]Mutation, 0, slots)
	for slot := uint32(0); slot < slots; slot++ {
		value, err := encodeRunnerHostSlotRecord(RunnerHostSlotRecord{
			Slot: slot, RunnerID: ids.NewAt(ids.KindRunner, taskJournalTime(), int64(6000+slot)),
		})
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord(%d) error = %v", slot, err)
		}
		mutations = append(mutations, Mutation{Type: MutationPut, Key: runnerHostSlotKey(slot), Value: value})
	}
	defer clearMutationValues(mutations)
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Runner host slots = %#v, %v", result, err)
	}
}

func runnerTestAllocationConfig() runnerallocation.RunnerAllocationConfig {
	return runnerallocation.RunnerAllocationConfig{
		SystemPool: netip.MustParsePrefix("10.128.0.0/9"),
		RunnerPool: netip.MustParsePrefix("10.240.0.0/16"),
		HostPool: runnerallocation.RunnerHostPoolConfig{
			HostUIDStart: 200000, HostUIDEnd: 200007,
			SubUIDStart: 300000, SubUIDEnd: 824287,
			SubGIDStart: 900000, SubGIDEnd: 1424287,
		},
	}
}

func runnerTestAllocationConfigWithSlots(slots uint32) runnerallocation.RunnerAllocationConfig {
	config := runnerTestAllocationConfig()
	config.HostPool.HostUIDEnd = config.HostPool.HostUIDStart + slots - 1
	config.HostPool.SubUIDEnd = config.HostPool.SubUIDStart + slots*runnerallocation.RunnerSubordinateBlockSize - 1
	config.HostPool.SubGIDEnd = config.HostPool.SubGIDStart + slots*runnerallocation.RunnerSubordinateBlockSize - 1
	return config
}

const (
	runnerDesiredEntropyBase   = int64(800)
	runnerTaskEntropyBase      = int64(900)
	runnerOperationEntropyBase = int64(1000)
)

func runnerTestDesired(
	offset int,
	ownerKind RunnerOwnerKind,
	ownerID string,
	tenantID string,
) RunnerDesiredRecord {
	runnerID := ids.NewAt(ids.KindRunner, taskJournalTime(), runnerDesiredEntropyBase+int64(offset))
	return RunnerDesiredRecord{
		ID:        runnerID,
		Slug:      "runner-" + strings.ToLower(strings.TrimPrefix(runnerID, "run_")),
		OwnerKind: ownerKind, OwnerID: ownerID, TenantID: tenantID,
		GitHubURL: runnerTestGitHubURL(ownerKind), Labels: []string{"qa-workload"},
		ImageRef: RunnerImageRef + strings.Repeat("0", 64),
	}
}

func runnerTestGitHubURL(ownerKind RunnerOwnerKind) string {
	if ownerKind == RunnerOwnerProject {
		return "https://github.com/aland20/groundplane"
	}
	return "https://github.com/aland20"
}

func runnerTestTask(desired RunnerDesiredRecord, taskType TaskType, offset int, key string) TaskRecord {
	task := validTaskRecord(taskJournalTime())
	owner := TaskOwner{WorkspaceType: TaskWorkspaceTenant, TenantID: desired.TenantID}
	if desired.OwnerKind == RunnerOwnerProject {
		owner.ProjectID = desired.OwnerID
	}
	task.ID = ids.NewAt(ids.KindTask, taskJournalTime(), runnerTaskEntropyBase+int64(offset))
	task.OperationID = ids.NewAt(ids.KindOperation, taskJournalTime(), runnerOperationEntropyBase+int64(offset))
	task.Executor = TaskExecutorController
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.Type = taskType
	task.Target = desired.ID
	task.IdempotencyKey = key
	task.Params = map[string]string{TaskResourceKindParam: TaskResourceRunner}
	if taskType == TaskCreate {
		task.Params[RunnerRegistrationTokenPresentParam] = "true"
	}
	return task
}

func runnerTestMarker(task TaskRecord, desired RunnerDesiredRecord) IdempotencyMarker {
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeKind = IdempotencyScopeTenant
	if desired.OwnerKind == RunnerOwnerProject {
		marker.Locator.ScopeKind = IdempotencyScopeProject
	}
	marker.Locator.ScopeID = desired.OwnerID
	marker.Locator.Route = "/runners"
	if task.Type == TaskRemove {
		marker.Locator.Method = "DELETE"
		marker.Locator.Route = "/runners/{id}"
		target := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRunner, ID: desired.ID}
		marker.ReplayTarget = &target
	} else if task.RetryOf != "" {
		marker.Locator.Route = "/runners/{id}/retry"
	}
	marker.Locator.Key = task.IdempotencyKey
	return marker
}
