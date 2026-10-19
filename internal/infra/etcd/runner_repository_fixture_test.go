package etcd

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
)

func assertRunnerAllocationRetained(
	t *testing.T,
	repository *RunnerRepository,
	want testrunners.RunnerRecord,
	allocation runnerallocation.RunnerHostAllocationRecord,
) {
	t.Helper()
	current, err := repository.GetRunner(context.Background(), want.Desired.ID)
	if err != nil || current.Record.Allocation != allocation ||
		current.Record.Desired.ID != want.Desired.ID || current.Record.Desired.OwnerKind != want.Desired.OwnerKind ||
		current.Record.Desired.OwnerID != want.Desired.OwnerID || current.Record.Desired.TenantID != want.Desired.TenantID {
		t.Fatalf("retained Runner = %#v, %v", current, err)
	}
	if _, err := repository.ReadRunnerAllocationEvidence(
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
	tenantID := ids.NewAt(ids.KindTenant, runnerStorageTime(), 700)
	projectID := ids.NewAt(ids.KindProject, runnerStorageTime(), 701)
	if _, err := hierarchy.CreateTenant(context.Background(), testhierarchy.TenantRecord{
		ID: tenantID, Slug: "example", Name: "Example",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if _, err := hierarchy.CreateProject(context.Background(), testhierarchy.ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "application", Name: "Application", Kind: testhierarchy.ProjectKindTenant,
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
	allocationRaceKey   string
	allocationRaceValue []byte
	allocationRaceDone  bool
}

func (store *runnerTransactionAuditStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	result, err := store.memoryHierarchyStore.GetMany(ctx, request)
	if err != nil || store.allocationRaceDone || store.allocationRaceKey == "" ||
		!containsHierarchyKey(request.Keys, store.allocationRaceKey) {
		return result, err
	}
	store.allocationRaceDone = true
	_, err = store.memoryHierarchyStore.Transact(
		ctx,
		[]testkeyvalue.Condition{{Key: store.allocationRaceKey}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: store.allocationRaceKey, Value: store.allocationRaceValue,
		}},
	)
	return result, err
}

func seedRunnerHostSlots(t *testing.T, store *memoryHierarchyStore, slots uint32) {
	t.Helper()
	mutations := make([]testkeyvalue.Mutation, 0, slots)
	for slot := uint32(0); slot < slots; slot++ {
		value, err := testrunners.EncodeRunnerHostSlotRecord(testrunners.RunnerHostSlotRecord{
			Slot: slot, RunnerID: ids.NewAt(ids.KindRunner, runnerStorageTime(), int64(6000+slot)),
		})
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord(%d) error = %v", slot, err)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testrunners.RunnerHostSlotKey(slot),
				Value: value,
			},
		)
	}
	defer testkeyvalue.ClearMutationValues(mutations)
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
	runnerDesiredEntropyBase = int64(800)
	runnerTaskEntropyBase    = int64(900)
)

func runnerTestDesired(
	offset int,
	ownerKind testrunners.RunnerOwnerKind,
	ownerID string,
	tenantID string,
) testrunners.RunnerDesiredRecord {
	runnerID := ids.NewAt(ids.KindRunner, runnerStorageTime(), runnerDesiredEntropyBase+int64(offset))
	return testrunners.RunnerDesiredRecord{
		ID:        runnerID,
		Slug:      "runner-" + strings.ToLower(strings.TrimPrefix(runnerID, "run_")),
		OwnerKind: ownerKind, OwnerID: ownerID, TenantID: tenantID,
		GitHubURL: runnerTestGitHubURL(ownerKind), Labels: []string{"qa-workload"},
		ImageRef: "ghcr.io/groundplane/runner@sha256:" + strings.Repeat("0", 64),
	}
}

func runnerTestGitHubURL(ownerKind testrunners.RunnerOwnerKind) string {
	if ownerKind == testrunners.RunnerOwnerProject || ownerKind == testrunners.RunnerOwnerEnvironment {
		return "https://github.com/aland20/groundplane"
	}
	return "https://github.com/aland20"
}

func runnerTestEnvironment(t *testing.T, store *memoryHierarchyStore, tenantID, projectID string) string {
	t.Helper()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, runnerStorageTime(), 702)
	_, err = hierarchy.CreateEnvironment(context.Background(), testhierarchy.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "ci", NetworkPool: "10.242.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, runnerStorageTime(), 703), CreatedAt: runnerStorageTime(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return environmentID
}

func runnerStorageTime() time.Time {
	return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
}

type runnerCreationFixture struct {
	source RunnerCreationSource
	input  runnerwork.Input
	task   workexecution.Task
	marker testidempotency.IdempotencyMarker
}

// The runtime plan is supplied storage input, not proof of runtime planning or
// host isolation. Allocation and publication are actual repository operations.
func prepareRunnerCreationFixture(t *testing.T, repository *RunnerRepository,
	config runnerallocation.RunnerAllocationConfig, desired testrunners.RunnerDesiredRecord,
	offset int, key string,
) (runnerCreationFixture, error) {
	t.Helper()
	at := runnerStorageTime()
	taskID := ids.NewAt(ids.KindTask, at, runnerTaskEntropyBase+int64(offset))
	source, err := repository.PrepareRunnerCreation(t.Context(), config, desired, taskID, at)
	if err != nil {
		return runnerCreationFixture{}, err
	}
	t.Cleanup(source.Clear)
	record := source.Runner()
	plan := runnerallocation.RuntimePlan{
		RunnerID: desired.ID, TenantID: desired.TenantID,
		OwnerKind: string(desired.OwnerKind), OwnerID: desired.OwnerID,
		RuntimeEpoch: record.RuntimeEpoch + 1, AllocationConfig: config, Allocation: record.Allocation,
		Paths: runnerallocation.RunnerRuntimePaths{RawSocket: runnerallocation.DaemonSocketPath(desired.ID)},
		Container: runnerallocation.RunnerContainer{
			ImageRef: desired.ImageRef, GitHubURL: desired.GitHubURL, Labels: desired.Labels,
		},
	}
	input := runnerwork.Input{TaskID: taskID,
		WorkItemID: ids.NewAt(ids.KindWorkItem, at, int64(offset)),
		PlanID:     ids.NewAt(ids.KindPlan, at, int64(offset)),
		Operation:  runnerallocation.RuntimeOperationCreate, Desired: desired, Plan: plan, CreatedAt: at,
	}
	ref, err := input.Reference()
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor,
		Scope: workexecution.Scope{
			TenantID:      desired.TenantID,
			ProjectID:     desired.ProjectID(),
			EnvironmentID: desired.EnvironmentID(),
		},
		Action: workexecution.ActionCreate, TargetID: desired.ID, TargetName: desired.Slug,
		Intent: ref, ExpectedItems: 1, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(5 * time.Minute),
	}
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = testidempotency.IdempotencyMarkerTask, testidempotency.IdempotencyMarkerPending, taskID
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: desired.IdempotencyScope(), ScopeID: desired.OwnerID,
		Method: http.MethodPost, Route: "/runners", Key: key,
	}
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Response.Status, marker.Response.Body = http.StatusAccepted, []byte(fmt.Sprintf(`{"task_id":%q}`, taskID))
	return runnerCreationFixture{source: source, input: input, task: task, marker: marker}, nil
}

func (fixture runnerCreationFixture) publish(t *testing.T, repository *RunnerRepository) IdempotencyTransactionResult {
	t.Helper()
	result, err := repository.PublishRunnerCreation(
		t.Context(),
		fixture.source,
		fixture.input,
		fixture.task,
		fixture.marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
