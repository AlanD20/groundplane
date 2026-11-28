package gitdelivery

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestAmbiguousDispatchAndNativeAdmissionResumeTheSameTasks(t *testing.T) {
	ctx := context.Background()
	id := ids.New(ids.KindTask)
	native := &testReleases{fetchID: ids.New(ids.KindTask), deployID: id}
	service, binding, github := testService(t, native)
	run, err := service.Begin(ctx, binding.ServiceID, StartInput{IdempotencyKey: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Advance(ctx, run.ID); !errs.IsRetryable(err) {
		t.Fatalf("lost dispatch response = %v", err)
	}
	for _, expected := range []Phase{Building, AwaitingApproval} {
		step, err := service.Advance(ctx, run.ID)
		if err != nil || step.Phase != expected {
			t.Fatalf("advance to %s: got %s, %v", expected, step.Phase, err)
		}
	}
	if github.dispatches != 1 || native.fetches != 0 {
		t.Fatalf("dispatches = %d, fetches before approval = %d", github.dispatches, native.fetches)
	}
	if _, err := service.Approve(ctx, binding.ServiceID, run.ID); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []Phase{Fetching, DeployPending, Deploying, Succeeded} {
		step, err := service.Advance(ctx, run.ID)
		if err != nil || step.Phase != expected {
			t.Fatalf("advance to %s: got %s, %v", expected, step.Phase, err)
		}
	}
	final, err := service.Get(ctx, binding.ServiceID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.DeployTaskID != id || final.ReleaseID == "" || native.deploys != 1 ||
		native.input.Policy.Strategy != release.StrategyBlueGreen || native.input.Digest != testDigest ||
		native.input.ImageRepository != run.ImageRepository() {
		t.Fatalf(
			"deployment lost captured evidence: release %s, deploys %d, input %+v",
			final.ReleaseID,
			native.deploys,
			native.input,
		)
	}
}

func TestSupersededBuildNeverFetchesItsImage(t *testing.T) {
	ctx := context.Background()
	native := &testReleases{}
	service, binding, _ := testService(t, native)
	earlier, err := service.Begin(ctx, binding.ServiceID, StartInput{IdempotencyKey: "earlier"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Advance(ctx, earlier.ID); !errs.IsRetryable(err) {
		t.Fatalf("lost dispatch response = %v", err)
	}
	if _, err := service.Begin(ctx, binding.ServiceID, StartInput{IdempotencyKey: "later"}); err != nil {
		t.Fatal(err)
	}
	result, err := service.Advance(ctx, earlier.ID)
	if err != nil || result.Phase != Superseded || native.fetches != 0 {
		t.Fatalf(
			"superseded build entered activation: phase %s, fetches %d, error %v",
			result.Phase,
			native.fetches,
			err,
		)
	}
}

var testCommit = strings.Repeat("a", 40)
var testDigest = "sha256:" + strings.Repeat("b", 64)

func testService(t *testing.T, native *testReleases) (*Service, Binding, *testGitHub) {
	t.Helper()
	store := &testStore{revision: 1, entries: make(map[string]keyvalue.KeyValue)}
	github := &testGitHub{}
	service, err := NewService(&Repository{store: store}, github, testAccess{}, native)
	if err != nil {
		t.Fatal(err)
	}
	target := Target{
		TenantID: ids.New(ids.KindTenant), ProjectID: ids.New(ids.KindProject),
		EnvironmentID: ids.New(ids.KindEnvironment), ServiceID: ids.New(ids.KindService),
	}
	binding := Binding{
		Target: target, ConnectionID: "connection", ConnectionScopeKind: ids.KindEnvironment,
		ConnectionScopeID: target.EnvironmentID, ConnectionGeneration: 1, InstallationID: 10,
		RepositoryID: 11, WorkflowID: 12, WorkflowRef: "main", WorkflowCommit: testCommit,
		Branch: "main", Dockerfile: "Dockerfile", BuildContext: ".", RunnerLabel: "build", RequireApproval: true,
	}
	if _, err := service.Bind(context.Background(), binding, 0); err != nil {
		t.Fatal(err)
	}
	return service, binding, github
}

type testAccess struct{}

func (testAccess) Capture(context.Context, Binding) ([]int64, error) { return []int64{22}, nil }

func (testAccess) Recheck(context.Context, Binding, int64) ([]keyvalue.Condition, error) {
	return []keyvalue.Condition{{Key: "/v1/deleted"}}, nil
}

type testGitHub struct {
	commit     string
	dispatches int
}

func (github *testGitHub) ResolveSource(context.Context, Binding) (string, error) {
	if github.commit != "" {
		return github.commit, nil
	}
	return testCommit, nil
}
func (*testGitHub) VerifyWorkflow(context.Context, Binding) error { return nil }
func (github *testGitHub) Dispatch(context.Context, Run) (WorkflowRun, error) {
	github.dispatches++
	return WorkflowRun{}, errs.New(errs.KindRequestUnavailable, "response lost")
}

func (github *testGitHub) FindDispatch(ctx context.Context, run Run) (WorkflowRun, bool, error) {
	observed, err := github.Inspect(ctx, run)
	return observed, true, err
}

func (*testGitHub) Inspect(_ context.Context, run Run) (WorkflowRun, error) {
	return WorkflowRun{
		ID: 33, WorkflowID: run.Binding.WorkflowID, Attempt: 1, HeadSHA: testCommit,
		Event: "workflow_dispatch", Title: BuildJobName + " " + run.ID, State: WorkflowSucceeded,
	}, nil
}

func (*testGitHub) BuildResult(_ context.Context, run Run) (BuildEvidence, error) {
	return BuildEvidence{
		RunID:        33,
		RunAttempt:   1,
		RunnerID:     22,
		RequestID:    run.ID,
		SourceCommit: testCommit,
		ImageDigest:  testDigest,
	}, nil
}

type testReleases struct {
	input             DeployInput
	fetchID, deployID string
	fetches, deploys  int
}

func (*testReleases) Policy(context.Context, Target) (RolloutPolicy, []keyvalue.Condition, error) {
	return RolloutPolicy{Strategy: release.StrategyBlueGreen, OnFailure: release.OnFailureLeaveActive}, nil, nil
}

func (native *testReleases) Fetch(context.Context, FetchInput) (string, error) {
	native.fetches++
	return native.fetchID, nil
}

func (native *testReleases) Deploy(_ context.Context, input DeployInput) (string, error) {
	native.deploys++
	native.input = input
	return "", errs.New(errs.KindStorageUnavailable, "accepted response lost")
}

func (native *testReleases) DeployReceipt(context.Context, DeployInput) (string, keyvalue.Condition, error) {
	return native.deployID, keyvalue.Condition{}, nil
}

func (*testReleases) Task(_ context.Context, input TaskInput) (TaskResult, error) {
	if input.Kind == TaskDeploy {
		return TaskResult{State: TaskSucceeded, ReleaseID: ids.New(ids.KindDeployment)}, nil
	}
	return TaskResult{State: TaskSucceeded}, nil
}

// This store models compare-and-swap at a single current revision. The tests
// drive one worker at a time and do not present it as an MVCC pagination fake.
type testStore struct {
	revision int64
	entries  map[string]keyvalue.KeyValue
}

func (store *testStore) lookup(key string) *keyvalue.KeyValue {
	value, exists := store.entries[key]
	if !exists {
		return nil
	}
	return &value
}

func (store *testStore) Get(_ context.Context, key string) (*keyvalue.GetResult, error) {
	return &keyvalue.GetResult{Entry: store.lookup(key), ReadRevision: store.revision}, nil
}

func (store *testStore) GetMany(_ context.Context, req keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	if req.Revision != 0 && req.Revision != store.revision {
		return nil, errs.New(errs.KindInternal, "unexpected historical fixture read")
	}
	values := make([]*keyvalue.KeyValue, len(req.Keys))
	for i, key := range req.Keys {
		values[i] = store.lookup(key)
	}
	return &keyvalue.GetManyResult{Values: values, ReadRevision: store.revision}, nil
}

func (*testStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errs.New(errs.KindInternal, "unexpected fixture range")
}

func (store *testStore) Transact(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	result := keyvalue.TransactionResult{Revision: store.revision}
	for _, guard := range conditions {
		if guard.Prefix || revision(store.lookup(guard.Key)) != guard.ModRevision {
			return result, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type == keyvalue.MutationDelete {
			delete(store.entries, mutation.Key)
		} else {
			store.entries[mutation.Key] = keyvalue.KeyValue{Key: mutation.Key, Value: mutation.Value, ModRevision: store.revision}
		}
	}
	return keyvalue.TransactionResult{Revision: store.revision, Succeeded: true}, nil
}
