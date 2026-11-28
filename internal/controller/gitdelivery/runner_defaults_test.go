package gitdelivery

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func TestRepositoryRunnerDefaultIsFencedAtAdmissionAndCapturedInRun(t *testing.T) {
	ctx := context.Background()
	service, original, _ := testService(t, &testReleases{})
	store := service.repository.store.(*testStore)
	access := &defaultTestAccess{store: store}
	service.access = access
	access.set(t, "old-repository-label")

	explicit, err := service.Begin(ctx, original.ServiceID, StartInput{IdempotencyKey: "explicit"})
	if err != nil || explicit.Binding.RunnerLabel != original.RunnerLabel {
		t.Fatalf("explicit selector changed: label %q, err %v", explicit.Binding.RunnerLabel, err)
	}
	binding, err := service.GetBinding(ctx, original.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	binding.Binding.InheritRunnerDefault = true
	binding.Binding.RunnerLabel = ""
	if _, err := service.Bind(ctx, binding.Binding, binding.Revision); err != nil {
		t.Fatal(err)
	}
	old, err := service.Begin(ctx, original.ServiceID, StartInput{IdempotencyKey: "first-inherited"})
	if err != nil || old.Binding.RunnerLabel != "old-repository-label" {
		t.Fatalf("initial inheritance: label %q, err %v", old.Binding.RunnerLabel, err)
	}

	access.beforeCapture = func() { access.set(t, "new-repository-label") }
	request := StartInput{IdempotencyKey: "next-inherited"}
	if _, err := service.Begin(ctx, original.ServiceID, request); err == nil {
		t.Fatal("accepted a run after its inherited selector changed")
	}
	next, err := service.Begin(ctx, original.ServiceID, request)
	if err != nil || next.Binding.RunnerLabel != "new-repository-label" {
		t.Fatalf("retry did not select the current default: label %q, err %v", next.Binding.RunnerLabel, err)
	}
	captured, err := service.Get(ctx, original.ServiceID, old.ID)
	if err != nil || captured.Binding.RunnerLabel != "old-repository-label" {
		t.Fatalf("historical run lost its captured selector: label %q, err %v", captured.Binding.RunnerLabel, err)
	}
}

const defaultTestKey = "/v1/fake-runner-default"

type defaultTestAccess struct {
	testAccess
	store         *testStore
	beforeCapture func()
}

func (access *defaultTestAccess) set(t *testing.T, label string) {
	t.Helper()
	if _, err := access.store.Transact(context.Background(), nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: defaultTestKey, Value: []byte(label)},
	}); err != nil {
		t.Fatal(err)
	}
}

func (access *defaultTestAccess) ResolveRunnerDefault(
	context.Context,
	Binding,
) (gitselector.Selector, []keyvalue.Condition, error) {
	current := access.store.lookup(defaultTestKey)
	return gitselector.Selector{
			Label: string(current.Value),
		}, []keyvalue.Condition{
			{Key: defaultTestKey, ModRevision: current.ModRevision},
		}, nil
}

func (access *defaultTestAccess) Capture(ctx context.Context, binding Binding) ([]int64, error) {
	if before := access.beforeCapture; before != nil {
		access.beforeCapture = nil
		before()
	}
	return access.testAccess.Capture(ctx, binding)
}
