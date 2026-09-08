package etcd

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ManualScriptAdmissionFixture supplies a hermetic source snapshot to the real
// Controller plan producer. Release history is seeded, not deployed: this proves
// plan production/publication/admission, not serving-Release source discovery.
type ManualScriptAdmissionFixture struct {
	Sources   ScriptExecutionSources
	Task      TaskRecord
	Execution ScriptExecutionRecord
	Scripts   *ScriptRepository
	store     *memoryHierarchyStore
	marker    IdempotencyMarker
}

func NewManualScriptAdmissionFixture(t *testing.T) *ManualScriptAdmissionFixture {
	t.Helper()
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	// Use an explicit isolated runner with no physical Network or Volume inputs.
	sources.RenderInput.Record.ServiceName = sources.Service.Record.Desired.Name
	sources.RenderInput.Record.CandidateWorkload = sources.Release.Intent.CandidateWorkload
	compose := []byte("services:\n  api:\n    image: app:latest\n    user: '1000:1000'\n    networks: {}\n")
	sources.RenderInput.Record.Projection.NormalizedCompose = compose
	sources.DesiredProjection.Record.NormalizedCompose = append([]byte(nil), compose...)
	return &ManualScriptAdmissionFixture{
		Sources: sources, Task: task, Execution: execution,
		Scripts: &ScriptRepository{store: store}, store: store, marker: marker,
	}
}

func (fixture *ManualScriptAdmissionFixture) Publish(t *testing.T, plan *agentpb.ExecutionPlan) {
	t.Helper()
	fixture.Task.PlanID = plan.PlanId
	fixture.Task.PlanHash = hex.EncodeToString(plan.PlanHash)
	fixture.Task.RenderGeneration = int32(plan.RenderGeneration)
	execution, err := NewScriptExecutionRecord(fixture.Task, plan, fixture.Task.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.Scripts.PublishExecutionWithTask(
		context.Background(), fixture.Sources, execution, fixture.Task, fixture.marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish generated manual Script = %#v, %v", result, err)
	}
}

func (fixture *ManualScriptAdmissionFixture) ChangeRoot(t *testing.T, change string) {
	t.Helper()
	key := scriptSourceRootKey(fixture.Task.OperationID)
	value := fixture.store.valueAt(key, fixture.store.revision)
	root, err := decodeScriptOperationSourceRoot(value.Value)
	if err != nil {
		t.Fatal(err)
	}
	mutation := Mutation{Type: MutationPut, Key: key}
	switch change {
	case "digest":
		root.MembershipSHA256 = scriptSourceReferenceDigest("different source set")
	case "count":
		root.MembershipCount++
	case "releasing":
		root.Phase = ScriptOperationSourceReleasing
		root.ReleasePath = ScriptSourceReleaseNormal
		root.RetryDisposition = ScriptRetryDispositionForbidden
	case "retry_available":
		root.RetryDisposition = ScriptRetryDispositionAvailable
		deadline := fixture.Task.CreatedAt.Add(24 * time.Hour)
		root.RetryExpiresAt = &deadline
	case "absent":
		mutation.Type = MutationDelete
	default:
		t.Fatalf("unknown root change %q", change)
	}
	if mutation.Type == MutationPut {
		mutation.Value, err = encodeEnvelope("script-operation-source-root", root)
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := fixture.store.Transact(context.Background(), nil, []Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("change manual Script root = %#v, %v", result, err)
	}
}
