package etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// DNS-04/TASK-58: native resolver acceptance must publish executable work,
// captured resource state and request replay together. Host/source changes
// cannot authorize stale work. This uses a disabled resolver with no predecessor;
// it does not simulate file activation, DNS queries or physical recovery.
func TestNativeResolverPublicationReplaysAndFencesHostAndSources(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := hostbindings.PrepareLocalEnrollment(t.Context(), store, ids.New(ids.KindAgent))
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(enrollment.Mutations)
	if result, err := queue.Commit(t.Context(), enrollment); err != nil || !result.Succeeded {
		t.Fatalf("seed enrolled host: %v", err)
	}
	host, err := hostbindings.ReadLocal(t.Context(), store, 0)
	if err != nil {
		t.Fatal(err)
	}
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := platformcomponents.DefaultPlatformComponents(false)
	if err != nil {
		t.Fatal(err)
	}
	defaults[0].Desired.Enabled = false
	current, err := components.CreatePlatformComponent(t.Context(), defaults[0])
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := resolverbaseline.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	prior, err := baseline.EnsureHostResolverBaseline(t.Context(), []byte("nameserver 1.1.1.1\n"), at)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := hostresolution.NewHostResolutionProjectionRecord(store.revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := resolverexecution.Input{
		TaskID: ids.New(ids.KindTask), WorkID: ids.New(ids.KindWorkItem), HostID: host.Binding.HostID,
		OperationID: ids.New(ids.KindOperation), ComponentID: current.Record.Desired.ID,
		BaselineSHA256: prior.Record.SHA256, Generation: 1, CreatedAt: at, Deadline: at.Add(time.Minute),
	}
	capture := resolverwork.Capture{
		PlanID: ids.New(ids.KindPlan), TaskID: input.TaskID, WorkID: input.WorkID, HostID: input.HostID,
		TargetName: "CoreDNS", Actor: workexecution.OperatorActor, Action: workexecution.ActionStop,
		Desired: current.Record.Desired, ReadRevision: store.revision, ComponentRevision: current.Revision,
		BaselineRevision: prior.Revision, BaselineSHA256: prior.Record.SHA256, Projection: projection,
	}
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, input.TaskID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: "POST",
		Route: "/components/{id}/update", Key: input.OperationID,
	}
	marker.Response.Status = 202
	marker.Response.Body = []byte(fmt.Sprintf(`{"task_id":%q}`, input.TaskID))
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)

	wrongInput, wrongCapture := input, capture
	wrongInput.HostID = ids.New(ids.KindHost)
	wrongCapture.HostID = wrongInput.HostID
	before := store.revision
	if _, err := tasks.PublishResolverWork(t.Context(), wrongInput, wrongCapture, marker); err == nil ||
		store.revision != before {
		t.Fatal("resolver accepted another host or partially published rejected work")
	}
	published, err := tasks.PublishResolverWork(t.Context(), input, capture, marker)
	if err != nil || published.kind != idempotencyTransactionApplied {
		t.Fatalf("publish native resolver: %v", err)
	}
	acceptedRevision := store.revision
	view, err := queue.ReadWork(t.Context(), input.TaskID, input.WorkID, acceptedRevision)
	if err != nil || !view.Task.Task.PublicationComplete || view.Item.Item.Phase != workexecution.Ready {
		t.Fatalf("resolver acceptance did not publish ready native work: %v", err)
	}
	storedProjection, err := hostresolution.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatal(err)
	}
	selected, found, err := storedProjection.GetHostResolutionProjection(t.Context())
	if err != nil || !found || selected.Revision != view.Item.Revision ||
		selected.Record.InputSHA256 != projection.InputSHA256 {
		t.Fatalf("resolver projection was not published with its native work: %v", err)
	}
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	// Reconciliation reuses the persisted projection. An empty route set must
	// remain valid across decoding, rather than becoming an absent route set.
	capture.Projection = selected.Record
	replayed, err := restarted.PublishResolverWork(t.Context(), input, capture, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting || store.revision != acceptedRevision {
		t.Fatalf("resolver replay wrote storage or lost acceptance: %v", err)
	}
	records, err := tasks.ResolverWorkRepository()
	if err != nil {
		t.Fatal(err)
	}
	stored, guards, admitted, err := records.ReadAdmission(t.Context(), view)
	if err != nil || !admitted || stored.Input.WorkID != input.WorkID {
		t.Fatalf("accepted resolver input is not executable: %v", err)
	}
	defer keyvalue.ClearMutationValues(guards.Mutations)
	// Rewriting the captured baseline to identical bytes still revokes the old
	// source revision. Both the prepared CAS and a fresh admission must reject it.
	read, err := store.Get(t.Context(), resolverbaseline.StorageKey)
	if err != nil || read.Entry == nil {
		t.Fatalf("read resolver baseline: %v", err)
	}
	defer clear(read.Entry.Value)
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: read.Entry.Key, Value: read.Entry.Value,
	}}); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Transact(t.Context(), guards.Conditions, nil); err != nil || result.Succeeded {
		t.Fatalf("stale resolver admission crossed a source revision change: %v", err)
	}
	view, err = queue.ReadWork(t.Context(), input.TaskID, input.WorkID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, admitted, err := records.ReadAdmission(t.Context(), view); err != nil || admitted {
		t.Fatalf("fresh admission accepted a stale resolver baseline: %v", err)
	}
}
