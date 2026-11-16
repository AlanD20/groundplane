package etcd

import (
	"fmt"
	"strings"
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
	for _, source := range []string{"baseline", "reference", "prefix reference"} {
		t.Run(source, func(t *testing.T) { testNativeResolverPublicationSources(t, source) })
	}
}

func testNativeResolverPublicationSources(t *testing.T, source string) {
	t.Helper()
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
	if source != "baseline" {
		capture.References = []keyvalue.Condition{{Key: "/test/resolver-source", Prefix: source == "prefix reference"}}
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
	if _, err := tasks.PrepareResolverAuthority(t.Context(), wrongInput, wrongCapture); err == nil ||
		store.revision != before {
		t.Fatal("resolver accepted another host or partially published rejected work")
	}
	nativeTask, item, err := resolverwork.NewWork(input, capture)
	if err != nil {
		t.Fatal(err)
	}
	accepted := workexecution.CloneTask(nativeTask)
	accepted.Intent, accepted.ExpectedItems = workexecution.Input{}, 0
	accepted.AcceptedIntent = &workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	pure, err := queue.PrepareAcceptedTask(t.Context(), accepted, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(pure.Mutations)
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(mutations)
	acceptance, err := workqueue.JoinChanges(pure, workqueue.Change{Conditions: conditions, Mutations: mutations})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := queue.Commit(t.Context(), acceptance); err != nil || !result.Succeeded {
		t.Fatalf("accept pure resolver Task: %v", err)
	}
	snapshot, _, err := queue.ReadTask(t.Context(), input.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	nativeTask.AcceptedIntent = accepted.AcceptedIntent
	promotion, err := queue.PrepareAcceptedTaskPromotion(
		t.Context(),
		snapshot,
		nativeTask,
		[]workexecution.Item{item},
		nil,
		false,
		at,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(promotion.Mutations)
	authority, err := tasks.PrepareResolverAuthority(t.Context(), input, capture)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(authority.Mutations)
	publication, err := workqueue.JoinChanges(promotion, authority)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := queue.Commit(t.Context(), publication); err != nil || !result.Succeeded {
		t.Fatalf("promote native resolver: %v", err)
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
	// Reconciliation reuses the persisted projection. An empty route set must
	// remain valid across decoding, rather than becoming an absent route set.
	capture.Projection = selected.Record
	replayed, foundReplay, err := existingIdempotencyTransaction(t.Context(), store, marker)
	if err != nil || !foundReplay || replayed.kind != idempotencyTransactionExisting ||
		store.revision != acceptedRevision {
		t.Fatalf("resolver replay wrote storage or lost acceptance: %v", err)
	}
	records, err := tasks.ResolverWorkRepository()
	if err != nil {
		t.Fatal(err)
	}
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     input.HostID,
		AgentID:    ids.New(ids.KindAgent),
		Generation: 1,
	}
	expired, err := queue.ReadCandidate(
		t.Context(),
		workqueue.QueueEntry{TaskID: input.TaskID, WorkItemID: input.WorkID, Revision: view.Item.Revision},
		identity,
		input.Deadline,
		nil,
	)
	if err != nil || expired.Wait != nil || expired.Assignment.Executor.Record.Identity != identity {
		t.Fatalf("expired resolver lost the host identity needed to settle without a free executor: %v", err)
	}
	if _, _, err := workqueue.PrepareAssignment(expired.Assignment); err == nil {
		t.Fatal("expiry metadata granted a new host attempt")
	}
	stored, guards, admitted, err := records.ReadAdmission(t.Context(), view)
	if err != nil || !admitted || stored.Input.WorkID != input.WorkID {
		t.Fatalf("accepted resolver input is not executable: %v", err)
	}
	defer keyvalue.ClearMutationValues(guards.Mutations)
	// Rewriting the captured baseline to identical bytes still revokes the old
	// source revision. Both the prepared CAS and a fresh admission must reject it.
	mutation := keyvalue.Mutation{Type: keyvalue.MutationPut, Key: "/test/resolver-source", Value: []byte("new source")}
	if source == "prefix reference" {
		mutation.Key += "/child"
	}
	if source == "baseline" {
		read, err := store.Get(t.Context(), resolverbaseline.StorageKey)
		if err != nil || read.Entry == nil {
			t.Fatalf("read resolver baseline: %v", err)
		}
		defer clear(read.Entry.Value)
		mutation.Key, mutation.Value = read.Entry.Key, read.Entry.Value
	}
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{mutation}); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Transact(t.Context(), guards.Conditions, nil); err != nil || result.Succeeded {
		t.Fatalf("stale resolver admission crossed a source revision change: %v", err)
	}
	view, err = queue.ReadWork(t.Context(), input.TaskID, input.WorkID, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, revoked, admitted, err := records.ReadAdmission(t.Context(), view)
	if err != nil || admitted {
		t.Fatalf("fresh admission accepted a stale resolver baseline: %v", err)
	}
	// Revocation must settle using current evidence rather than repeat a stale CAS.
	stop, err := records.PrepareQueuedStop(t.Context(), view, workexecution.Superseded, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := queue.Commit(t.Context(), stop, revoked); err != nil || !result.Succeeded {
		t.Fatalf("stale resolver did not settle without an execution attempt: %v", err)
	}
	settled, err := queue.ReadWork(t.Context(), input.TaskID, input.WorkID, 0)
	if err != nil || settled.Item.Item.Outcome != workexecution.Superseded || settled.Item.Item.AttemptNumber != 0 {
		t.Fatalf("stale work obtained effects: %v", err)
	}
	if source == "reference" {
		request, err := records.ReadRequest(t.Context(), input.HostID, input.ComponentID, 0)
		if err != nil {
			t.Fatal(err)
		}
		capture.ReadRevision, capture.RequestRevision = store.revision, request.Revision
		changed, err := store.Get(t.Context(), mutation.Key)
		if err != nil || changed.Entry == nil {
			t.Fatalf("read changed source: %v", err)
		}
		capture.References[0].ModRevision = changed.Entry.ModRevision
		needed, err := records.NeedsAutomaticWork(t.Context(), input, capture)
		if err != nil || !needed {
			t.Fatalf("superseded source blocked fresh DNS reconciliation: %v", err)
		}
	}
}
