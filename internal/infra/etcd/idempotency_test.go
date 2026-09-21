package etcd

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	json "encoding/json"
	errors "errors"
	fmt "fmt"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	http "net/http"
	strings "strings"
	sync "sync"
	testing "testing"
	time "time"
)

// Rationale: the marker is durable replay evidence, so its schema union,
// tuple binding, byte bounds, and canonical encoding must fail closed.
func TestIdempotencyMarkerCodecIsStrictAndTupleBound(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetAttach, ID: ids.NewAt(ids.KindAttach, marker.CreatedAt, 2),
	}
	value, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	decoded, err := testidempotency.DecodeIdempotencyMarker(value, marker.Locator)
	if err != nil {
		t.Fatalf("decodeIdempotencyMarker() error = %v", err)
	}
	if decoded.Kind != marker.Kind || decoded.State != marker.State ||
		!bytes.Equal(decoded.Intent.Ciphertext, marker.Intent.Ciphertext) ||
		!bytes.Equal(decoded.Response.Body, marker.Response.Body) ||
		!decoded.RetainUntil.Equal(marker.RetainUntil) || decoded.ReplayTarget == nil ||
		*decoded.ReplayTarget != *marker.ReplayTarget {
		t.Fatalf("decoded marker = %#v", decoded)
	}

	duplicate := bytes.Replace(value, []byte(`"schema":2`), []byte(`"schema":2,"schema":2`), 1)
	if _, err := testidempotency.DecodeIdempotencyMarker(duplicate, marker.Locator); !isKind(err, errs.KindInternal) {
		t.Fatalf("duplicate marker error = %v, want internal", err)
	}
	unknown := bytes.Replace(value, []byte(`"schema":2`), []byte(`"schema":2,"extra":true`), 1)
	if _, err := testidempotency.DecodeIdempotencyMarker(unknown, marker.Locator); !isKind(err, errs.KindInternal) {
		t.Fatalf("unknown marker error = %v, want internal", err)
	}
	other := marker.Locator
	other.Key = "01ARZ3NDEKTSV4RRFFQ69G5FB"
	if _, err := testidempotency.DecodeIdempotencyMarker(value, other); !isKind(err, errs.KindInternal) {
		t.Fatalf("tuple mismatch error = %v, want internal", err)
	}
}

// Rationale: Task replay must preserve the exact original compact 202 body,
// including endpoint-specific fields, and bind it to the marker's stable Task.
func TestIdempotencyTaskMarkerRequiresCanonicalResponse(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.Kind = testidempotency.IdempotencyMarkerTask
	marker.State = testidempotency.IdempotencyMarkerPending
	marker.TaskID = ids.NewAt(ids.KindTask, marker.CreatedAt, 4)
	marker.TerminalAt = time.Time{}
	marker.RetainUntil = time.Time{}
	marker.Response = testidempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"` + marker.TaskID + `"}`),
	}
	if _, err := testidempotency.EncodeIdempotencyMarker(marker); err != nil {
		t.Fatalf("encodeIdempotencyMarker(task) error = %v", err)
	}
	marker.Response.Body = []byte(
		`{"task_id":"` + marker.TaskID + `","operation_id":"op_01M12TQSNE508NMQWCJQWEW4MZ","release_id":"dep_01M12TQSNE508NMQWCKBCJGEEW"}`,
	)
	if _, err := testidempotency.EncodeIdempotencyMarker(marker); err != nil {
		t.Fatalf("encodeIdempotencyMarker(endpoint-specific task) error = %v", err)
	}
	marker.Response.Body = []byte(`{"task_id":"task_01M12TQSNE508NMQWCJRYKK0Y8"}`)
	if _, err := testidempotency.EncodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("mismatched Task response error = %v, want internal", err)
	}
	marker.Response.Body = []byte(`{ "task_id": "` + marker.TaskID + `" }`)
	if _, err := testidempotency.EncodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("noncanonical Task response error = %v, want internal", err)
	}
}

// Rationale: direct replay status and content are part of the registered
// operation contract, not interchangeable 2xx values stored by a caller.
func TestIdempotencyDirectResponseIsBoundToMethod(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		method   string
		response testidempotency.IdempotencyResponse
		valid    bool
	}{
		{http.MethodPost, testidempotency.IdempotencyResponse{Status: 201, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPost, testidempotency.IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPost, testidempotency.IdempotencyResponse{Status: 202, ContentKind: "application/json", Body: []byte(`{"task_id":"task"}`)}, false},
		{http.MethodPut, testidempotency.IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPut, testidempotency.IdempotencyResponse{Status: 201, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, false},
		{http.MethodPatch, testidempotency.IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPatch, testidempotency.IdempotencyResponse{Status: 204, ContentKind: "none"}, false},
		{http.MethodDelete, testidempotency.IdempotencyResponse{Status: 204, ContentKind: "none"}, true},
		{http.MethodDelete, testidempotency.IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{}`)}, false},
	} {
		marker := testDirectMarker()
		marker.Locator.Method = test.method
		marker.Response = test.response
		_, err := testidempotency.EncodeIdempotencyMarker(marker)
		if test.valid && err != nil {
			t.Fatalf("encodeIdempotencyMarker(%s valid) error = %v", test.method, err)
		}
		if !test.valid && !isKind(err, errs.KindInternal) {
			t.Fatalf("encodeIdempotencyMarker(%s invalid) error = %v, want internal", test.method, err)
		}
	}
}

// Rationale: exact replay bodies are intentionally durable but must not leak
// through ordinary, detailed, or Go-syntax formatting.
func TestIdempotencyResponseFormattingRedactsBody(t *testing.T) {
	t.Parallel()

	response := testidempotency.IdempotencyResponse{
		Status: 200, ContentKind: "application/json", Body: []byte(`{"token":"format-secret"}`),
	}
	formatted := fmt.Sprintf("%v|%+v|%#v", response, &response, response)
	if strings.Contains(formatted, "format-secret") || !strings.Contains(formatted, "body:redacted") {
		t.Fatalf("formatted response leaked body: %s", formatted)
	}
}

// Rationale: retention ordering must preserve nanoseconds, and validation must
// bind the entire timestamp and marker key rather than a seconds bucket.
func TestIdempotencyRetentionKeyUsesFullUnixNanoseconds(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	first, err := testidempotency.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	secondTime := marker.RetainUntil.Add(time.Nanosecond)
	second, err := testidempotency.IdempotencyRetentionKey(markerKey, secondTime)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey(+1ns) error = %v", err)
	}
	if first == second || !strings.HasPrefix(first, testidempotency.IdempotencyRetentionPrefix) {
		t.Fatalf("retention keys = %q, %q", first, second)
	}
	if err := testidempotency.ValidateIdempotencyRetentionKey(first, markerKey, marker.RetainUntil); err != nil {
		t.Fatalf("validateIdempotencyRetentionKey() error = %v", err)
	}
	if err := testidempotency.ValidateIdempotencyRetentionKey(first, markerKey, secondTime); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("retention timestamp mismatch error = %v, want internal", err)
	}
}

// Rationale: both Task indexes share one canonical reference schema and must
// reject raw ids, duplicate fields, and unknown compatibility members.
func TestTaskIndexReferenceIsCanonicalStrictJSON(t *testing.T) {
	t.Parallel()

	taskID := ids.NewAt(ids.KindTask, testMarkerTime(), 9)
	value, err := testidempotency.EncodeTaskReference(taskID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	want := `{"schema":1,"record_id":"` + taskID + `"}`
	if string(value) != want {
		t.Fatalf("task reference = %s, want %s", value, want)
	}
	if got, err := testidempotency.DecodeTaskReference(value); err != nil || got != taskID {
		t.Fatalf("decodeTaskReference() = %q, %v", got, err)
	}
	for _, invalid := range [][]byte{
		[]byte(taskID),
		[]byte(`{"schema":1,"schema":1,"record_id":"` + taskID + `"}`),
		[]byte(`{"schema":1,"record_id":"` + taskID + `","id":"` + taskID + `"}`),
	} {
		if _, err := testidempotency.DecodeTaskReference(invalid); !isKind(err, errs.KindInternal) {
			t.Fatalf("decodeTaskReference(%s) error = %v, want internal", invalid, err)
		}
	}
}

// Rationale: a known compare failure must classify the marker from reads at
// the transaction revision and must not issue a later linearizable Get.
func TestIdempotencyRepositoryUsesTransactionFailureEvidence(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 41},
		Responses: []*etcdserverpb.ResponseOp{
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
				Kvs: []*mvccpb.KeyValue{{
					Key: []byte("/groundplane" + markerKey), Value: markerValue, ModRevision: 37,
				}},
			}}},
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}}},
		},
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	plan, err := NewIdempotencyMutationPlan(
		[]testkeyvalue.Condition{{Key: "/records/resource", ModRevision: 7}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/records/resource", Value: []byte("value")}},
		func(int64, []*testkeyvalue.KeyValue) error { return errs.New(errs.KindStateConflict, "state changed") },
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan() error = %v", err)
	}
	result, err := repository.Apply(context.Background(), marker, plan)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	outcome, existing, conflict, classifyErr := result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownExisting || conflict != nil ||
		existing.Kind != marker.Kind || result.revision != 41 || backend.getKey != "" {
		t.Fatalf("Apply() result/get = %#v/%q", result, backend.getKey)
	}
}

// Rationale: a child-resource delete replay must recover its original owner-scoped marker from the stable
// target id and validate both records at the replay-index read revision after the child primary is gone.
func TestIdempotencyRepositoryResolvesReplayTargetAtFixedRevision(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   ids.NewAt(ids.KindEnvironment, marker.CreatedAt, 3),
		Method:    http.MethodDelete,
		Route:     "/attaches/{id}",
		Key:       "attach-detach-key-0001",
	}
	marker.Response = testidempotency.IdempotencyResponse{Status: http.StatusNoContent, ContentKind: "none"}
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetAttach,
		ID:   ids.NewAt(ids.KindAttach, marker.CreatedAt, 4),
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	targetKey, err := testidempotency.IdempotencyReplayTargetKey(
		*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	targetValue, err := testidempotency.EncodeReplayTargetReference(markerKey)
	if err != nil {
		t.Fatalf("encodeReplayTargetReference() error = %v", err)
	}
	store := &replayLookupTestStore{
		targetKey: targetKey,
		target: testkeyvalue.GetResult{
			Entry: &testkeyvalue.KeyValue{Key: targetKey, Value: targetValue, ModRevision: 17}, ReadRevision: 23,
		},
		markerKey: markerKey,
		markers: testkeyvalue.GetManyResult{
			Values: []*testkeyvalue.KeyValue{{Key: markerKey, Value: markerValue, ModRevision: 16}}, ReadRevision: 23,
		},
	}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	resolved, found, err := repository.ResolveReplayLocator(
		context.Background(), *marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
	)
	if err != nil || !found || resolved != marker.Locator || store.markerRevision != 23 {
		t.Fatalf("ResolveReplayLocator() = %#v, %v, %v; revision = %d", resolved, found, err, store.markerRevision)
	}
}

// Rationale: transaction plans contain raw persistence operations and are
// therefore opaque and single-use even under concurrent retry paths.
func TestIdempotencyMutationPlanHasOneConcurrentConsumer(t *testing.T) {
	t.Parallel()

	plan, err := NewIdempotencyMutationPlan(nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/record", Value: []byte("value")}},
		func(int64, []*testkeyvalue.KeyValue) error { return errs.New(errs.KindStateConflict, "classified") },
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan() error = %v", err)
	}
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _, _, err := plan.consume()
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	var success, failure int
	for err := range results {
		if err == nil {
			success++
		} else if isKind(err, errs.KindInternal) {
			failure++
		}
	}
	if success != 1 || failure != 1 {
		t.Fatalf("plan consumers = %d success, %d failure", success, failure)
	}
	if plan.conditions != nil || plan.mutations != nil || plan.classify != nil {
		t.Fatal("consumed plan retained compare, mutation, or classifier state")
	}
}

// Rationale: resource plans may CAS their own keys, but cannot duplicate one
// branch key or reach into the coordinator-owned marker/index namespaces.
func TestIdempotencyMutationPlanRejectsDuplicateAndReservedKeys(t *testing.T) {
	t.Parallel()

	classifier := func(int64, []*testkeyvalue.KeyValue) error { return errs.New(errs.KindStateConflict, "changed") }
	for _, test := range []struct {
		conditions []testkeyvalue.Condition
		mutations  []testkeyvalue.Mutation
	}{
		{[]testkeyvalue.Condition{{Key: "/record", ModRevision: 1}, {Key: "/record", ModRevision: 1}},
			[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/record", Value: []byte("x")}}},
		{nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/one", Value: []byte("x")},
			{Type: testkeyvalue.MutationDelete, Key: "/one"}}},
		{[]testkeyvalue.Condition{{Key: testidempotency.IdempotencyMarkerPrefix + "owned", ModRevision: 0}},
			[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/record", Value: []byte("x")}}},
		{nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testidempotency.IdempotencyRetentionPrefix + "owned", Value: []byte("x")}}},
		{nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testidempotency.IdempotencyReplayTargetPrefix + "owned", Value: []byte("x")}}},
	} {
		if _, err := NewIdempotencyMutationPlan(
			test.conditions,
			test.mutations,
			classifier,
		); !isKind(
			err,
			errs.KindInternal,
		) {
			t.Fatalf(
				"newIdempotencyMutationPlan(%#v, %#v) error = %v, want internal",
				test.conditions,
				test.mutations,
				err,
			)
		}
	}
	if _, err := NewIdempotencyMutationPlan(
		[]testkeyvalue.Condition{{Key: "/record", ModRevision: 1}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/record", Value: []byte("x")}},
		classifier,
	); err != nil {
		t.Fatalf("same CAS compare/mutation key error = %v", err)
	}
}

// Rationale: Task marker construction remains unavailable until the Task
// repository can contribute Task and both index writes to the same plan.
func TestIdempotencyRepositoryRejectsTaskMarkerBeforeConsumingPlan(t *testing.T) {
	t.Parallel()

	repository, err := NewIdempotencyRepository(&collectorTestStore{})
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	plan, err := NewIdempotencyMutationPlan(nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/task", Value: []byte("task")}},
		func(int64, []*testkeyvalue.KeyValue) error { return errs.New(errs.KindStateConflict, "changed") },
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Kind = testidempotency.IdempotencyMarkerTask
	if _, err := repository.Apply(context.Background(), marker, plan); !isKind(err, errs.KindInternal) {
		t.Fatalf("Apply(Task) error = %v, want internal", err)
	}
	if _, _, _, err := plan.consume(); err != nil {
		t.Fatalf("Task rejection consumed plan: %v", err)
	}
}

// Rationale: pruning a deletion marker must validate marker, retention, and replay-target counterparts while
// fitting exactly 16 triples into 48 compares plus 48 deletes.
func TestIdempotencyRepositoryPrunesAtMost16ValidatedTriples(t *testing.T) {
	t.Parallel()

	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 80}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	candidates := make([]idempotencyPruneCandidate, maximumPruneMarkers)
	for index := range candidates {
		marker := testDirectMarker()
		marker.Locator.Key = "01ARZ3NDEKTSV4RRFFQ69G5" + string(rune('A'+index))
		marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
			Kind: testidempotency.IdempotencyReplayTargetAttach,
			ID:   ids.NewAt(ids.KindAttach, marker.CreatedAt, int64(index+30)),
		}
		markerKey, keyErr := testidempotency.IdempotencyMarkerKey(marker.Locator)
		if keyErr != nil {
			t.Fatalf("idempotencyMarkerKey(%d) error = %v", index, keyErr)
		}
		retentionKey, keyErr := testidempotency.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
		if keyErr != nil {
			t.Fatalf("idempotencyRetentionKey(%d) error = %v", index, keyErr)
		}
		retentionValue, marshalErr := json.Marshal(
			testidempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey},
		)
		if marshalErr != nil {
			t.Fatalf("json.Marshal(retention %d) error = %v", index, marshalErr)
		}
		targetKey, keyErr := testidempotency.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if keyErr != nil {
			t.Fatalf("idempotencyReplayTargetKey(%d) error = %v", index, keyErr)
		}
		targetValue, marshalErr := testidempotency.EncodeReplayTargetReference(markerKey)
		if marshalErr != nil {
			t.Fatalf("encodeReplayTargetReference(%d) error = %v", index, marshalErr)
		}
		candidates[index] = idempotencyPruneCandidate{
			Marker:       IdempotencyEvidence{marker: marker, modRevision: int64(index + 1)},
			RetentionKey: retentionKey, RetentionValue: retentionValue,
			RetentionModRevision: int64(index + 101),
			ReplayTargetKey:      targetKey, ReplayTargetValue: targetValue,
			ReplayTargetModRevision: int64(index + 201),
		}
	}
	revision, err := repository.pruneExpired(
		context.Background(),
		testDirectMarker().RetainUntil.Add(time.Nanosecond),
		candidates,
	)
	if err != nil || revision != 80 {
		t.Fatalf("pruneExpired(16) = %d, %v", revision, err)
	}
	if len(backend.transaction.conditions) != 48 || len(backend.transaction.operations) != 48 {
		t.Fatalf(
			"prune transaction conditions/operations = %d/%d",
			len(backend.transaction.conditions),
			len(backend.transaction.operations),
		)
	}
	for _, operation := range backend.transaction.operations {
		if !operation.IsDelete() {
			t.Fatal("prune transaction contains a non-delete operation")
		}
	}

	backend.transaction = nil
	tooMany := append(append([]idempotencyPruneCandidate(nil), candidates...), candidates[0])
	if _, err := repository.pruneExpired(
		context.Background(),
		testMarkerTime(),
		tooMany,
	); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("pruneExpired(25) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("pruneExpired(17) reached etcd")
	}

	backend.transaction = nil
	malformed := append([]idempotencyPruneCandidate(nil), candidates...)
	malformed[0].RetentionValue = nil
	if _, err := repository.pruneExpired(
		context.Background(),
		testDirectMarker().RetainUntil.Add(time.Nanosecond),
		malformed,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("pruneExpired(missing counterpart) error = %v, want internal", err)
	}
	if backend.transaction != nil {
		t.Fatal("pruneExpired(missing counterpart) reached etcd")
	}
}

// Rationale: a pruning transaction error has unknown commit status and must
// propagate unchanged rather than being followed by a speculative read.
func TestIdempotencyPrunePropagatesUnknownOutcome(t *testing.T) {
	t.Parallel()

	backendError := status.Error(codes.Unavailable, "private backend detail")
	backend := &fakeClient{transactionError: backendError}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	marker := testDirectMarker()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := testidempotency.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(testidempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	_, err = repository.pruneExpired(context.Background(), marker.RetainUntil, []idempotencyPruneCandidate{{
		Marker:       IdempotencyEvidence{marker: marker, modRevision: 7},
		RetentionKey: retentionKey, RetentionValue: retentionValue, RetentionModRevision: 8,
	}})
	if !isKind(err, errs.KindStorageUnavailable) || !errors.Is(err, backendError) || backend.getKey != "" {
		t.Fatalf("pruneExpired() error/get = %v/%q", err, backend.getKey)
	}
}

// Rationale: daily collection must hydrate both counterparts at one fixed
// revision and retry a known CAS miss only after taking a fresh snapshot.
func TestIdempotencyPruneExpiredUsesFreshFixedRevisionAfterCASConflict(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	first := testCollectorSnapshot(t, marker, 51, 41, 42)
	second := testCollectorSnapshot(t, marker, 52, 43, 44)
	store := &collectorTestStore{
		snapshots: []collectorSnapshot{first, second},
		transactionResults: []testkeyvalue.TransactionResult{
			{Revision: 60},
			{Succeeded: true, Revision: 61},
		},
	}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	count, err := repository.PruneExpired(context.Background(), marker.RetainUntil.Add(time.Nanosecond))
	if err != nil || count != 1 {
		t.Fatalf("PruneExpired() = %d, %v", count, err)
	}
	if store.rangeCalls != 2 || store.getManyCalls != 2 || store.transactionCalls != 2 ||
		store.getManyRevisions[0] != 51 || store.getManyRevisions[1] != 52 {
		t.Fatalf(
			"collector calls/revisions = %d/%d/%d %#v",
			store.rangeCalls, store.getManyCalls, store.transactionCalls, store.getManyRevisions,
		)
	}
}

// Rationale: a missing marker counterpart is corruption before any prune
// write, while an unknown transaction outcome is returned without a retry.
func TestIdempotencyPruneExpiredAbortsMissingAndUnknownEvidence(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	snapshot := testCollectorSnapshot(t, marker, 51, 41, 42)
	snapshot.markers.Values[0] = nil
	missingStore := &collectorTestStore{snapshots: []collectorSnapshot{snapshot}}
	repository, err := NewIdempotencyRepository(missingStore)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository(missing) error = %v", err)
	}
	if _, err := repository.PruneExpired(
		context.Background(), marker.RetainUntil,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("PruneExpired(missing) error = %v, want internal", err)
	}
	if missingStore.transactionCalls != 0 {
		t.Fatal("PruneExpired(missing) reached transaction")
	}

	backendError := errs.New(errs.KindStorageUnavailable, "unknown transaction outcome")
	unknownStore := &collectorTestStore{
		snapshots:         []collectorSnapshot{testCollectorSnapshot(t, marker, 53, 45, 46)},
		transactionErrors: []error{backendError},
	}
	repository, err = NewIdempotencyRepository(unknownStore)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository(unknown) error = %v", err)
	}
	if _, err := repository.PruneExpired(
		context.Background(), marker.RetainUntil,
	); !errors.Is(err, backendError) {
		t.Fatalf("PruneExpired(unknown) error = %v, want original", err)
	}
	if unknownStore.rangeCalls != 1 || unknownStore.transactionCalls != 1 {
		t.Fatalf("PruneExpired(unknown) calls = %d/%d", unknownStore.rangeCalls, unknownStore.transactionCalls)
	}
}

// Rationale: the marker-absent compare is the serialization authority; two
// concurrent valid claims have exactly one applied outcome and one existing
// evidence outcome under the race detector.
func TestIdempotencyRepositoryConcurrentClaimsHaveOneWinner(t *testing.T) {
	t.Parallel()

	store := &atomicClaimStore{}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	marker := testDirectMarker()
	results := make(chan IdempotencyTransactionResult, 2)
	errors := make(chan error, 2)
	for index := range 2 {
		plan, planErr := NewIdempotencyMutationPlan(
			nil,
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: fmt.Sprintf("/resource/%d", index), Value: []byte("value")},
			},
			func(int64, []*testkeyvalue.KeyValue) error { return errs.New(errs.KindStateConflict, "changed") },
		)
		if planErr != nil {
			t.Fatalf("newIdempotencyMutationPlan(%d) error = %v", index, planErr)
		}
		go func(candidate *idempotencyMutationPlan) {
			result, applyErr := repository.Apply(context.Background(), marker, candidate)
			results <- result
			errors <- applyErr
		}(plan)
	}
	var applied, existing int
	for range 2 {
		result := <-results
		if err := <-errors; err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		outcome, _, _, err := result.Classify()
		if err != nil {
			t.Fatalf("Classify() error = %v", err)
		}
		switch outcome {
		case IdempotencyKnownApplied:
			applied++
		case IdempotencyKnownExisting:
			existing++
		}
	}
	if applied != 1 || existing != 1 {
		t.Fatalf("claim outcomes = %d applied, %d existing", applied, existing)
	}
}

// Rationale: a direct no-op still needs durable protected replay evidence, but
// must not invent a domain mutation merely to make the coordinator transaction non-empty.
func TestIdempotencyRepositoryMarkerOnlyReplayAndUnknownOutcomeRecovery(t *testing.T) {
	t.Parallel()
	store := &markerOnlyRecoveryStore{}
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	marker := testDirectMarker()
	classifier := func(int64, []*testkeyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "domain fence changed")
	}
	plan, err := NewIdempotencyMutationPlan(
		[]testkeyvalue.Condition{{Key: "/v1/test/no-op-domain", ModRevision: 0}}, nil, classifier,
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan(marker-only) error = %v", err)
	}
	_, err = repository.Apply(context.Background(), marker, plan)
	if !isKind(err, errs.KindStorageUnavailable) {
		t.Fatalf("Apply(unknown marker-only) error = %v", err)
	}
	retryPlan, err := NewIdempotencyMutationPlan(
		[]testkeyvalue.Condition{{Key: "/v1/test/no-op-domain", ModRevision: 0}}, nil, classifier,
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan(retry) error = %v", err)
	}
	retry, err := repository.Apply(context.Background(), marker, retryPlan)
	if err != nil {
		t.Fatalf("Apply(retry) error = %v", err)
	}
	outcome, existing, conflict, classifyErr := retry.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		existing.Intent.CiphertextDigest != marker.Intent.CiphertextDigest {
		t.Fatalf("marker-only retry = %v, %#v, %v, %v", outcome, existing, conflict, classifyErr)
	}
	for _, call := range store.mutations {
		for _, mutation := range call {
			if !strings.HasPrefix(mutation.Key, testidempotency.IdempotencyMarkerPrefix) &&
				!strings.HasPrefix(mutation.Key, testidempotency.IdempotencyRetentionPrefix) {
				t.Fatalf("marker-only transaction wrote domain key %q", mutation.Key)
			}
		}
	}

	mismatch := marker
	mismatch.Intent.Ciphertext = []byte("different-protected-intent")
	digest := sha256.Sum256(mismatch.Intent.Ciphertext)
	mismatch.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	mismatchPlan, err := NewIdempotencyMutationPlan(nil, nil, classifier)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan(mismatch) error = %v", err)
	}
	mismatchResult, err := repository.Apply(context.Background(), mismatch, mismatchPlan)
	if err != nil {
		t.Fatalf("Apply(mismatch) error = %v", err)
	}
	mismatchOutcome, mismatchExisting, mismatchConflict, mismatchClassifyErr := mismatchResult.Classify()
	defer clear(mismatchExisting.Intent.Ciphertext)
	defer clear(mismatchExisting.Response.Body)
	if mismatchClassifyErr != nil || mismatchConflict != nil || mismatchOutcome != IdempotencyKnownExisting ||
		mismatchExisting.Intent.CiphertextDigest == mismatch.Intent.CiphertextDigest {
		t.Fatalf(
			"mismatched marker was not rejected as existing evidence: %v, %#v, %v, %v",
			mismatchOutcome,
			mismatchExisting,
			mismatchConflict,
			mismatchClassifyErr,
		)
	}
	if _, err := newIdempotencyMutationPlanForMarker(testidempotency.IdempotencyMarkerTask, nil, nil, classifier); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("zero-mutation Task plan error = %v, want internal", err)
	}
}

type markerOnlyRecoveryStore struct {
	testkeyvalue.Store

	mu        sync.Mutex
	revision  int64
	marker    *testkeyvalue.KeyValue
	mutations [][]testkeyvalue.Mutation
}

func (store *markerOnlyRecoveryStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.revision++
	store.mutations = append(store.mutations, testkeyvalue.CloneMutations(mutations))
	if store.marker == nil {
		for _, mutation := range mutations {
			if mutation.Type == testkeyvalue.MutationPut &&
				strings.HasPrefix(mutation.Key, testidempotency.IdempotencyMarkerPrefix) {
				store.marker = &testkeyvalue.KeyValue{
					Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision,
				}
				break
			}
		}
		if store.marker == nil {
			return testkeyvalue.TransactionResult{}, errs.New(
				errs.KindInternal,
				"marker-only transaction omitted its marker",
			)
		}
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindStorageUnavailable,
			"unknown marker-only transaction outcome",
		)
	}
	reads := make([]*testkeyvalue.KeyValue, len(conditions))
	reads[0] = &testkeyvalue.KeyValue{
		Key: store.marker.Key, Value: append([]byte(nil), store.marker.Value...), ModRevision: store.marker.ModRevision,
	}
	return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: reads}, nil
}

type replayLookupTestStore struct {
	testkeyvalue.Store

	targetKey      string
	target         testkeyvalue.GetResult
	markerKey      string
	markers        testkeyvalue.GetManyResult
	markerRevision int64
}

func (store *replayLookupTestStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key != store.targetKey {
		return nil, errs.New(errs.KindInternal, "unexpected replay target key")
	}
	result := store.target
	if result.Entry != nil {
		entry := *result.Entry
		entry.Value = append([]byte(nil), result.Entry.Value...)
		result.Entry = &entry
	}
	return &result, nil
}

func (store *replayLookupTestStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(request.Keys) != 1 || request.Keys[0] != store.markerKey || request.Revision != store.target.ReadRevision {
		return nil, errs.New(errs.KindInternal, "unexpected replay marker lookup")
	}
	store.markerRevision = request.Revision
	result := store.markers
	result.Values = cloneKeyValuePointers(result.Values)
	return &result, nil
}

type atomicClaimStore struct {
	testkeyvalue.Store

	mu       sync.Mutex
	revision int64
	marker   *testkeyvalue.KeyValue
}

func (store *atomicClaimStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.revision++
	if store.marker != nil {
		reads := make([]*testkeyvalue.KeyValue, len(conditions))
		reads[0] = &testkeyvalue.KeyValue{
			Key: store.marker.Key, Value: append([]byte(nil), store.marker.Value...),
			ModRevision: store.marker.ModRevision,
		}
		return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: reads}, nil
	}
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationPut &&
			strings.HasPrefix(mutation.Key, testidempotency.IdempotencyMarkerPrefix) {
			store.marker = &testkeyvalue.KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision,
			}
			break
		}
	}
	if store.marker == nil {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "claim transaction omitted marker")
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func cloneKeyValueSlice(values []testkeyvalue.KeyValue) []testkeyvalue.KeyValue {
	result := make([]testkeyvalue.KeyValue, len(values))
	for index, value := range values {
		result[index] = testkeyvalue.KeyValue{
			Key:         value.Key,
			Value:       append([]byte(nil), value.Value...),
			ModRevision: value.ModRevision,
		}
	}
	return result
}

func cloneKeyValuePointers(values []*testkeyvalue.KeyValue) []*testkeyvalue.KeyValue {
	result := make([]*testkeyvalue.KeyValue, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = &testkeyvalue.KeyValue{
				Key:         value.Key,
				Value:       append([]byte(nil), value.Value...),
				ModRevision: value.ModRevision,
			}
		}
	}
	return result
}

func testDirectMarker() testidempotency.IdempotencyMarker {
	createdAt := testMarkerTime()
	ciphertext := []byte("protected-intent")
	digest := sha256.Sum256(ciphertext)
	return testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerDirect, State: testidempotency.IdempotencyMarkerCompleted,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeTenant,
			ScopeID:   ids.NewAt(ids.KindTenant, createdAt, 1),
			Method:    http.MethodPatch, Route: "/projects/{id}", Key: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: testidempotency.IdempotencyResponse{
			Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"prj"}`),
		},
		CreatedAt: createdAt, UpdatedAt: createdAt, TerminalAt: createdAt,
		RetainUntil: createdAt.Add(testidempotency.MarkerRetention),
	}
}

func testMarkerTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 123456789, time.UTC)
}

func isKind(err error, want errs.Kind) bool {
	got, ok := errs.KindOf(err)
	return ok && got == want
}
