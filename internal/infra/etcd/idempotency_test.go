package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Rationale: the marker is durable replay evidence, so its schema union,
// tuple binding, byte bounds, and canonical encoding must fail closed.
func TestIdempotencyMarkerCodecIsStrictAndTupleBound(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	value, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	decoded, err := decodeIdempotencyMarker(value, marker.Locator)
	if err != nil {
		t.Fatalf("decodeIdempotencyMarker() error = %v", err)
	}
	if decoded.Kind != marker.Kind || decoded.State != marker.State ||
		!bytes.Equal(decoded.Intent.Ciphertext, marker.Intent.Ciphertext) ||
		!bytes.Equal(decoded.Response.Body, marker.Response.Body) ||
		!decoded.RetainUntil.Equal(marker.RetainUntil) {
		t.Fatalf("decoded marker = %#v", decoded)
	}

	duplicate := bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1)
	if _, err := decodeIdempotencyMarker(duplicate, marker.Locator); !isKind(err, errs.KindInternal) {
		t.Fatalf("duplicate marker error = %v, want internal", err)
	}
	unknown := bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1)
	if _, err := decodeIdempotencyMarker(unknown, marker.Locator); !isKind(err, errs.KindInternal) {
		t.Fatalf("unknown marker error = %v, want internal", err)
	}
	other := marker.Locator
	other.Key = "01ARZ3NDEKTSV4RRFFQ69G5FB"
	if _, err := decodeIdempotencyMarker(value, other); !isKind(err, errs.KindInternal) {
		t.Fatalf("tuple mismatch error = %v, want internal", err)
	}
}

// Rationale: Task replay must always be the exact original canonical 202 body
// and its task_id must equal the marker's stable Task reference.
func TestIdempotencyTaskMarkerRequiresExactResponse(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.Kind = IdempotencyMarkerTask
	marker.State = IdempotencyMarkerPending
	marker.TaskID = ids.NewAt(ids.KindTask, marker.CreatedAt, 4)
	marker.TerminalAt = time.Time{}
	marker.RetainUntil = time.Time{}
	marker.Response = IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"` + marker.TaskID + `"}`),
	}
	if _, err := encodeIdempotencyMarker(marker); err != nil {
		t.Fatalf("encodeIdempotencyMarker(task) error = %v", err)
	}
	marker.Response.Body = []byte(`{ "task_id": "` + marker.TaskID + `" }`)
	if _, err := encodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("noncanonical Task response error = %v, want internal", err)
	}
}

// Rationale: direct replay status and content are part of the registered
// operation contract, not interchangeable 2xx values stored by a caller.
func TestIdempotencyDirectResponseIsBoundToMethod(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		method   string
		response IdempotencyResponse
		valid    bool
	}{
		{http.MethodPost, IdempotencyResponse{Status: 201, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPost, IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, false},
		{http.MethodPut, IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPut, IdempotencyResponse{Status: 201, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, false},
		{http.MethodPatch, IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{"id":"x"}`)}, true},
		{http.MethodPatch, IdempotencyResponse{Status: 204, ContentKind: "none"}, false},
		{http.MethodDelete, IdempotencyResponse{Status: 204, ContentKind: "none"}, true},
		{http.MethodDelete, IdempotencyResponse{Status: 200, ContentKind: "application/json", Body: []byte(`{}`)}, false},
	} {
		marker := testDirectMarker()
		marker.Locator.Method = test.method
		marker.Response = test.response
		_, err := encodeIdempotencyMarker(marker)
		if test.valid && err != nil {
			t.Fatalf("encodeIdempotencyMarker(%s valid) error = %v", test.method, err)
		}
		if !test.valid && !isKind(err, errs.KindInternal) {
			t.Fatalf("encodeIdempotencyMarker(%s invalid) error = %v, want internal", test.method, err)
		}
	}
}

// Rationale: accepted ciphertext and replay-body ceilings are inclusive, and
// one byte over either limit must fail before transaction submission.
func TestIdempotencyMarkerPayloadBoundaries(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.Intent.Ciphertext = bytes.Repeat([]byte{'c'}, maximumIntentCiphertext)
	digest := sha256.Sum256(marker.Intent.Ciphertext)
	marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	marker.Response.Body = bytes.Repeat([]byte{'a'}, maximumReplayBody)
	marker.Response.Body[0] = '"'
	marker.Response.Body[len(marker.Response.Body)-1] = '"'
	value, err := encodeIdempotencyMarker(marker)
	if err != nil || len(value) > maximumMarkerBytes {
		t.Fatalf("encodeIdempotencyMarker(boundary) bytes/error = %d/%v", len(value), err)
	}

	marker.Intent.Ciphertext = append(marker.Intent.Ciphertext, 'x')
	digest = sha256.Sum256(marker.Intent.Ciphertext)
	marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	if _, err := encodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("ciphertext over limit error = %v, want internal", err)
	}

	marker = testDirectMarker()
	marker.Response.Body = bytes.Repeat([]byte{'a'}, maximumReplayBody+1)
	marker.Response.Body[0] = '"'
	marker.Response.Body[len(marker.Response.Body)-1] = '"'
	if _, err := encodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("response over limit error = %v, want internal", err)
	}
}

// Rationale: exact replay bodies are intentionally durable but must not leak
// through ordinary, detailed, or Go-syntax formatting.
func TestIdempotencyResponseFormattingRedactsBody(t *testing.T) {
	t.Parallel()

	response := IdempotencyResponse{
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
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	first, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	secondTime := marker.RetainUntil.Add(time.Nanosecond)
	second, err := idempotencyRetentionKey(markerKey, secondTime)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey(+1ns) error = %v", err)
	}
	if first == second || !strings.HasPrefix(first, idempotencyRetentionPrefix) {
		t.Fatalf("retention keys = %q, %q", first, second)
	}
	if err := validateIdempotencyRetentionKey(first, markerKey, marker.RetainUntil); err != nil {
		t.Fatalf("validateIdempotencyRetentionKey() error = %v", err)
	}
	if err := validateIdempotencyRetentionKey(first, markerKey, secondTime); !isKind(err, errs.KindInternal) {
		t.Fatalf("retention timestamp mismatch error = %v, want internal", err)
	}
}

// Rationale: both Task indexes share one canonical reference schema and must
// reject raw ids, duplicate fields, and unknown compatibility members.
func TestTaskIndexReferenceIsCanonicalStrictJSON(t *testing.T) {
	t.Parallel()

	taskID := ids.NewAt(ids.KindTask, testMarkerTime(), 9)
	value, err := encodeTaskReference(taskID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	want := `{"schema":1,"record_id":"` + taskID + `"}`
	if string(value) != want {
		t.Fatalf("task reference = %s, want %s", value, want)
	}
	if got, err := decodeTaskReference(value); err != nil || got != taskID {
		t.Fatalf("decodeTaskReference() = %q, %v", got, err)
	}
	for _, invalid := range [][]byte{
		[]byte(taskID),
		[]byte(`{"schema":1,"schema":1,"record_id":"` + taskID + `"}`),
		[]byte(`{"schema":1,"record_id":"` + taskID + `","id":"` + taskID + `"}`),
	} {
		if _, err := decodeTaskReference(invalid); !isKind(err, errs.KindInternal) {
			t.Fatalf("decodeTaskReference(%s) error = %v, want internal", invalid, err)
		}
	}
}

// Rationale: a known compare failure must classify the marker from reads at
// the transaction revision and must not issue a later linearizable Get.
func TestIdempotencyRepositoryUsesTransactionFailureEvidence(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	markerKey, err := idempotencyMarkerKey(marker.Locator)
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
	plan, err := newIdempotencyMutationPlan(
		[]Condition{{Key: "/records/resource", ModRevision: 7}},
		[]Mutation{{Type: MutationPut, Key: "/records/resource", Value: []byte("value")}},
		func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "state changed") },
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

// Rationale: transaction plans contain raw persistence operations and are
// therefore opaque and single-use even under concurrent retry paths.
func TestIdempotencyMutationPlanHasOneConcurrentConsumer(t *testing.T) {
	t.Parallel()

	plan, err := newIdempotencyMutationPlan(nil,
		[]Mutation{{Type: MutationPut, Key: "/record", Value: []byte("value")}},
		func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "classified") },
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

	classifier := func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "changed") }
	for _, test := range []struct {
		conditions []Condition
		mutations  []Mutation
	}{
		{[]Condition{{Key: "/record", ModRevision: 1}, {Key: "/record", ModRevision: 1}},
			[]Mutation{{Type: MutationPut, Key: "/record", Value: []byte("x")}}},
		{nil, []Mutation{{Type: MutationPut, Key: "/one", Value: []byte("x")},
			{Type: MutationDelete, Key: "/one"}}},
		{[]Condition{{Key: idempotencyMarkerPrefix + "owned", ModRevision: 0}},
			[]Mutation{{Type: MutationPut, Key: "/record", Value: []byte("x")}}},
		{nil, []Mutation{{Type: MutationPut, Key: idempotencyRetentionPrefix + "owned", Value: []byte("x")}}},
	} {
		if _, err := newIdempotencyMutationPlan(test.conditions, test.mutations, classifier); !isKind(err, errs.KindInternal) {
			t.Fatalf("newIdempotencyMutationPlan(%#v, %#v) error = %v, want internal", test.conditions, test.mutations, err)
		}
	}
	if _, err := newIdempotencyMutationPlan(
		[]Condition{{Key: "/record", ModRevision: 1}},
		[]Mutation{{Type: MutationPut, Key: "/record", Value: []byte("x")}},
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
	plan, err := newIdempotencyMutationPlan(nil,
		[]Mutation{{Type: MutationPut, Key: "/task", Value: []byte("task")}},
		func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "changed") },
	)
	if err != nil {
		t.Fatalf("newIdempotencyMutationPlan() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Kind = IdempotencyMarkerTask
	if _, err := repository.Apply(context.Background(), marker, plan); !isKind(err, errs.KindInternal) {
		t.Fatalf("Apply(Task) error = %v, want internal", err)
	}
	if _, _, _, err := plan.consume(); err != nil {
		t.Fatalf("Task rejection consumed plan: %v", err)
	}
}

// Rationale: pruning must validate both counterparts before writing and fit
// exactly 24 markers into 48 compares plus 48 deletes.
func TestIdempotencyRepositoryPrunesAtMost24ValidatedCounterparts(t *testing.T) {
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
		markerKey, keyErr := idempotencyMarkerKey(marker.Locator)
		if keyErr != nil {
			t.Fatalf("idempotencyMarkerKey(%d) error = %v", index, keyErr)
		}
		retentionKey, keyErr := idempotencyRetentionKey(markerKey, marker.RetainUntil)
		if keyErr != nil {
			t.Fatalf("idempotencyRetentionKey(%d) error = %v", index, keyErr)
		}
		retentionValue, marshalErr := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if marshalErr != nil {
			t.Fatalf("json.Marshal(retention %d) error = %v", index, marshalErr)
		}
		candidates[index] = idempotencyPruneCandidate{
			Marker:       idempotencyEvidence{marker: marker, modRevision: int64(index + 1)},
			RetentionKey: retentionKey, RetentionValue: retentionValue,
			RetentionModRevision: int64(index + 101),
		}
	}
	revision, err := repository.pruneExpired(
		context.Background(),
		testDirectMarker().RetainUntil.Add(time.Nanosecond),
		candidates,
	)
	if err != nil || revision != 80 {
		t.Fatalf("pruneExpired(24) = %d, %v", revision, err)
	}
	if len(backend.transaction.conditions) != 48 || len(backend.transaction.operations) != 48 {
		t.Fatalf("prune transaction conditions/operations = %d/%d", len(backend.transaction.conditions), len(backend.transaction.operations))
	}
	for _, operation := range backend.transaction.operations {
		if !operation.IsDelete() {
			t.Fatal("prune transaction contains a non-delete operation")
		}
	}

	backend.transaction = nil
	tooMany := append(append([]idempotencyPruneCandidate(nil), candidates...), candidates[0])
	if _, err := repository.pruneExpired(context.Background(), testMarkerTime(), tooMany); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("pruneExpired(25) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("pruneExpired(25) reached etcd")
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
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	_, err = repository.pruneExpired(context.Background(), marker.RetainUntil, []idempotencyPruneCandidate{{
		Marker:       idempotencyEvidence{marker: marker, modRevision: 7},
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
		transactionResults: []TransactionResult{
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
		plan, planErr := newIdempotencyMutationPlan(nil,
			[]Mutation{{Type: MutationPut, Key: fmt.Sprintf("/resource/%d", index), Value: []byte("value")}},
			func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "changed") },
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

type collectorSnapshot struct {
	rangeResult RangeResult
	markers     GetManyResult
}

type collectorTestStore struct {
	Store
	mu                 sync.Mutex
	snapshots          []collectorSnapshot
	transactionResults []TransactionResult
	transactionErrors  []error
	rangeCalls         int
	getManyCalls       int
	transactionCalls   int
	getManyRevisions   []int64
}

func (store *collectorTestStore) Range(ctx context.Context, request RangeRequest) (*RangeResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Prefix != idempotencyRetentionPrefix || request.Limit != maximumPruneMarkers || request.Revision != 0 ||
		store.rangeCalls >= len(store.snapshots) {
		return nil, errs.New(errs.KindInternal, "unexpected collector range")
	}
	snapshot := store.snapshots[store.rangeCalls]
	store.rangeCalls++
	result := snapshot.rangeResult
	result.Values = cloneKeyValueSlice(result.Values)
	return &result, nil
}

func (store *collectorTestStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store.getManyCalls >= len(store.snapshots) || request.Revision != store.snapshots[store.getManyCalls].rangeResult.ReadRevision {
		return nil, errs.New(errs.KindInternal, "unexpected collector multi-get")
	}
	snapshot := store.snapshots[store.getManyCalls]
	store.getManyCalls++
	store.getManyRevisions = append(store.getManyRevisions, request.Revision)
	result := snapshot.markers
	result.Values = cloneKeyValuePointers(result.Values)
	return &result, nil
}

func (store *collectorTestStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return TransactionResult{}, err
	}
	index := store.transactionCalls
	store.transactionCalls++
	if len(conditions) != 2 || len(mutations) != 2 {
		return TransactionResult{}, errs.New(errs.KindInternal, "unexpected collector transaction")
	}
	if index < len(store.transactionErrors) && store.transactionErrors[index] != nil {
		return TransactionResult{}, store.transactionErrors[index]
	}
	if index >= len(store.transactionResults) {
		return TransactionResult{}, errs.New(errs.KindInternal, "missing collector transaction result")
	}
	return store.transactionResults[index], nil
}

type atomicClaimStore struct {
	Store
	mu       sync.Mutex
	revision int64
	marker   *KeyValue
}

func (store *atomicClaimStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return TransactionResult{}, err
	}
	store.revision++
	if store.marker != nil {
		reads := make([]*KeyValue, len(conditions))
		reads[0] = &KeyValue{
			Key: store.marker.Key, Value: append([]byte(nil), store.marker.Value...),
			ModRevision: store.marker.ModRevision,
		}
		return TransactionResult{Revision: store.revision, FailureReads: reads}, nil
	}
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && strings.HasPrefix(mutation.Key, idempotencyMarkerPrefix) {
			store.marker = &KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision,
			}
			break
		}
	}
	if store.marker == nil {
		return TransactionResult{}, errs.New(errs.KindInternal, "claim transaction omitted marker")
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func testCollectorSnapshot(
	t *testing.T,
	marker IdempotencyMarker,
	revision int64,
	retentionRevision int64,
	markerRevision int64,
) collectorSnapshot {
	t.Helper()
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal(retention) error = %v", err)
	}
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	return collectorSnapshot{
		rangeResult: RangeResult{
			Values:       []KeyValue{{Key: retentionKey, Value: retentionValue, ModRevision: retentionRevision}},
			ReadRevision: revision, ResponseRevision: revision,
		},
		markers: GetManyResult{
			Values:       []*KeyValue{{Key: markerKey, Value: markerValue, ModRevision: markerRevision}},
			ReadRevision: revision, ResponseRevision: revision,
		},
	}
}

func cloneKeyValueSlice(values []KeyValue) []KeyValue {
	result := make([]KeyValue, len(values))
	for index, value := range values {
		result[index] = KeyValue{Key: value.Key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision}
	}
	return result
}

func cloneKeyValuePointers(values []*KeyValue) []*KeyValue {
	result := make([]*KeyValue, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = &KeyValue{Key: value.Key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision}
		}
	}
	return result
}

func testDirectMarker() IdempotencyMarker {
	createdAt := testMarkerTime()
	ciphertext := []byte("protected-intent")
	digest := sha256.Sum256(ciphertext)
	return IdempotencyMarker{
		Kind: IdempotencyMarkerDirect, State: IdempotencyMarkerCompleted,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeTenant,
			ScopeID:   ids.NewAt(ids.KindTenant, createdAt, 1),
			Method:    http.MethodPatch, Route: "/projects/{id}", Key: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: IdempotencyResponse{
			Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"prj"}`),
		},
		CreatedAt: createdAt, UpdatedAt: createdAt, TerminalAt: createdAt,
		RetainUntil: createdAt.Add(markerRetention),
	}
}

func testMarkerTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 123456789, time.UTC)
}

func isKind(err error, want errs.Kind) bool {
	got, ok := errs.KindOf(err)
	return ok && got == want
}
