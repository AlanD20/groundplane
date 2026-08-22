package etcd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStoreScopesCRUDAndRangeKeys(t *testing.T) {
	// Rationale: the configured key prefix must isolate every ordinary etcd
	// operation while remaining invisible to Controller repository callers.
	backend := &fakeClient{}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	backend.getResponse = &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 8},
		Kvs: []*mvccpb.KeyValue{{
			Key: []byte("/groundplane/tasks/task_1"), Value: []byte("task"), ModRevision: 7,
		}},
	}
	value, err := store.Get(context.Background(), "/tasks/task_1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if backend.getKey != "/groundplane/tasks/task_1" || value.ReadRevision != 8 ||
		value.Entry == nil || value.Entry.Key != "/tasks/task_1" ||
		string(value.Entry.Value) != "task" || value.Entry.ModRevision != 7 {
		t.Fatalf("Get() key/value = %q/%#v", backend.getKey, value)
	}

	backend.putResponse = &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 8}}
	putRevision, err := store.Put(context.Background(), "/tasks/task_1", []byte("updated"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if backend.putKey != "/groundplane/tasks/task_1" || backend.putValue != "updated" || putRevision != 8 {
		t.Fatalf("Put() key/value/revision = %q/%q/%d", backend.putKey, backend.putValue, putRevision)
	}

	backend.deleteResponse = &clientv3.DeleteResponse{Header: &etcdserverpb.ResponseHeader{Revision: 9}}
	deleteRevision, err := store.Delete(context.Background(), "/tasks/task_1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if backend.deleteKey != "/groundplane/tasks/task_1" || deleteRevision != 9 {
		t.Fatalf("Delete() key/revision = %q/%d", backend.deleteKey, deleteRevision)
	}

	backend.getResponse = &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 12},
		Kvs: []*mvccpb.KeyValue{
			{Key: []byte("/groundplane/tasks/task_1"), Value: []byte("one"), ModRevision: 10},
			{Key: []byte("/groundplane/tasks/task_2"), Value: []byte("two"), ModRevision: 11},
		},
	}
	values, err := store.Range(context.Background(), RangeRequest{
		Prefix:         "/tasks/",
		StartExclusive: "/tasks/task_0",
		Limit:          2,
		Revision:       11,
	})
	if err != nil {
		t.Fatalf("Range() error = %v", err)
	}
	operation := clientv3.OpGet(backend.getKey, backend.getOptions...)
	if backend.getKey != "/groundplane/tasks/task_0\x00" ||
		string(operation.RangeBytes()) != clientv3.GetPrefixRangeEnd("/groundplane/tasks/") ||
		operation.Limit() != 2 || operation.Rev() != 11 {
		t.Fatalf(
			"Range() key/end/limit/revision = %q/%q/%d/%d",
			backend.getKey,
			operation.RangeBytes(),
			operation.Limit(),
			operation.Rev(),
		)
	}
	if values.ReadRevision != 11 || values.ResponseRevision != 12 || values.More || len(values.Values) != 2 ||
		values.Values[0].Key != "/tasks/task_1" || string(values.Values[0].Value) != "one" ||
		values.Values[0].ModRevision != 10 || values.Values[1].Key != "/tasks/task_2" {
		t.Fatalf("Range() values = %#v", values)
	}
}

func TestStoreRangeRejectsInvalidInputs(t *testing.T) {
	// Rationale: a low-level range must remain explicitly bounded and scoped;
	// invalid cursor mechanics must fail before reaching etcd.
	backend := &fakeClient{}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	cases := []RangeRequest{
		{Prefix: "/tasks/", Limit: 0},
		{Prefix: "/tasks/", Limit: -1},
		{Prefix: "/tasks/", Limit: 1, Revision: -1},
		{Prefix: "tasks/", Limit: 1},
		{Prefix: "/tasks/", StartExclusive: "tasks/task_1", Limit: 1},
		{Prefix: "/tasks/", StartExclusive: "/agents/agent_1", Limit: 1},
	}
	for _, request := range cases {
		if _, err := store.Range(context.Background(), request); err == nil {
			t.Fatalf("Range(%#v) error = nil", request)
		}
	}
}

func TestStoreGetManyUsesOneHistoricalRevision(t *testing.T) {
	// Rationale: index pagination must hydrate every primary from exactly one
	// MVCC view while retaining request order and explicit missing entries.
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 18},
		Responses: []*etcdserverpb.ResponseOp{
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
				Kvs: []*mvccpb.KeyValue{{
					Key: []byte("/groundplane/services/svc_1"), Value: []byte("one"), ModRevision: 10,
				}},
			}}},
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}}},
		},
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	result, err := store.GetMany(context.Background(), GetManyRequest{
		Keys:     []string{"/services/svc_1", "/services/svc_2"},
		Revision: 14,
	})
	if err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}
	if result.ReadRevision != 14 || result.ResponseRevision != 18 || len(result.Values) != 2 ||
		result.Values[0] == nil || result.Values[0].Key != "/services/svc_1" ||
		string(result.Values[0].Value) != "one" || result.Values[0].ModRevision != 10 ||
		result.Values[1] != nil {
		t.Fatalf("GetMany() result = %#v", result)
	}
	if len(backend.transaction.operations) != 2 {
		t.Fatalf("GetMany() operations = %#v", backend.transaction.operations)
	}
	for index, operation := range backend.transaction.operations {
		if operation.Rev() != 14 ||
			string(operation.KeyBytes()) != "/groundplane/services/svc_"+string(rune('1'+index)) {
			t.Fatalf("GetMany() operation %d = key %q revision %d", index, operation.KeyBytes(), operation.Rev())
		}
	}
}

func TestStoreGetManyRejectsInvalidInputs(t *testing.T) {
	// Rationale: multi-get must reject empty batches, invalid revisions, and
	// unscoped keys rather than issuing ambiguous or partially scoped reads.
	backend := &fakeClient{}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	cases := []GetManyRequest{
		{},
		{Keys: []string{"/services/svc_1"}, Revision: -1},
		{Keys: []string{"services/svc_1"}},
	}
	for _, request := range cases {
		if _, err := store.GetMany(context.Background(), request); err == nil {
			t.Fatalf("GetMany(%#v) error = nil", request)
		}
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	// Rationale: the Store interface has no found boolean, so absence must have
	// one deterministic representation that repositories can map by resource.
	backend := &fakeClient{getResponse: &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 14},
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	value, err := store.Get(context.Background(), "/missing")
	if err != nil || value == nil || value.Entry != nil || value.ReadRevision != 14 {
		t.Fatalf("Get() = %#v, %v; want absent entry at revision 14", value, err)
	}
}

func TestStoreWatchTranslatesLogicalEvents(t *testing.T) {
	// Rationale: reconciliation consumes logical keys and must not learn the
	// deployment-specific etcd prefix or etcd protobuf event types.
	backend := &fakeClient{watchResponses: make(chan clientv3.WatchResponse, 1)}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	stream, err := store.Watch(context.Background(), "/tasks/", 13)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	backend.watchResponses <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
			Key: []byte("/groundplane/tasks/task_1"), Value: []byte("one"), ModRevision: 13,
		}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/groundplane/tasks/task_2")}},
	}}
	close(backend.watchResponses)

	got := make([]Event, 0, 2)
	for event := range stream.Events {
		got = append(got, event)
	}
	if watchErr, ok := <-stream.Errors; ok {
		t.Fatalf("Watch() terminal error = %v", watchErr)
	}
	if backend.watchKey != "/groundplane/tasks/" || !clientv3.IsOptsWithPrefix(backend.watchOptions) {
		t.Fatalf("Watch() key/prefix option = %q/%v", backend.watchKey, clientv3.IsOptsWithPrefix(backend.watchOptions))
	}
	if revision := clientv3.OpGet("key", backend.watchOptions...).Rev(); revision != 13 {
		t.Fatalf("Watch() start revision = %d, want 13", revision)
	}
	if len(got) != 2 || got[0].Key != "/tasks/task_1" || got[0].Type != EventPut ||
		got[0].ModRevision != 13 || got[1].Key != "/tasks/task_2" || got[1].Type != EventDelete {
		t.Fatalf("Watch() events = %#v", got)
	}
}

func TestStoreWatchReportsTerminalErrors(t *testing.T) {
	// Rationale: compaction and transport failures must never look like a clean
	// end of reconciliation; callers need an explicit terminal error signal.
	backend := &fakeClient{watchResponses: make(chan clientv3.WatchResponse, 1)}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	stream, err := store.Watch(context.Background(), "/tasks/", 0)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	backend.watchResponses <- clientv3.WatchResponse{Canceled: true, CompactRevision: 12}
	close(backend.watchResponses)

	watchErr, ok := <-stream.Errors
	if !ok || watchErr == nil {
		t.Fatal("Watch() terminal error = nil; want compaction error")
	}
	var groundplaneError *errs.Error
	if !errors.As(watchErr, &groundplaneError) || groundplaneError.Code != errs.CodeCursorExpired {
		t.Fatalf("Watch() terminal error = %#v; want cursor.expired Groundplane error", watchErr)
	}
	if _, ok := <-stream.Events; ok {
		t.Fatal("Watch() events channel remained open after terminal error")
	}
}

func TestStoreWatchRejectsEventsOutsideConfiguredPrefix(t *testing.T) {
	// Rationale: a prefix leak is an isolation failure, not an event that a
	// reconciler may silently skip while believing its watch is complete.
	backend := &fakeClient{watchResponses: make(chan clientv3.WatchResponse, 1)}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	stream, err := store.Watch(context.Background(), "/tasks/", 0)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	backend.watchResponses <- clientv3.WatchResponse{Events: []*clientv3.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/other/tasks/task_1")},
	}}}
	close(backend.watchResponses)

	watchErr, ok := <-stream.Errors
	if !ok || watchErr == nil {
		t.Fatal("Watch() terminal error = nil; want prefix isolation error")
	}
	if _, ok := <-stream.Events; ok {
		t.Fatal("Watch() events channel remained open after prefix isolation error")
	}
}

func TestStoreTransactScopesAtomicCompareAndMutations(t *testing.T) {
	// Rationale: token consumption and uniqueness reservations require one
	// compare-and-mutate commit, not a racy read followed by independent writes.
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{Revision: 22},
		Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: "/tokens/join_1", ModRevision: 19}},
		[]Mutation{
			{Type: MutationDelete, Key: "/tokens/join_1"},
			{Type: MutationPut, Key: "/agents/agent_1", Value: []byte("agent")},
		},
	)
	if err != nil {
		t.Fatalf("Transact() error = %v", err)
	}
	if !result.Succeeded || result.Revision != 22 {
		t.Fatalf("Transact() result = %#v", result)
	}
	if len(backend.transaction.conditions) != 1 ||
		string(backend.transaction.conditions[0].KeyBytes()) != "/groundplane/tokens/join_1" {
		t.Fatalf("Transact() conditions = %#v", backend.transaction.conditions)
	}
	if len(backend.transaction.operations) != 2 || !backend.transaction.operations[0].IsDelete() ||
		string(backend.transaction.operations[0].KeyBytes()) != "/groundplane/tokens/join_1" ||
		!backend.transaction.operations[1].IsPut() ||
		string(backend.transaction.operations[1].KeyBytes()) != "/groundplane/agents/agent_1" ||
		string(backend.transaction.operations[1].ValueBytes()) != "agent" {
		t.Fatalf("Transact() operations = %#v", backend.transaction.operations)
	}
}

func TestStoreTransactScopesPrefixDelete(t *testing.T) {
	// Rationale: deleting an Entry must remove every immutable subordinate
	// value generation in the same transaction without imposing an artificial
	// limit on the number of edits retained for Task retry.
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 23}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	_, err = store.Transact(
		context.Background(),
		nil,
		[]Mutation{{Type: MutationDelete, Key: "/v1/secret-values/entries/ev_1/", Prefix: true}},
	)
	if err != nil {
		t.Fatalf("Transact() error = %v", err)
	}
	if len(backend.transaction.operations) != 1 || !backend.transaction.operations[0].IsDelete() {
		t.Fatalf("Transact() operations = %#v", backend.transaction.operations)
	}
	operation := backend.transaction.operations[0]
	wantKey := "/groundplane/v1/secret-values/entries/ev_1/"
	if string(operation.KeyBytes()) != wantKey ||
		string(operation.RangeBytes()) != clientv3.GetPrefixRangeEnd(wantKey) {
		t.Fatalf("prefix delete = key %q range %q", operation.KeyBytes(), operation.RangeBytes())
	}
}

func TestStoreSeparatesCallerContextFromBackendStatus(t *testing.T) {
	// Rationale: only the caller context decides caller cancellation; an
	// independent gRPC outage is retryable storage state, while a plain
	// dependency context sentinel has no trusted transport classification.
	for _, test := range []struct {
		name    string
		ctx     func() context.Context
		backend error
		want    error
	}{
		{
			name: "caller canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			backend: status.Error(codes.Unavailable, "backend secret"),
			want:    context.Canceled,
		},
		{
			name: "caller deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				t.Cleanup(cancel)
				return ctx
			},
			backend: status.Error(codes.Unavailable, "backend secret"),
			want:    context.DeadlineExceeded,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := newStore(&fakeClient{getError: test.backend}, "/groundplane/")
			if err != nil {
				t.Fatalf("newStore() error = %v", err)
			}
			_, err = store.Get(test.ctx(), "/tasks/task_1")
			if !errors.Is(err, test.want) {
				t.Fatalf("Get() error = %v, want %v", err, test.want)
			}
			var domainError *errs.Error
			if errors.As(err, &domainError) {
				t.Fatalf("caller context was wrapped as %#v", domainError)
			}
		})
	}

	for _, backend := range []error{
		status.Error(codes.Unavailable, "password=secret"),
		status.Error(codes.DeadlineExceeded, "password=secret"),
	} {
		store, err := newStore(&fakeClient{getError: backend}, "/groundplane/")
		if err != nil {
			t.Fatalf("newStore() error = %v", err)
		}
		_, err = store.Get(context.Background(), "/tasks/task_1")
		if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || !errors.Is(err, backend) {
			t.Fatalf("backend error = %v", err)
		}
		var domainError *errs.Error
		if !errors.As(err, &domainError) || strings.Contains(domainError.ToProblem().Detail, "secret") {
			t.Fatalf("backend problem = %#v", domainError)
		}
	}

	store, err := newStore(&fakeClient{getError: context.DeadlineExceeded}, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	_, err = store.Get(context.Background(), "/tasks/task_1")
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("plain dependency deadline = %v, want internal", err)
	}
}

func TestStoreSnapshotStreamsAndCloses(t *testing.T) {
	// Rationale: DR snapshots may be large, so the adapter must stream them to
	// the caller and release the etcd response body after the copy completes.
	reader := &trackingReadCloser{Reader: bytes.NewBufferString("snapshot")}
	backend := &fakeClient{snapshotReader: reader}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	var destination bytes.Buffer
	if err := store.Snapshot(context.Background(), &destination); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if destination.String() != "snapshot" || !reader.closed {
		t.Fatalf("Snapshot() destination/closed = %q/%v", destination.String(), reader.closed)
	}
}

type fakeClient struct {
	getKey              string
	getOptions          []clientv3.OpOption
	getResponse         *clientv3.GetResponse
	getError            error
	putKey              string
	putValue            string
	putResponse         *clientv3.PutResponse
	putError            error
	deleteKey           string
	deleteResponse      *clientv3.DeleteResponse
	deleteError         error
	transaction         *fakeTransaction
	transactionResponse *clientv3.TxnResponse
	transactionError    error
	watchKey            string
	watchOptions        []clientv3.OpOption
	watchResponses      chan clientv3.WatchResponse
	snapshotReader      io.ReadCloser
	snapshotError       error
	closeError          error
}

func (f *fakeClient) Get(_ context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.getKey = key
	f.getOptions = opts
	if f.getResponse == nil {
		f.getResponse = &clientv3.GetResponse{}
	}
	return f.getResponse, f.getError
}

func (f *fakeClient) Put(
	_ context.Context,
	key string,
	value string,
	_ ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	f.putKey = key
	f.putValue = value
	return f.putResponse, f.putError
}

func (f *fakeClient) Delete(
	_ context.Context,
	key string,
	_ ...clientv3.OpOption,
) (*clientv3.DeleteResponse, error) {
	f.deleteKey = key
	return f.deleteResponse, f.deleteError
}

func (f *fakeClient) Txn(context.Context) clientv3.Txn {
	f.transaction = &fakeTransaction{response: f.transactionResponse, err: f.transactionError}
	return f.transaction
}

func (f *fakeClient) Watch(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	f.watchKey = key
	f.watchOptions = opts
	return f.watchResponses
}

func (f *fakeClient) Snapshot(context.Context) (io.ReadCloser, error) {
	return f.snapshotReader, f.snapshotError
}

func (f *fakeClient) Close() error {
	return f.closeError
}

type fakeTransaction struct {
	conditions []clientv3.Cmp
	operations []clientv3.Op
	otherwise  []clientv3.Op
	response   *clientv3.TxnResponse
	err        error
}

func (t *fakeTransaction) If(conditions ...clientv3.Cmp) clientv3.Txn {
	t.conditions = append(t.conditions, conditions...)
	return t
}

func (t *fakeTransaction) Then(operations ...clientv3.Op) clientv3.Txn {
	t.operations = append(t.operations, operations...)
	return t
}

func (t *fakeTransaction) Else(operations ...clientv3.Op) clientv3.Txn {
	t.otherwise = append(t.otherwise, operations...)
	return t
}

func (t *fakeTransaction) Commit() (*clientv3.TxnResponse, error) {
	return t.response, t.err
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}
