package etcd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStoreScopesCRUDAndListKeys(t *testing.T) {
	// Rationale: the configured key prefix must isolate every ordinary etcd
	// operation while remaining invisible to Controller repository callers.
	backend := &fakeClient{}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	backend.getResponse = &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Value: []byte("task")}}}
	value, err := store.Get(context.Background(), "/tasks/task_1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if backend.getKey != "/groundplane/tasks/task_1" || string(value) != "task" {
		t.Fatalf("Get() key/value = %q/%q", backend.getKey, value)
	}

	if err := store.Put(context.Background(), "/tasks/task_1", []byte("updated")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if backend.putKey != "/groundplane/tasks/task_1" || backend.putValue != "updated" {
		t.Fatalf("Put() key/value = %q/%q", backend.putKey, backend.putValue)
	}

	if err := store.Delete(context.Background(), "/tasks/task_1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if backend.deleteKey != "/groundplane/tasks/task_1" {
		t.Fatalf("Delete() key = %q", backend.deleteKey)
	}

	backend.getResponse = &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
		{Key: []byte("/groundplane/tasks/task_1"), Value: []byte("one")},
		{Key: []byte("/groundplane/tasks/task_2"), Value: []byte("two")},
	}}
	values, err := store.List(context.Background(), "/tasks/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if backend.getKey != "/groundplane/tasks/" || !clientv3.IsOptsWithPrefix(backend.getOptions) {
		t.Fatalf("List() key/prefix option = %q/%v", backend.getKey, clientv3.IsOptsWithPrefix(backend.getOptions))
	}
	if string(values["/tasks/task_1"]) != "one" || string(values["/tasks/task_2"]) != "two" {
		t.Fatalf("List() values = %#v", values)
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	// Rationale: the Store interface has no found boolean, so absence must have
	// one deterministic representation that repositories can map by resource.
	backend := &fakeClient{getResponse: &clientv3.GetResponse{}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	value, err := store.Get(context.Background(), "/missing")
	if err != nil || value != nil {
		t.Fatalf("Get() = %q, %v; want nil, nil", value, err)
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

	stream, err := store.Watch(context.Background(), "/tasks/")
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	backend.watchResponses <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/groundplane/tasks/task_1"), Value: []byte("one")}},
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
	if len(got) != 2 || got[0].Key != "/tasks/task_1" || got[0].Type != EventPut ||
		got[1].Key != "/tasks/task_2" || got[1].Type != EventDelete {
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

	stream, err := store.Watch(context.Background(), "/tasks/")
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
	if !errors.As(watchErr, &groundplaneError) || groundplaneError.Code != errs.CodeInternal {
		t.Fatalf("Watch() terminal error = %#v; want internal Groundplane error", watchErr)
	}
	if _, ok := <-stream.Events; ok {
		t.Fatal("Watch() events channel remained open after terminal error")
	}
}

func TestStorePreservesContextErrors(t *testing.T) {
	// Rationale: callers must be able to detect cancellation through the one
	// Groundplane error wrapper rather than losing context propagation.
	backend := &fakeClient{getError: context.DeadlineExceeded}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), "/tasks/task_1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get() error = %v; want deadline in error chain", err)
	}
	var groundplaneError *errs.Error
	if !errors.As(err, &groundplaneError) || groundplaneError.Code != errs.CodeInternal {
		t.Fatalf("Get() error = %#v; want internal Groundplane error", err)
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
	getKey         string
	getOptions     []clientv3.OpOption
	getResponse    *clientv3.GetResponse
	getError       error
	putKey         string
	putValue       string
	deleteKey      string
	watchKey       string
	watchOptions   []clientv3.OpOption
	watchResponses chan clientv3.WatchResponse
	snapshotReader io.ReadCloser
	snapshotError  error
	closeError     error
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
	return &clientv3.PutResponse{}, nil
}

func (f *fakeClient) Delete(
	_ context.Context,
	key string,
	_ ...clientv3.OpOption,
) (*clientv3.DeleteResponse, error) {
	f.deleteKey = key
	return &clientv3.DeleteResponse{}, nil
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

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}
