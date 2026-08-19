// Package etcd wraps go.etcd.io/etcd/client/v3 behind a narrow Store
// interface — the single source of truth for desired state, the task
// queue, the encrypted secret store, and platform component settings.
// See mvp.md, "Everything is etcd (locked)". Only internal/controller
// and internal/agent import this package; internal/cli never does.
package etcd

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// EventType distinguishes a put from a delete in a Watch stream.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

type Event struct {
	Key   string
	Value []byte
	Type  EventType
}

// Store is the narrow interface the rest of the codebase depends on —
// swapping the etcd client library, or pointing at a disposable
// containerized etcd for dev, never ripples past this package.
type Store interface {
	Health(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) (map[string][]byte, error)
	Watch(ctx context.Context, prefix string) (<-chan Event, error)

	// Snapshot writes a full etcd snapshot to w — the DR export path
	// (paired with the controller age key, per mvp.md's DR story).
	Snapshot(ctx context.Context, w io.Writer) error

	Close() error
}

// New dials the etcd cluster named in controller.yaml's etcd.endpoints.
//
// TODO: wire go.etcd.io/etcd/client/v3. Sketch:
//
//	cli, err := clientv3.New(clientv3.Config{Context: ctx, Endpoints: endpoints})
//	return &etcdStore{cli: cli, prefix: keyPrefix}, err
func New(ctx context.Context, endpoints []string, keyPrefix string) (Store, error) {
	return nil, errs.New(errs.CodeNotImplemented, "etcd: not implemented — wire go.etcd.io/etcd/client/v3 in internal/infra/etcd")
}
