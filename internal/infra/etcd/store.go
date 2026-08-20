// Package etcd isolates the official etcd v3 client behind Groundplane's
// narrow persistence interface. Logical keys always begin with slash and are
// stored beneath the configured Controller key prefix.
package etcd

import (
	"context"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// WatchStream separates ordinary key events from terminal watch failures.
// Consumers must observe Errors and restart from a durable revision once that
// resume contract is defined by the repository layer.
type WatchStream struct {
	Events <-chan Event
	Errors <-chan error
}

// Store is the persistence interface used by the Controller and Agent. A
// missing exact key is returned as (nil, nil); deleting a missing key is
// idempotent. List and Watch return logical keys with the configured storage
// prefix removed.
type Store interface {
	Health(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) (map[string][]byte, error)
	Watch(ctx context.Context, prefix string) (*WatchStream, error)

	// Snapshot writes the complete etcd snapshot to w. The deployment contract
	// uses a dedicated single-node etcd, so this is the complete DR state export.
	Snapshot(ctx context.Context, w io.Writer) error

	Close() error
}

type client interface {
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan
	Snapshot(context.Context) (io.ReadCloser, error)
	Close() error
}

type store struct {
	client client
	root   string
}

// New connects to the etcd cluster named in controller.yaml and scopes every
// ordinary key operation beneath keyPrefix. Snapshot intentionally remains a
// cluster operation: the MVP's etcd instance is dedicated to Groundplane.
func New(ctx context.Context, endpoints []string, keyPrefix string) (Store, error) {
	if err := validateConfig(endpoints, keyPrefix); err != nil {
		return nil, err
	}

	cli, err := clientv3.New(clientv3.Config{
		Context:   ctx,
		Endpoints: append([]string(nil), endpoints...),
	})
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, err)
	}

	return newStore(cli, keyPrefix)
}

func newStore(cli client, keyPrefix string) (*store, error) {
	if cli == nil {
		return nil, errs.New(errs.CodeValidationFailed, "etcd client is required")
	}
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return nil, errs.New(errs.CodeValidationFailed, "etcd key prefix must begin and end with /")
	}

	return &store{client: cli, root: strings.TrimSuffix(keyPrefix, "/")}, nil
}

func validateConfig(endpoints []string, keyPrefix string) error {
	if len(endpoints) == 0 {
		return errs.New(errs.CodeValidationFailed, "at least one etcd endpoint is required")
	}
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint) == "" {
			return errs.New(errs.CodeValidationFailed, "etcd endpoints must not be empty")
		}
	}
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return errs.New(errs.CodeValidationFailed, "etcd key prefix must begin and end with /")
	}
	return nil
}

func (s *store) Health(ctx context.Context) error {
	// Default etcd reads are linearizable. Reading a deliberately absent key
	// proves that the cluster can serve a consistent request without mutating it.
	_, err := s.client.Get(ctx, s.root+"/.health", clientv3.WithLimit(1))
	return wrap(err)
}

func (s *store) Get(ctx context.Context, key string) ([]byte, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, err
	}

	response, err := s.client.Get(ctx, physical)
	if err != nil {
		return nil, wrap(err)
	}
	if len(response.Kvs) == 0 {
		return nil, nil
	}
	return append([]byte(nil), response.Kvs[0].Value...), nil
}

func (s *store) Put(ctx context.Context, key string, value []byte) error {
	physical, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, physical, string(value))
	return wrap(err)
}

func (s *store) Delete(ctx context.Context, key string) error {
	physical, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	_, err = s.client.Delete(ctx, physical)
	return wrap(err)
}

func (s *store) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}

	response, err := s.client.Get(ctx, physical, clientv3.WithPrefix())
	if err != nil {
		return nil, wrap(err)
	}

	values := make(map[string][]byte, len(response.Kvs))
	for _, item := range response.Kvs {
		key, ok := s.logicalKey(string(item.Key))
		if !ok {
			continue
		}
		values[key] = append([]byte(nil), item.Value...)
	}
	return values, nil
}

func (s *store) Watch(ctx context.Context, prefix string) (*WatchStream, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}

	upstream := s.client.Watch(ctx, physical, clientv3.WithPrefix())
	events := make(chan Event)
	errors := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(errors)
		for response := range upstream {
			if err := response.Err(); err != nil {
				select {
				case errors <- wrap(err):
				case <-ctx.Done():
				}
				return
			}
			for _, item := range response.Events {
				key, ok := s.logicalKey(string(item.Kv.Key))
				if !ok {
					continue
				}
				event := Event{Key: key, Value: append([]byte(nil), item.Kv.Value...)}
				switch item.Type {
				case mvccpb.PUT:
					event.Type = EventPut
				case mvccpb.DELETE:
					event.Type = EventDelete
				default:
					continue
				}

				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return &WatchStream{Events: events, Errors: errors}, nil
}

func (s *store) Snapshot(ctx context.Context, w io.Writer) error {
	if w == nil {
		return errs.New(errs.CodeValidationFailed, "snapshot writer is required")
	}

	reader, err := s.client.Snapshot(ctx)
	if err != nil {
		return wrap(err)
	}
	_, copyErr := io.Copy(w, reader)
	closeErr := reader.Close()
	if copyErr != nil {
		return wrap(copyErr)
	}
	return wrap(closeErr)
}

func (s *store) Close() error {
	return wrap(s.client.Close())
}

func (s *store) physicalKey(key string) (string, error) {
	if key == "" || !strings.HasPrefix(key, "/") {
		return "", errs.New(errs.CodeValidationFailed, "etcd logical keys must begin with /")
	}
	return s.root + key, nil
}

func (s *store) logicalKey(key string) (string, bool) {
	if !strings.HasPrefix(key, s.root+"/") {
		return "", false
	}
	return strings.TrimPrefix(key, s.root), true
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.CodeInternal, err)
}
