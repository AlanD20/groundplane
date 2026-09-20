// Package etcd isolates the official etcd v3 client behind Groundplane's
// narrow persistence interface. Logical keys always begin with slash and are
// stored beneath the configured Controller key prefix.
package etcd

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
)

type client interface {
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Txn(context.Context) clientv3.Txn
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
func New(ctx context.Context, endpoints []string, keyPrefix string) (*store, error) {
	if err := validateConfig(endpoints, keyPrefix); err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientv3.Config{
		Context:   ctx,
		Endpoints: append([]string(nil), endpoints...),
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return newStore(cli, keyPrefix)
}
func newStore(cli client, keyPrefix string) (*store, error) {
	if cli == nil {
		return nil, errs.New(errs.KindValidationFailed, "etcd client is required")
	}
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return nil, errs.New(errs.KindValidationFailed, "etcd key prefix must begin and end with /")
	}
	return &store{client: cli, root: strings.TrimSuffix(keyPrefix, "/")}, nil
}
func validateConfig(endpoints []string, keyPrefix string) error {
	if len(endpoints) == 0 {
		return errs.New(errs.KindValidationFailed, "at least one etcd endpoint is required")
	}
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint) == "" {
			return errs.New(errs.KindValidationFailed, "etcd endpoints must not be empty")
		}
	}
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return errs.New(errs.KindValidationFailed, "etcd key prefix must begin and end with /")
	}
	return nil
}
func (s *store) Health(ctx context.Context) error {
	// Default etcd reads are linearizable. Reading a deliberately absent key
	// proves that the cluster can serve a consistent request without mutating it.
	_, err := s.client.Get(ctx, s.root+"/.health", clientv3.WithLimit(1))
	return wrap(ctx, err)
}

func (s *store) Close() error {
	return wrap(context.Background(), s.client.Close())
}

func (s *store) physicalKey(key string) (string, error) {
	if key == "" || !strings.HasPrefix(key, "/") {
		return "", errs.New(errs.KindValidationFailed, "etcd logical keys must begin with /")
	}
	return s.root + key, nil
}

func (s *store) logicalKey(key string) (string, bool) {
	if !strings.HasPrefix(key, s.root+"/") {
		return "", false
	}
	return strings.TrimPrefix(key, s.root), true
}

func wrap(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if errors.Is(err, rpctypes.ErrCompacted) {
		return errs.Wrap(errs.KindCursorExpired, err)
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return errs.Wrap(errs.KindInternal, err)
}
