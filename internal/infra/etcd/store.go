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
	Key         string
	Value       []byte
	Type        EventType
	ModRevision int64
}

// KeyValue is one logical key read at a known modification revision.
type KeyValue struct {
	Key         string
	Value       []byte
	ModRevision int64
}

// GetResult preserves the revision of an exact read even when Entry is nil.
// This lets a repository follow an absent key without a read-to-watch gap.
type GetResult struct {
	Entry        *KeyValue
	ReadRevision int64
}

// Range is a consistent, key-ordered prefix read. ReadRevision+1 can be passed
// as the start revision to Watch so no mutation is lost between listing and
// starting reconciliation.
type Range struct {
	Values       []KeyValue
	ReadRevision int64
}

// Condition requires Key's current modification revision to equal
// ModRevision. A zero ModRevision means that the key must not exist.
type Condition struct {
	Key         string
	ModRevision int64
}

// MutationType distinguishes the two writes supported inside a transaction.
type MutationType uint8

const (
	MutationPut MutationType = iota + 1
	MutationDelete
)

// Mutation is one atomic write. Value is used only by MutationPut.
type Mutation struct {
	Type  MutationType
	Key   string
	Value []byte
}

// TransactionResult reports whether all conditions matched and the etcd
// revision at which the transaction was evaluated. Failed conditions perform
// no mutations and are not errors.
type TransactionResult struct {
	Succeeded bool
	Revision  int64
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
	Get(ctx context.Context, key string) (*GetResult, error)
	Put(ctx context.Context, key string, value []byte) (int64, error)
	Delete(ctx context.Context, key string) (int64, error)
	List(ctx context.Context, prefix string) (*Range, error)
	Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (TransactionResult, error)

	// Watch starts at startRevision when it is positive. A zero revision uses
	// etcd's current-watch semantics. After List, pass ReadRevision+1 to close
	// the read-to-watch race.
	Watch(ctx context.Context, prefix string, startRevision int64) (*WatchStream, error)

	// Snapshot writes the complete etcd snapshot to w. The deployment contract
	// uses a dedicated single-node etcd, so this is the complete DR state export.
	Snapshot(ctx context.Context, w io.Writer) error

	Close() error
}

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

func (s *store) Get(ctx context.Context, key string) (*GetResult, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, err
	}

	response, err := s.client.Get(ctx, physical)
	if err != nil {
		return nil, wrap(err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.CodeInternal, "etcd get response is missing its read revision")
	}
	result := &GetResult{ReadRevision: response.Header.Revision}
	if len(response.Kvs) == 0 {
		return result, nil
	}
	item := response.Kvs[0]
	logical, ok := s.logicalKey(string(item.Key))
	if !ok {
		return nil, errs.New(errs.CodeInternal, "etcd returned a key outside the configured prefix")
	}
	result.Entry = &KeyValue{
		Key:         logical,
		Value:       append([]byte(nil), item.Value...),
		ModRevision: item.ModRevision,
	}
	return result, nil
}

func (s *store) Put(ctx context.Context, key string, value []byte) (int64, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Put(ctx, physical, string(value))
	if err != nil {
		return 0, wrap(err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.CodeInternal, "etcd put response is missing its revision")
	}
	return response.Header.Revision, nil
}

func (s *store) Delete(ctx context.Context, key string) (int64, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Delete(ctx, physical)
	if err != nil {
		return 0, wrap(err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.CodeInternal, "etcd delete response is missing its revision")
	}
	return response.Header.Revision, nil
}

func (s *store) List(ctx context.Context, prefix string) (*Range, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}

	response, err := s.client.Get(
		ctx,
		physical,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, wrap(err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.CodeInternal, "etcd list response is missing its read revision")
	}

	result := &Range{
		Values:       make([]KeyValue, 0, len(response.Kvs)),
		ReadRevision: response.Header.Revision,
	}
	for _, item := range response.Kvs {
		key, ok := s.logicalKey(string(item.Key))
		if !ok {
			return nil, errs.New(errs.CodeInternal, "etcd returned a key outside the configured prefix")
		}
		result.Values = append(result.Values, KeyValue{
			Key:         key,
			Value:       append([]byte(nil), item.Value...),
			ModRevision: item.ModRevision,
		})
	}
	return result, nil
}

func (s *store) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if len(mutations) == 0 {
		return TransactionResult{}, errs.New(errs.CodeValidationFailed, "etcd transaction requires a mutation")
	}

	comparisons := make([]clientv3.Cmp, 0, len(conditions))
	for _, condition := range conditions {
		if condition.ModRevision < 0 {
			return TransactionResult{}, errs.New(
				errs.CodeValidationFailed,
				"etcd transaction revisions must not be negative",
			)
		}
		key, err := s.physicalKey(condition.Key)
		if err != nil {
			return TransactionResult{}, err
		}
		comparisons = append(
			comparisons,
			clientv3.Compare(clientv3.ModRevision(key), "=", condition.ModRevision),
		)
	}

	operations := make([]clientv3.Op, 0, len(mutations))
	for _, mutation := range mutations {
		key, err := s.physicalKey(mutation.Key)
		if err != nil {
			return TransactionResult{}, err
		}
		switch mutation.Type {
		case MutationPut:
			operations = append(operations, clientv3.OpPut(key, string(mutation.Value)))
		case MutationDelete:
			operations = append(operations, clientv3.OpDelete(key))
		default:
			return TransactionResult{}, errs.New(errs.CodeValidationFailed, "invalid etcd transaction mutation")
		}
	}

	transaction := s.client.Txn(ctx)
	if len(comparisons) > 0 {
		transaction = transaction.If(comparisons...)
	}
	response, err := transaction.Then(operations...).Commit()
	if err != nil {
		return TransactionResult{}, wrap(err)
	}
	if response.Header == nil {
		return TransactionResult{}, errs.New(errs.CodeInternal, "etcd transaction response is missing its revision")
	}
	return TransactionResult{Succeeded: response.Succeeded, Revision: response.Header.Revision}, nil
}

func (s *store) Watch(ctx context.Context, prefix string, startRevision int64) (*WatchStream, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}
	if startRevision < 0 {
		return nil, errs.New(errs.CodeValidationFailed, "etcd watch revision must not be negative")
	}

	options := []clientv3.OpOption{clientv3.WithPrefix()}
	if startRevision > 0 {
		options = append(options, clientv3.WithRev(startRevision))
	}
	upstream := s.client.Watch(ctx, physical, options...)
	events := make(chan Event)
	watchErrors := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(watchErrors)
		for response := range upstream {
			if err := response.Err(); err != nil {
				deliverWatchError(ctx, watchErrors, wrap(err))
				return
			}
			for _, item := range response.Events {
				if item == nil || item.Kv == nil {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.CodeInternal, "etcd watch returned an empty event"),
					)
					return
				}
				key, ok := s.logicalKey(string(item.Kv.Key))
				if !ok {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.CodeInternal, "etcd watch returned a key outside the configured prefix"),
					)
					return
				}
				event := Event{
					Key:         key,
					Value:       append([]byte(nil), item.Kv.Value...),
					ModRevision: item.Kv.ModRevision,
				}
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

	return &WatchStream{Events: events, Errors: watchErrors}, nil
}

func deliverWatchError(ctx context.Context, destination chan<- error, err error) {
	select {
	case destination <- err:
	case <-ctx.Done():
	}
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
