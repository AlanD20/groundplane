// Package etcd isolates the official etcd v3 client behind Groundplane's
// narrow persistence interface. Logical keys always begin with slash and are
// stored beneath the configured Controller key prefix.
package etcd

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	Version     int64
	ModRevision int64
}

// GetResult preserves the revision of an exact read even when Entry is nil.
// This lets a repository follow an absent key without a read-to-watch gap.
type GetResult struct {
	Entry        *KeyValue
	ReadRevision int64
}

// RangeRequest describes a bounded prefix read. StartExclusive, when set,
// must be a logical key within Prefix. Revision zero reads the current view;
// a positive revision reads that historical MVCC view.
type RangeRequest struct {
	Prefix         string
	StartExclusive string
	Limit          int64
	Revision       int64
	Descending     bool
}

// RangeResult is a deterministic key-ascending range page. ReadRevision is
// the MVCC view that produced Values; ResponseRevision is etcd's latest store
// revision when it served the request.
type RangeResult struct {
	Values           []KeyValue
	ReadRevision     int64
	ResponseRevision int64
	More             bool
}

// GetManyRequest reads exact logical keys at one MVCC view. Values in the
// result retain this key order and use nil for a missing key.
type GetManyRequest struct {
	Keys     []string
	Revision int64
}

// GetManyResult preserves both the requested MVCC view and etcd's latest
// response revision. Values has exactly one entry for each requested key.
type GetManyResult struct {
	Values           []*KeyValue
	ReadRevision     int64
	ResponseRevision int64
}

// Condition requires Key's current modification revision to equal
// ModRevision. A zero ModRevision means that the key must not exist. Prefix
// is valid only with zero and requires the complete prefix to remain empty.
type Condition struct {
	Key         string
	ModRevision int64
	Prefix      bool
}

// MutationType distinguishes the two writes supported inside a transaction.
type MutationType uint8

const (
	MutationPut MutationType = iota + 1
	MutationDelete
)

// Mutation is one atomic write. Value is used only by MutationPut.
type Mutation struct {
	Type   MutationType
	Key    string
	Value  []byte
	Prefix bool
}

// TransactionResult reports whether all conditions matched and the etcd
// revision at which the transaction was evaluated. Failed conditions perform
// no mutations and are not errors.
type TransactionResult struct {
	Succeeded    bool
	Revision     int64
	FailureReads []*KeyValue
}

const (
	maximumTransactionOperations                           = 96
	maximumEnvironmentBlueprintTransactionOperationsPerArm = 256
	maximumTransactionBytes                                = 1 << 20
)

// WatchStream separates ordinary key events from terminal watch failures.
// Consumers must observe Errors and restart from a durable revision once that
// resume contract is defined by the repository layer.
type WatchStream struct {
	Events <-chan Event
	Errors <-chan error
}

// Store is the persistence interface used by the Controller and Agent. A
// missing exact key returns a GetResult with a nil Entry and its read revision;
// deleting a missing key is idempotent. Range and Watch return logical keys
// with the configured storage prefix removed.
type Store interface {
	Health(ctx context.Context) error
	Get(ctx context.Context, key string) (*GetResult, error)
	GetMany(ctx context.Context, request GetManyRequest) (*GetManyResult, error)
	Put(ctx context.Context, key string, value []byte) (int64, error)
	Delete(ctx context.Context, key string) (int64, error)
	Range(ctx context.Context, request RangeRequest) (*RangeResult, error)
	Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (TransactionResult, error)
	// Watch starts at startRevision when it is positive. A zero revision uses
	// etcd's current-watch semantics. After Range, pass ReadRevision+1 to close
	// the read-to-watch race.
	Watch(ctx context.Context, prefix string, startRevision int64) (*WatchStream, error)
	// Snapshot writes the complete etcd snapshot to w. The deployment contract
	// uses a dedicated single-node etcd, so this is the complete DR state export.
	Snapshot(ctx context.Context, w io.Writer) error
	Close() error
}

// EnvironmentBlueprintStore owns the privileged final Blueprint transaction
// envelope without widening ordinary Store transactions.
type EnvironmentBlueprintStore interface {
	Store
	TransactEnvironmentBlueprint(
		ctx context.Context,
		conditions []Condition,
		mutations []Mutation,
	) (TransactionResult, error)
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
func New(ctx context.Context, endpoints []string, keyPrefix string) (EnvironmentBlueprintStore, error) {
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
func (s *store) Get(ctx context.Context, key string) (*GetResult, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Get(ctx, physical)
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.KindInternal, "etcd get response is missing its read revision")
	}
	result := &GetResult{ReadRevision: response.Header.Revision}
	if len(response.Kvs) == 0 {
		return result, nil
	}
	item := response.Kvs[0]
	logical, ok := s.logicalKey(string(item.Key))
	if !ok {
		return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
	}
	result.Entry = &KeyValue{
		Key:         logical,
		Value:       append([]byte(nil), item.Value...),
		Version:     item.Version,
		ModRevision: item.ModRevision,
	}
	return result, nil
}
func (s *store) GetMany(ctx context.Context, request GetManyRequest) (*GetManyResult, error) {
	if len(request.Keys) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get requires at least one key")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get revision must not be negative")
	}

	physicalKeys := make([]string, len(request.Keys))
	operations := make([]clientv3.Op, len(request.Keys))
	for index, key := range request.Keys {
		physical, err := s.physicalKey(key)
		if err != nil {
			return nil, err
		}
		physicalKeys[index] = physical
		options := make([]clientv3.OpOption, 0, 1)
		if request.Revision > 0 {
			options = append(options, clientv3.WithRev(request.Revision))
		}
		operations[index] = clientv3.OpGet(physical, options...)
	}

	response, err := s.client.Txn(ctx).Then(operations...).Commit()
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		clearGetManyResponseValues(response)
		return nil, errs.New(errs.KindInternal, "etcd multi-get response is missing its revision")
	}
	if len(response.Responses) != len(request.Keys) {
		clearGetManyResponseValues(response)
		return nil, errs.New(errs.KindInternal, "etcd multi-get returned an unexpected response count")
	}

	result := &GetManyResult{
		Values:           make([]*KeyValue, len(request.Keys)),
		ReadRevision:     readRevision(request.Revision, response.Header.Revision),
		ResponseRevision: response.Header.Revision,
	}
	for index, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil || len(rangeResponse.Kvs) > 1 {
			clearKeyValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd multi-get returned an invalid range response")
		}
		if len(rangeResponse.Kvs) == 0 {
			continue
		}
		item := rangeResponse.Kvs[0]
		if string(item.Key) != physicalKeys[index] {
			clearKeyValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd multi-get returned an unexpected key")
		}
		logical, ok := s.logicalKey(string(item.Key))
		if !ok {
			clearKeyValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
		}
		result.Values[index] = &KeyValue{
			Key:         logical,
			Value:       append([]byte(nil), item.Value...),
			Version:     item.Version,
			ModRevision: item.ModRevision,
		}
	}
	return result, nil
}

// clearGetManyResponseValues releases response buffers on paths where no
// caller can take ownership. Successful reads retain the Store's established
// copy-out behavior and do not mutate the client response.
func clearGetManyResponseValues(response *clientv3.TxnResponse) {
	if response == nil {
		return
	}
	for _, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil {
			continue
		}
		for _, item := range rangeResponse.Kvs {
			if item == nil {
				continue
			}
			clear(item.Value)
			item.Value = nil
		}
	}
}

func (s *store) Put(ctx context.Context, key string, value []byte) (int64, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Put(ctx, physical, string(value))
	if err != nil {
		return 0, wrap(ctx, err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.KindInternal, "etcd put response is missing its revision")
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
		return 0, wrap(ctx, err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.KindInternal, "etcd delete response is missing its revision")
	}
	return response.Header.Revision, nil
}

func (s *store) Range(ctx context.Context, request RangeRequest) (*RangeResult, error) {
	if request.Limit <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range limit must be positive")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range revision must not be negative")
	}

	physicalPrefix, err := s.physicalKey(request.Prefix)
	if err != nil {
		return nil, err
	}
	start := physicalPrefix
	rangeEnd := clientv3.GetPrefixRangeEnd(physicalPrefix)
	if request.StartExclusive != "" {
		if !strings.HasPrefix(request.StartExclusive, request.Prefix) {
			return nil, errs.New(errs.KindValidationFailed, "etcd range start must be within its prefix")
		}
		physicalStart, err := s.physicalKey(request.StartExclusive)
		if err != nil {
			return nil, err
		}
		if request.Descending {
			rangeEnd = physicalStart
		} else {
			start = physicalStart + "\x00"
		}
	}
	direction := clientv3.SortAscend
	if request.Descending {
		direction = clientv3.SortDescend
	}

	options := []clientv3.OpOption{
		clientv3.WithRange(rangeEnd),
		clientv3.WithLimit(request.Limit),
		clientv3.WithSort(clientv3.SortByKey, direction),
	}
	if request.Revision > 0 {
		options = append(options, clientv3.WithRev(request.Revision))
	}
	response, err := s.client.Get(ctx, start, options...)
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.KindInternal, "etcd range response is missing its revision")
	}

	result := &RangeResult{
		Values:           make([]KeyValue, 0, len(response.Kvs)),
		ReadRevision:     readRevision(request.Revision, response.Header.Revision),
		ResponseRevision: response.Header.Revision,
		More:             response.More,
	}
	for _, item := range response.Kvs {
		key, ok := s.logicalKey(string(item.Key))
		if !ok {
			return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
		}
		if !strings.HasPrefix(key, request.Prefix) {
			return nil, errs.New(errs.KindInternal, "etcd range returned a key outside the requested prefix")
		}
		result.Values = append(result.Values, KeyValue{
			Key:         key,
			Value:       append([]byte(nil), item.Value...),
			Version:     item.Version,
			ModRevision: item.ModRevision,
		})
	}
	return result, nil
}

func readRevision(requested, response int64) int64 {
	if requested > 0 {
		return requested
	}
	return response
}

func (s *store) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if len(mutations) == 0 {
		return TransactionResult{}, errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return TransactionResult{}, errs.Newf(
			errs.KindValidationFailed,
			"etcd transaction exceeds the %d compare-and-mutation limit",
			maximumTransactionOperations,
		)
	}
	return s.transact(ctx, conditions, mutations)
}

func (s *store) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return TransactionResult{}, err
	}
	return s.transact(ctx, conditions, mutations)
}

func (s *store) transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {

	comparisons := make([]clientv3.Cmp, 0, len(conditions))
	failureReads := make([]clientv3.Op, 0, len(conditions))
	physicalConditions := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		if condition.ModRevision < 0 {
			return TransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"etcd transaction revisions must not be negative",
			)
		}
		key, err := s.physicalKey(condition.Key)
		if err != nil {
			return TransactionResult{}, err
		}
		comparison := clientv3.Compare(clientv3.ModRevision(key), "=", condition.ModRevision)
		failureRead := clientv3.OpGet(key)
		if condition.Prefix {
			if condition.ModRevision != 0 {
				return TransactionResult{}, errs.New(
					errs.KindValidationFailed,
					"etcd prefix transaction condition requires a zero revision",
				)
			}
			end := clientv3.GetPrefixRangeEnd(key)
			comparison = comparison.WithRange(end)
			failureRead = clientv3.OpGet(key, clientv3.WithRange(end), clientv3.WithLimit(1))
		}
		comparisons = append(comparisons, comparison)
		failureReads = append(failureReads, failureRead)
		physicalConditions = append(physicalConditions, key)
	}

	operations := make([]clientv3.Op, 0, len(mutations))
	physicalMutations := make([]string, 0, len(mutations))
	for _, mutation := range mutations {
		key, err := s.physicalKey(mutation.Key)
		if err != nil {
			return TransactionResult{}, err
		}
		switch mutation.Type {
		case MutationPut:
			if mutation.Prefix {
				return TransactionResult{}, errs.New(
					errs.KindValidationFailed,
					"etcd put mutation must not use prefix semantics",
				)
			}
			operations = append(operations, clientv3.OpPut(key, string(mutation.Value)))
		case MutationDelete:
			if mutation.Prefix {
				operations = append(operations, clientv3.OpDelete(key, clientv3.WithPrefix()))
			} else {
				operations = append(operations, clientv3.OpDelete(key))
			}
		default:
			return TransactionResult{}, errs.New(errs.KindValidationFailed, "invalid etcd transaction mutation")
		}
		physicalMutations = append(physicalMutations, key)
	}
	request := transactionRequest(conditions, mutations, physicalConditions, physicalMutations)
	if request.Size() > maximumTransactionBytes {
		return TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"etcd transaction exceeds the 1 MiB serialized request limit",
		)
	}

	transaction := s.client.Txn(ctx)
	if len(comparisons) > 0 {
		transaction = transaction.If(comparisons...)
	}
	transaction = transaction.Then(operations...)
	if len(failureReads) > 0 {
		transaction = transaction.Else(failureReads...)
	}
	response, err := transaction.Commit()
	if err != nil {
		return TransactionResult{}, wrap(ctx, err)
	}
	if response.Header == nil {
		return TransactionResult{}, errs.New(errs.KindInternal, "etcd transaction response is missing its revision")
	}
	result := TransactionResult{Succeeded: response.Succeeded, Revision: response.Header.Revision}
	if !response.Succeeded {
		reads, err := transactionFailureReads(response, conditions, physicalConditions, s.root)
		if err != nil {
			return TransactionResult{}, err
		}
		result.FailureReads = reads
	}
	return result, nil
}

func validateEnvironmentBlueprintTransactionBudget(conditions []Condition, mutations []Mutation) error {
	if len(conditions) <= maximumEnvironmentBlueprintTransactionOperationsPerArm &&
		len(mutations) <= maximumEnvironmentBlueprintTransactionOperationsPerArm {
		return nil
	}
	return errs.Newf(
		errs.KindValidationFailed,
		"Environment Blueprint publication exceeds a %d-operation transaction arm (%d/%d/%d)",
		maximumEnvironmentBlueprintTransactionOperationsPerArm,
		len(conditions), len(mutations), len(conditions),
	)
}

type environmentBlueprintTransactionStore interface {
	TransactEnvironmentBlueprint(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

func executeEnvironmentBlueprintTransaction(
	ctx context.Context,
	store environmentBlueprintTransactionStore,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store == nil {
		return TransactionResult{}, errs.New(errs.KindInternal, "Environment Blueprint transaction store is required")
	}
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return TransactionResult{}, err
	}
	if len(mutations) == 0 {
		return TransactionResult{}, errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	physicalConditions := make([]string, len(conditions))
	for index := range conditions {
		physicalConditions[index] = conditions[index].Key
	}
	physicalMutations := make([]string, len(mutations))
	for index := range mutations {
		physicalMutations[index] = mutations[index].Key
	}
	if transactionRequest(conditions, mutations, physicalConditions, physicalMutations).Size() > maximumTransactionBytes {
		return TransactionResult{}, errs.New(
			errs.KindValidationFailed, "etcd transaction exceeds the 1 MiB serialized request limit",
		)
	}
	return store.TransactEnvironmentBlueprint(ctx, conditions, mutations)
}

func transactionRequest(
	conditions []Condition,
	mutations []Mutation,
	physicalConditions []string,
	physicalMutations []string,
) *etcdserverpb.TxnRequest {
	request := &etcdserverpb.TxnRequest{
		Compare: make([]*etcdserverpb.Compare, 0, len(conditions)),
		Success: make([]*etcdserverpb.RequestOp, 0, len(mutations)),
		Failure: make([]*etcdserverpb.RequestOp, 0, len(conditions)),
	}
	for index, condition := range conditions {
		comparison := &etcdserverpb.Compare{
			Result:      etcdserverpb.Compare_EQUAL,
			Target:      etcdserverpb.Compare_MOD,
			Key:         []byte(physicalConditions[index]),
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: condition.ModRevision},
		}
		failure := &etcdserverpb.RangeRequest{Key: []byte(physicalConditions[index])}
		if condition.Prefix {
			end := []byte(clientv3.GetPrefixRangeEnd(physicalConditions[index]))
			comparison.RangeEnd = end
			failure.RangeEnd = end
			failure.Limit = 1
		}
		request.Compare = append(request.Compare, comparison)
		request.Failure = append(request.Failure, &etcdserverpb.RequestOp{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: failure},
		})
	}
	for index, mutation := range mutations {
		operation := &etcdserverpb.RequestOp{}
		switch mutation.Type {
		case MutationPut:
			operation.Request = &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
				Key: []byte(physicalMutations[index]), Value: mutation.Value,
			}}
		case MutationDelete:
			request := &etcdserverpb.DeleteRangeRequest{Key: []byte(physicalMutations[index])}
			if mutation.Prefix {
				request.RangeEnd = []byte(clientv3.GetPrefixRangeEnd(physicalMutations[index]))
			}
			operation.Request = &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: request,
			}
		}
		request.Success = append(request.Success, operation)
	}
	return request
}

func transactionFailureReads(
	response *clientv3.TxnResponse,
	conditions []Condition,
	physicalKeys []string,
	root string,
) ([]*KeyValue, error) {
	if len(response.Responses) != len(physicalKeys) || len(conditions) != len(physicalKeys) {
		return nil, errs.New(errs.KindInternal, "etcd transaction failure reads are incomplete")
	}
	values := make([]*KeyValue, len(physicalKeys))
	for index, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil || (!conditions[index].Prefix && rangeResponse.More) || len(rangeResponse.Kvs) > 1 {
			return nil, errs.New(errs.KindInternal, "etcd transaction failure read is invalid")
		}
		if len(rangeResponse.Kvs) == 0 {
			continue
		}
		entry := rangeResponse.Kvs[0]
		matches := string(entry.Key) == physicalKeys[index]
		if conditions[index].Prefix {
			matches = strings.HasPrefix(string(entry.Key), physicalKeys[index])
		}
		if !matches || entry.ModRevision <= 0 {
			return nil, errs.New(errs.KindInternal, "etcd transaction failure read is inconsistent")
		}
		values[index] = &KeyValue{
			Key:         strings.TrimPrefix(string(entry.Key), root),
			Value:       append([]byte(nil), entry.Value...),
			Version:     entry.Version,
			ModRevision: entry.ModRevision,
		}
	}
	return values, nil
}

func (s *store) Watch(ctx context.Context, prefix string, startRevision int64) (*WatchStream, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}
	if startRevision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd watch revision must not be negative")
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
				deliverWatchError(ctx, watchErrors, wrap(ctx, err))
				return
			}
			for _, item := range response.Events {
				if item == nil || item.Kv == nil {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.KindInternal, "etcd watch returned an empty event"),
					)
					return
				}
				key, ok := s.logicalKey(string(item.Kv.Key))
				if !ok {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.KindInternal, "etcd watch returned a key outside the configured prefix"),
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
		return errs.New(errs.KindValidationFailed, "snapshot writer is required")
	}

	reader, err := s.client.Snapshot(ctx)
	if err != nil {
		return wrap(ctx, err)
	}
	_, copyErr := io.Copy(w, reader)
	closeErr := reader.Close()
	if copyErr != nil {
		return wrap(ctx, copyErr)
	}
	return wrap(ctx, closeErr)
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
