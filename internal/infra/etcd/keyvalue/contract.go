// Package keyvalue defines revision-aware reads and atomic writes shared by etcd repositories.
package keyvalue

import (
	"context"
	"io"
)

const (
	MaximumOperations = 96
	MaximumBytes      = 1 << 20
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
	MeasureTransaction(ctx context.Context, conditions []Condition, mutations []Mutation) (TransactionBudget, error)
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

// TransactionBudget is the exact physical request cost of an ordinary Store
// transaction after applying its configured storage prefix.
type TransactionBudget struct {
	Operations int
	Bytes      int
}

// Fits reports whether the measured request is within both ordinary Store
// transaction ceilings.
func (budget TransactionBudget) Fits() bool {
	return budget.Operations >= 0 && budget.Operations <= MaximumOperations &&
		budget.Bytes >= 0 && budget.Bytes <= MaximumBytes
}
