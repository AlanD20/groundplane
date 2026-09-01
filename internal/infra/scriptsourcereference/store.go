package scriptsourcereference

import "context"

type KeyValue struct {
	Key         string
	Value       []byte
	ModRevision int64
}

type GetManyResult struct {
	Values       []*KeyValue
	ReadRevision int64
}

type RangeResult struct {
	Values       []KeyValue
	ReadRevision int64
	More         bool
}

type Condition struct {
	Key         string
	ModRevision int64
	Prefix      bool
}

type MutationType uint8

const (
	MutationPut MutationType = iota + 1
	MutationDelete
)

type Mutation struct {
	Type   MutationType
	Key    string
	Value  []byte
	Prefix bool
}

type TransactionResult struct {
	Succeeded bool
	Revision  int64
}

// Store is the consumer-owned persistence side-effect seam. The etcd adapter
// supplies MVCC reads, ranges, and atomic compare-and-mutate transactions.
type Store interface {
	GetMany(context.Context, []string, int64) (*GetManyResult, error)
	Range(context.Context, string, int64) (*RangeResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

// ScriptPrimaryCodec preserves the existing Script storage codec while this
// capability owns reference-count adjustment semantics.
type ScriptPrimaryCodec interface {
	AdjustScriptPrimary([]byte, SourceIdentity, int64) ([]byte, error)
}
