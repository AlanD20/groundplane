package runtimeconfiguration

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

type Condition struct {
	Key         string
	ModRevision int64
}

type MutationType uint8

const (
	MutationPut MutationType = iota + 1
	MutationDelete
)

type Mutation struct {
	Type  MutationType
	Key   string
	Value []byte
}

type TxnResult struct {
	Succeeded bool
	Revision  int64
}

// Store is the consumer-owned persistence seam for immutable configuration
// source snapshots. Zero condition revisions require an absent key.
type Store interface {
	GetMany(context.Context, []string, int64) (*GetManyResult, error)
	Transact(context.Context, []Condition, []Mutation) (TxnResult, error)
}
