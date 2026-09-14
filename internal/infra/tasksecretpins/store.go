package tasksecretpins

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

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

// SecretAuthority is fixed source evidence returned by the Secret adapter.
// The adapter proves that the requested record identifies these exact current
// metadata and encrypted-value revisions.
type SecretAuthority struct {
	MetadataRevision int64
	ValueRevision    int64
	ProjectID        string
	TenantID         string
}

type Store interface {
	GetMany(context.Context, []string, int64) (*GetManyResult, error)
	Range(context.Context, string, int64) (*RangeResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
	VerifySecret(context.Context, tasksecretpinrecord.Record) (SecretAuthority, error)
}
