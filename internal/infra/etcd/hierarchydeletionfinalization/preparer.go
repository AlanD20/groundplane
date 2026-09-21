package hierarchydeletionfinalization

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type finalizationStore interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Preparer struct{ store finalizationStore }

func NewPreparer(store finalizationStore) *Preparer { return &Preparer{store: store} }

func (effects Effects) FixedInputDigest() string { return effects.fixedInputDigest }

// Conditions, Mutations and Values borrow the prepared transaction buffers.
func (effects Effects) Conditions() []keyvalue.Condition { return effects.conditions }
func (effects Effects) Mutations() []keyvalue.Mutation   { return effects.mutations }
func (effects Effects) Values() [][]byte                 { return effects.values }
