package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumEnvironmentBlueprintTransactionOperationsPerArm = 256
)

// MeasureTransaction validates and measures a transaction without contacting
// etcd or enforcing the ordinary transaction ceilings.
func (s *store) MeasureTransaction(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionBudget, error) {
	if ctx == nil {
		return etcdstore.TransactionBudget{}, errs.New(
			errs.KindInternal,
			"etcd transaction measurement context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return etcdstore.TransactionBudget{}, err
	}
	prepared, err := s.prepareTransactionWithoutLimit(conditions, mutations)
	if err != nil {
		return etcdstore.TransactionBudget{}, err
	}
	return etcdstore.TransactionBudget{
		Operations: len(conditions) + len(mutations),
		Bytes:      prepared.requestBytes,
	}, nil
}

// MeasureTransactionBudget provides the same exact measurement for in-memory
// etcdstore.Store implementations that know their configured physical key prefix.
func MeasureTransactionBudget(
	ctx context.Context,
	keyPrefix string,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionBudget, error) {
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return etcdstore.TransactionBudget{}, errs.New(
			errs.KindValidationFailed,
			"etcd key prefix must begin and end with /",
		)
	}
	return (&store{root: strings.TrimSuffix(keyPrefix, "/")}).MeasureTransaction(ctx, conditions, mutations)
}
