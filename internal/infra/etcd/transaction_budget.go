package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTransactionOperations                           = 96
	maximumEnvironmentBlueprintTransactionOperationsPerArm = 256
	maximumTransactionBytes                                = 1 << 20
)

// TransactionBudget is the exact physical request cost of an ordinary Store
// transaction after applying its configured storage prefix.
type TransactionBudget struct {
	Operations int
	Bytes      int
}

// Fits reports whether the measured request is within both ordinary Store
// transaction ceilings.
func (budget TransactionBudget) Fits() bool {
	return budget.Operations >= 0 && budget.Operations <= maximumTransactionOperations &&
		budget.Bytes >= 0 && budget.Bytes <= maximumTransactionBytes
}

// MeasureTransaction validates and measures a transaction without contacting
// etcd or enforcing the ordinary transaction ceilings.
func (s *store) MeasureTransaction(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionBudget, error) {
	if ctx == nil {
		return TransactionBudget{}, errs.New(errs.KindInternal, "etcd transaction measurement context is required")
	}
	if err := ctx.Err(); err != nil {
		return TransactionBudget{}, err
	}
	prepared, err := s.prepareTransactionWithoutLimit(conditions, mutations)
	if err != nil {
		return TransactionBudget{}, err
	}
	return TransactionBudget{
		Operations: len(conditions) + len(mutations),
		Bytes:      prepared.requestBytes,
	}, nil
}

// MeasureTransactionBudget provides the same exact measurement for in-memory
// Store implementations that know their configured physical key prefix.
func MeasureTransactionBudget(
	ctx context.Context,
	keyPrefix string,
	conditions []Condition,
	mutations []Mutation,
) (TransactionBudget, error) {
	if !strings.HasPrefix(keyPrefix, "/") || !strings.HasSuffix(keyPrefix, "/") {
		return TransactionBudget{}, errs.New(
			errs.KindValidationFailed,
			"etcd key prefix must begin and end with /",
		)
	}
	return (&store{root: strings.TrimSuffix(keyPrefix, "/")}).MeasureTransaction(ctx, conditions, mutations)
}
