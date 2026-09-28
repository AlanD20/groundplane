package etcd

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TransactBackingService keeps publication atomic and reads failed compares at its MVCC revision.
func (s *store) TransactBackingService(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	var empty etcdstore.TransactionResult
	if len(mutations) == 0 {
		return empty, errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	if err := validateBackingServicePublicationOperationCounts(len(etcdstore.ImageSelectionConditions(ctx, conditions)), len(mutations), 0); err != nil {
		return empty, err
	}
	for _, condition := range conditions {
		if condition.Prefix {
			return empty, errs.New(errs.KindInternal, "Backing-service compare must use exact keys")
		}
	}
	return s.transactWithFailureReads(ctx, conditions, mutations, true)
}

type backingServiceTransactionStore interface {
	TransactBackingService(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionResult, error)
}
