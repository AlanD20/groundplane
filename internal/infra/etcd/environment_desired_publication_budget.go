package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentDesiredPublicationBudget(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) error {
	return validateEnvironmentDesiredPublicationPartitionCounts(len(conditions), len(mutations), len(conditions))
}

func validateEnvironmentDesiredPublicationPartitionCounts(comparisons, successMutations, failureReads int) error {
	if comparisons > 32 || successMutations > 32 || failureReads > 32 {
		return errs.Newf(errs.KindValidationFailed,
			"Environment desired publication exceeds a 32-operation transaction partition (%d/%d/%d)",
			comparisons, successMutations, failureReads)
	}
	return nil
}
