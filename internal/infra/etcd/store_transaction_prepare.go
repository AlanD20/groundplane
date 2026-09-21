package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type preparedStoreTransaction struct {
	comparisons        []clientv3.Cmp
	failureReads       []clientv3.Op
	operations         []clientv3.Op
	physicalConditions []string
	requestBytes       int
}

// prepareTransaction validates and encodes the exact physical request without
// contacting etcd, then applies the etcdstore.Store-wide serialized-request ceiling.
func (s *store) prepareTransaction(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (preparedStoreTransaction, error) {
	prepared, err := s.prepareTransactionWithoutLimit(conditions, mutations)
	if err != nil {
		return preparedStoreTransaction{}, err
	}
	if prepared.requestBytes > etcdstore.MaximumBytes {
		return preparedStoreTransaction{}, errs.New(
			errs.KindValidationFailed,
			"etcd transaction exceeds the 1 MiB serialized request limit",
		)
	}
	return prepared, nil
}

// prepareTransactionWithoutLimit is shared by execution and non-executing
// measurement so both validate and encode one identical physical request.
func (s *store) prepareTransactionWithoutLimit(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (preparedStoreTransaction, error) {
	comparisons := make([]clientv3.Cmp, 0, len(conditions))
	failureReads := make([]clientv3.Op, 0, len(conditions))
	physicalConditions := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		if condition.ModRevision < 0 {
			return preparedStoreTransaction{}, errs.New(
				errs.KindValidationFailed,
				"etcd transaction revisions must not be negative",
			)
		}
		key, err := s.physicalKey(condition.Key)
		if err != nil {
			return preparedStoreTransaction{}, err
		}
		comparison := clientv3.Compare(clientv3.ModRevision(key), "=", condition.ModRevision)
		failureRead := clientv3.OpGet(key)
		if condition.Prefix {
			if condition.ModRevision != 0 {
				return preparedStoreTransaction{}, errs.New(
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
			return preparedStoreTransaction{}, err
		}
		switch mutation.Type {
		case etcdstore.MutationPut:
			if mutation.Prefix {
				return preparedStoreTransaction{}, errs.New(
					errs.KindValidationFailed,
					"etcd put mutation must not use prefix semantics",
				)
			}
			operations = append(operations, clientv3.OpPut(key, string(mutation.Value)))
		case etcdstore.MutationDelete:
			if mutation.Prefix {
				operations = append(operations, clientv3.OpDelete(key, clientv3.WithPrefix()))
			} else {
				operations = append(operations, clientv3.OpDelete(key))
			}
		default:
			return preparedStoreTransaction{}, errs.New(errs.KindValidationFailed, "invalid etcd transaction mutation")
		}
		physicalMutations = append(physicalMutations, key)
	}
	return preparedStoreTransaction{
		comparisons:        comparisons,
		failureReads:       failureReads,
		operations:         operations,
		physicalConditions: physicalConditions,
		requestBytes: transactionRequest(
			conditions,
			mutations,
			physicalConditions,
			physicalMutations,
		).Size(),
	}, nil
}
