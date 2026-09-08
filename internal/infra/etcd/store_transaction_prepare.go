package etcd

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type preparedStoreTransaction struct {
	comparisons        []clientv3.Cmp
	failureReads       []clientv3.Op
	operations         []clientv3.Op
	physicalConditions []string
}

// prepareTransaction validates the exact physical request without contacting etcd.
// Both pre-release budget checks and actual commits use this preparation.
func (s *store) prepareTransaction(conditions []Condition, mutations []Mutation) (preparedStoreTransaction, error) {

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
		case MutationPut:
			if mutation.Prefix {
				return preparedStoreTransaction{}, errs.New(
					errs.KindValidationFailed,
					"etcd put mutation must not use prefix semantics",
				)
			}
			operations = append(operations, clientv3.OpPut(key, string(mutation.Value)))
		case MutationDelete:
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
	request := transactionRequest(conditions, mutations, physicalConditions, physicalMutations)
	if request.Size() > maximumTransactionBytes {
		return preparedStoreTransaction{}, errs.New(
			errs.KindValidationFailed,
			"etcd transaction exceeds the 1 MiB serialized request limit",
		)
	}

	return preparedStoreTransaction{
		comparisons:        comparisons,
		failureReads:       failureReads,
		operations:         operations,
		physicalConditions: physicalConditions,
	}, nil
}
