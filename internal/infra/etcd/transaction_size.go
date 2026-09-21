package etcd

import etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

// transactionSize returns the exact protobuf request size after logical keys
// have been expanded beneath this etcdstore.Store's configured root. Capability modules
// use it to enforce ceilings narrower than the etcdstore.Store-wide 1 MiB limit.
func (s *store) transactionSize(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) (int, error) {
	physicalConditions := make([]string, len(conditions))
	for index, condition := range conditions {
		key, err := s.physicalKey(condition.Key)
		if err != nil {
			return 0, err
		}
		physicalConditions[index] = key
	}
	physicalMutations := make([]string, len(mutations))
	for index, mutation := range mutations {
		key, err := s.physicalKey(mutation.Key)
		if err != nil {
			return 0, err
		}
		physicalMutations[index] = key
	}
	return transactionRequest(conditions, mutations, physicalConditions, physicalMutations).Size(), nil
}
