package etcd

import "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

// desiredMutationFragment holds private desired-state writes until the owning
// resource mutation joins them in its atomic publication.
type desiredMutationFragment struct {
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	values     [][]byte
}

func clearDesiredMutationFragment(publication desiredMutationFragment) {
	for _, value := range publication.values {
		clear(value)
	}
}
