package etcd

import "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

type removalTaskChange struct {
	applies    bool
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	values     [][]byte
}

func clearRemovalTaskChange(change removalTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
