package volumeremoval

import etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

const maximumKeyBytes = 2 * 1024

type transactionSizer interface {
	TransactionSize([]etcdstore.Condition, []etcdstore.Mutation) (int, error)
}

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
		}
	}
}
