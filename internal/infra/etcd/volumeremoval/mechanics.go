package volumeremoval

import "github.com/AlanD20/groundplane/internal/infra/etcd"

const maximumKeyBytes = 2 * 1024

type transactionSizer interface {
	TransactionSize([]etcd.Condition, []etcd.Mutation) (int, error)
}

func clearKeyValues(values []*etcd.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
		}
	}
}
