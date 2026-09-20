package desiredrevision

import etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
		}
	}
}

func clearKeyValueSlice(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}

func clearMutationValues(values []etcdstore.Mutation) {
	for index := range values {
		clear(values[index].Value)
	}
}
