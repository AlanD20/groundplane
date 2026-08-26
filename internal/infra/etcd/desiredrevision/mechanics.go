package desiredrevision

import "github.com/AlanD20/groundplane/internal/infra/etcd"

func clearKeyValues(values []*etcd.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
		}
	}
}

func clearKeyValueSlice(values []etcd.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}

func clearMutationValues(values []etcd.Mutation) {
	for index := range values {
		clear(values[index].Value)
	}
}
