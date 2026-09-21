package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func allBackupRuntimeValuesAbsent(values []*etcdstore.KeyValue) bool {
	for _, value := range values {
		if value != nil {
			return false
		}
	}
	return true
}
