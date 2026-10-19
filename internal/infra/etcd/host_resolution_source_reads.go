package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func appendHostResolutionCondition(
	conditions []etcdstore.Condition,
	candidate etcdstore.Condition,
) []etcdstore.Condition {
	for _, existing := range conditions {
		if existing.Key != candidate.Key {
			continue
		}
		return conditions
	}
	return append(conditions, candidate)
}
