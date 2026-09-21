package runners

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func ClearRunnerAllocationEvidence(evidence RunnerAllocationEvidence) {
	for _, value := range []*etcdstore.KeyValue{evidence.Owner, evidence.Slug, evidence.Quota, evidence.Host, evidence.System} {
		if value != nil {
			clear(value.Value)
		}
	}
}
