package etcd

import (
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hostResolutionProjectionPublication struct {
	record     resolutionrecord.HostResolutionProjectionRecord
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func prepareHostResolutionProjectionPublication(
	current *etcdstore.KeyValue,
	inputRevision int64,
	routes []resolutionrecord.HostResolutionRouteRecord,
) (hostResolutionProjectionPublication, error) {
	condition := etcdstore.Condition{Key: resolutionrecord.StorageKey}
	if current != nil {
		stored, err := resolutionrecord.DecodeHostResolutionProjectionRecord(current.Value)
		if err != nil {
			return hostResolutionProjectionPublication{}, err
		}
		if inputRevision < stored.InputRevision {
			return hostResolutionProjectionPublication{}, errs.New(
				errs.KindStateConflict,
				"host-resolution input revision is stale",
			)
		}
		condition.ModRevision = current.ModRevision
	}
	record, err := resolutionrecord.NewHostResolutionProjectionRecord(inputRevision, routes)
	if err != nil {
		return hostResolutionProjectionPublication{}, err
	}
	value, err := resolutionrecord.EncodeHostResolutionProjectionRecord(record)
	if err != nil {
		return hostResolutionProjectionPublication{}, err
	}
	return hostResolutionProjectionPublication{
		record: record, conditions: []etcdstore.Condition{condition},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: resolutionrecord.StorageKey, Value: value}},
		values:    [][]byte{value},
	}, nil
}

func (publication hostResolutionProjectionPublication) clear() {
	for _, value := range publication.values {
		clear(value)
	}
}
