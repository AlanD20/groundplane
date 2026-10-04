package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RuntimeReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// RunningRuntimeMutation preserves operational fields under the owning
// Blueprint's Environment writer/epoch fence. Another runtime writer cannot
// publish while that ownership is retained.
func RunningRuntimeMutation(ctx context.Context, storage RuntimeReader, environmentID, serviceID string,
	revision int64,
) (keyvalue.Mutation, error) {
	key := ServiceRuntimeKey(serviceID)
	read, err := storage.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Mutation{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Mutation{}, errs.New(
			errs.KindStateConflict,
			"Service runtime intent is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	record := ServiceRuntimeRecord{EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID}}
	if read.Values[0] != nil {
		record, err = DecodeServiceRuntimeRecord(read.Values[0].Value)
		if err != nil || record.EnvironmentID != environmentID || record.ServiceID != serviceID {
			return keyvalue.Mutation{}, errs.New(
				errs.KindStateConflict,
				"Service runtime intent changed",
			)
		}
	}
	record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	value, err := EncodeServiceRuntimeRecord(record)
	if err != nil {
		return keyvalue.Mutation{}, err
	}
	return keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value}, nil
}
