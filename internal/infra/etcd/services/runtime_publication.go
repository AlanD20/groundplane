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

// AcknowledgeRunningRuntime joins confirmed running intent to the successful
// Deploy acknowledgement. The compare protects it from a concurrent lifecycle
// request; unrelated metadata publication never clears lifecycle uncertainty.
func AcknowledgeRunningRuntime(ctx context.Context, storage RuntimeReader, environmentID, serviceID string,
	revision int64,
) (keyvalue.Condition, keyvalue.Mutation, error) {
	key := ServiceRuntimeKey(serviceID)
	read, err := storage.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Condition{}, keyvalue.Mutation{}, errs.New(
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
			return keyvalue.Condition{}, keyvalue.Mutation{}, errs.New(
				errs.KindStateConflict,
				"Service runtime intent changed",
			)
		}
	}
	record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	record.Runtime.UnconfirmedLifecycleTaskID = ""
	value, err := EncodeServiceRuntimeRecord(record)
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	return keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value}, nil
}
