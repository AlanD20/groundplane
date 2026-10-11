package services

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConfirmLifecycleRuntime is published with the exact successful Task outcome,
// never merely when its active lock is removed. Failed and aborted Tasks leave
// the last requested runtime unconfirmed, including across Controller restart.
func ConfirmLifecycleRuntime(ctx context.Context, store RuntimeReader, serviceID, taskID string,
	intent core.ServiceRuntimeIntent, revision int64,
) (keyvalue.Condition, keyvalue.Mutation, error) {
	if ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		intent.Validate() != nil || revision <= 0 {
		return keyvalue.Condition{}, keyvalue.Mutation{}, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle confirmation is invalid",
		)
	}
	key := ServiceRuntimeKey(serviceID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle runtime is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := DecodeServiceRuntimeRecord(read.Values[0].Value)
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	if record.ServiceID != serviceID || record.Runtime.RuntimeIntent != intent ||
		record.Runtime.UnconfirmedLifecycleTaskID != taskID {
		return keyvalue.Condition{}, keyvalue.Mutation{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle confirmation belongs to another request",
		)
	}
	record.Runtime.UnconfirmedLifecycleTaskID = ""
	value, err := EncodeServiceRuntimeRecord(record)
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	return keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value}, nil
}
