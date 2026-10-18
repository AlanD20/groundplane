package services

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareLifecycleRuntime binds a native execution boundary to the exact
// unconfirmed lifecycle request. Only proven completion clears confirmation.
func PrepareLifecycleRuntime(ctx context.Context, store RuntimeReader, serviceID, environmentID, taskID string, action workexecution.Action, revision int64, confirm bool) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	var intent core.ServiceRuntimeIntent
	switch action {
	case workexecution.ActionStart:
		intent = core.ServiceRuntimeIntentRunning
	case workexecution.ActionStop:
		intent = core.ServiceRuntimeIntentStopped
	case workexecution.ActionDestroy:
		intent = core.ServiceRuntimeIntentAbsent
	default:
		return nil, nil, errs.New(errs.KindValidationFailed, "Service lifecycle action is invalid")
	}
	key := ServiceRuntimeKey(serviceID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if revision <= 0 || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return nil, nil, errs.New(errs.KindStateConflict, "Service lifecycle runtime is unavailable")
	}
	record, err := DecodeServiceRuntimeRecord(read.Values[0].Value)
	if err != nil {
		return nil, nil, err
	}
	if record.ServiceID != serviceID || record.EnvironmentID != environmentID || record.Runtime.RuntimeIntent != intent || record.Runtime.UnconfirmedLifecycleTaskID != taskID {
		return nil, nil, errs.New(errs.KindStateConflict, "Service lifecycle request was replaced; it cannot authorize another runtime change")
	}
	conditions := []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	if !confirm {
		return conditions, nil, nil
	}
	record.Runtime.UnconfirmedLifecycleTaskID = ""
	value, err := EncodeServiceRuntimeRecord(record)
	if err != nil {
		return nil, nil, err
	}
	return conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}, nil
}
