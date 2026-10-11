package backingchecks

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type referenceStore interface {
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

func RequireAttachIdle(
	ctx context.Context,
	backend referenceStore,
	attachID string,
	revision int64,
) (keyvalue.Condition, error) {
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return keyvalue.Condition{}, invalidRecord()
	}
	return requireAbsent(ctx, backend, AttachReferencePrefix(attachID), revision)
}

func RequireServiceIdle(
	ctx context.Context,
	backend referenceStore,
	serviceID string,
	revision int64,
) (keyvalue.Condition, error) {
	if ids.Validate(ids.KindService, serviceID) != nil {
		return keyvalue.Condition{}, invalidRecord()
	}
	return requireAbsent(ctx, backend, ServiceReferencePrefix(serviceID), revision)
}

func RequireEnvironmentIdle(
	ctx context.Context,
	backend referenceStore,
	environmentID string,
	revision int64,
) (keyvalue.Condition, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return keyvalue.Condition{}, invalidRecord()
	}
	return requireAbsent(ctx, backend, EnvironmentReferencePrefix(environmentID), revision)
}

func requireAbsent(
	ctx context.Context,
	backend referenceStore,
	prefix string,
	revision int64,
) (keyvalue.Condition, error) {
	if backend == nil || revision <= 0 {
		return keyvalue.Condition{}, errs.New(errs.KindInternal, "Backing check reference snapshot is required")
	}
	read, err := backend.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) > 1 || read.More && len(read.Values) == 0 {
		if read != nil {
			keyvalue.ClearRangeValues(read.Values)
		}
		return keyvalue.Condition{}, errs.New(errs.KindInternal, "Backing check reference read is incomplete")
	}
	defer keyvalue.ClearRangeValues(read.Values)
	if len(read.Values) != 0 {
		value := read.Values[0]
		taskID := string(value.Value)
		if ids.Validate(ids.KindTask, taskID) != nil || value.Key != prefix+taskID {
			return keyvalue.Condition{}, errs.New(errs.KindInternal, "Backing check cleanup reference is corrupt")
		}
		return keyvalue.Condition{}, errs.Newf(
			errs.KindResourceInUse,
			"access check Task %s still owns this resource; finish its helper cleanup before removal",
			taskID,
		)
	}
	return keyvalue.Condition{Key: prefix, Prefix: true}, nil
}
