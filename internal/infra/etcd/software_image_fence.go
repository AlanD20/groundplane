package etcd

import (
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A completed removal retains an idle record to invalidate earlier selections.
// Every software publisher compares its observed revision, including that idle state.
func softwareImageFence(value *keyvalue.KeyValue) (keyvalue.Condition, error) {
	condition := keyvalue.Condition{Key: imagefence.Key, ModRevision: keyvalue.RevisionOf(value)}
	if value == nil {
		return condition, nil
	}
	var fence struct {
		Operation string `json:"operation"`
	}
	if json.Unmarshal(value.Value, &fence) != nil {
		return keyvalue.Condition{}, errs.New(errs.KindInternal, "Host image cleanup fence is invalid")
	}
	if fence.Operation != "" {
		return keyvalue.Condition{}, errs.New(
			errs.KindResourceInUse,
			"Host image cleanup is in progress; wait before changing software",
		)
	}
	return condition, nil
}

func (repository *TaskRepository) readSoftwareImageFence(ctx context.Context) (keyvalue.Condition, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{imagefence.Key}})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision <= 0 {
		return keyvalue.Condition{}, errs.New(
			errs.KindStorageUnavailable,
			"Software image admission snapshot is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	return softwareImageFence(read.Values[0])
}
