package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AuthorizeServiceRemovalConfiguration rechecks the complete read authority at
// one fresh linearizable revision before private bytes leave the Controller.
// It does not create a new effect grant or rewrite a checkpoint to acknowledge IO.
func (repository *ServiceRepository) AuthorizeServiceRemovalConfiguration(ctx context.Context, conditions []keyvalue.Condition) error {
	if len(conditions) == 0 || len(conditions) > 64 {
		return errs.New(errs.KindValidationFailed, "Removal file delivery authority is incomplete")
	}
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		if condition.Key == "" || condition.Prefix || condition.ModRevision < 0 {
			return errs.New(errs.KindValidationFailed, "Removal file delivery comparison is invalid")
		}
		keys[index] = condition.Key
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil {
		return errs.New(errs.KindStorageUnavailable, "Removal file delivery authorization is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindStorageUnavailable, "Removal file delivery authorization is incomplete")
	}
	for index, value := range read.Values {
		var revision int64
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				return errs.New(errs.KindStorageUnavailable, "Removal file authorization returned invalid evidence")
			}
			revision = value.ModRevision
		}
		if revision != conditions[index].ModRevision {
			return errs.New(errs.KindStateConflict, "Removal recovery ownership changed before file delivery")
		}
	}
	return nil
}
