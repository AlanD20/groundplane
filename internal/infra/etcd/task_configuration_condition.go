package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskRuntimeConfiguration(task TaskRecord) (*runtimeconfiguration.Reference, error) {
	if task.Configuration == nil {
		return nil, nil
	}
	if err := taskconfiguration.ValidateTaskSecretPinSet(task.Configuration.SecretPins); err != nil {
		return nil, err
	}
	if err := validateTaskBackingHookInputSet(task); err != nil {
		return nil, err
	}
	reference := task.Configuration.Current
	if reference == (runtimeconfiguration.Reference{}) {
		if task.Configuration.Prior != nil || task.Configuration.PriorRevision != 0 ||
			task.Configuration.BackingHookInputs == nil {
			return nil, errs.New(errs.KindStateConflict, "Task configuration authority is inconsistent")
		}
		return nil, nil
	}
	if runtimeconfiguration.ValidateReference(reference) != nil || task.RenderGeneration <= 0 ||
		reference.EnvironmentID != task.Owner.EnvironmentID ||
		reference.Generation > uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindStateConflict, "Task configuration authority is inconsistent")
	}
	return &reference, nil
}

func taskConfigurationCondition(task TaskRecord) (etcdstore.Condition, bool, error) {
	reference, err := taskRuntimeConfiguration(task)
	if err != nil || reference == nil {
		return etcdstore.Condition{}, false, err
	}
	revision, prior := task.Configuration.PriorRevision, task.Configuration.Prior
	if revision < 0 || (prior == nil) != (revision == 0) {
		return etcdstore.Condition{}, false, errs.New(
			errs.KindStateConflict,
			"Task configuration predecessor is invalid",
		)
	}
	if revision > 0 {
		if runtimeconfiguration.ValidateReference(*prior) != nil || prior.EnvironmentID != reference.EnvironmentID ||
			prior.Generation > reference.Generation {
			return etcdstore.Condition{}, false, errs.New(
				errs.KindStateConflict,
				"Task prior configuration is invalid",
			)
		}
	}
	return etcdstore.Condition{
		Key:         runtimeconfiguration.HeadKey(reference.EnvironmentID),
		ModRevision: revision,
	}, true, nil
}
