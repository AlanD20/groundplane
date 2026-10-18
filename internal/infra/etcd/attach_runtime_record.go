package etcd

import (
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeAcknowledgedServiceRuntime(
	value []byte,
	environmentID, serviceID string,
) (serviceruntimerecord.Record, error) {
	record, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](value, "service-acknowledged-runtime")
	if err != nil {
		return serviceruntimerecord.Record{}, err
	}
	if record.EnvironmentID != environmentID || record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(record) != nil {
		return serviceruntimerecord.Record{}, errs.New(errs.KindInternal, "acknowledged Service runtime is corrupt")
	}
	return record, nil
}

func attachRuntimeSourceConditions(input attachrender.AttachTaskRenderInput) []etcdstore.Condition {
	conditions := make([]etcdstore.Condition, len(input.RuntimePreparation.Updates))
	for index, update := range input.RuntimePreparation.Updates {
		conditions[index] = etcdstore.Condition{
			Key:         serviceruntimerecord.Key(update.Runtime.ServiceID),
			ModRevision: update.PreviousRevision,
		}
	}
	return conditions
}
