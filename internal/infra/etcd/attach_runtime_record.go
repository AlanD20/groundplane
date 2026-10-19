package etcd

import (
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

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
