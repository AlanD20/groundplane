package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeTenant(record TenantRecord) ([]byte, error) { return recordcodec.Encode("tenant", record) }
func encodeProject(record ProjectRecord) ([]byte, error) {
	return recordcodec.Encode("project", record)
}
func decodeTenant(value []byte) (TenantRecord, error) {
	record, err := recordcodec.Decode[TenantRecord](value, "tenant")
	if err != nil {
		return TenantRecord{}, err
	}
	if err := validateTenant(record); err != nil {
		return TenantRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeProject(value []byte) (ProjectRecord, error) {
	record, err := recordcodec.Decode[ProjectRecord](value, "project")
	if err != nil {
		return ProjectRecord{}, err
	}
	if err := validateProject(record); err != nil {
		return ProjectRecord{}, corruptRecord()
	}
	return record, nil
}

func corruptRecord() error {
	return errs.New(errs.KindInternal, "durable record violates its repository schema")
}
