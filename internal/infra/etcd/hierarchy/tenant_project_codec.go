package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func EncodeTenant(record TenantRecord) ([]byte, error) { return recordcodec.Encode("tenant", record) }
func EncodeProject(record ProjectRecord) ([]byte, error) {
	return recordcodec.Encode("project", record)
}
func DecodeTenant(value []byte) (TenantRecord, error) {
	record, err := recordcodec.Decode[TenantRecord](value, "tenant")
	if err != nil {
		return TenantRecord{}, err
	}
	if err := ValidateTenant(record); err != nil {
		return TenantRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func DecodeProject(value []byte) (ProjectRecord, error) {
	record, err := recordcodec.Decode[ProjectRecord](value, "project")
	if err != nil {
		return ProjectRecord{}, err
	}
	if err := ValidateProject(record); err != nil {
		return ProjectRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
