// Package backingpostgresrelease owns the immutable release authority selected
// when a managed PostgreSQL Backing is first published. Controller upgrades do
// not rewrite the release of an existing database container.
package backingpostgresrelease

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const prefix = "/v1/records/backing-postgres-releases/"

type Record struct {
	EnvironmentID string                                 `json:"environment_id"`
	ServiceID     string                                 `json:"service_id"`
	Release       postgres16protocol.ManagedReleaseIndex `json:"release"`
}

func Key(environmentID, serviceID string) string {
	return prefix + environmentID + "/" + serviceID
}

func New(environmentID, serviceID string, release postgres16protocol.ManagedReleaseIndex) (Record, error) {
	record := Record{EnvironmentID: environmentID, ServiceID: serviceID, Release: release}
	if err := Validate(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func Validate(record Record) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, record.ServiceID) != nil ||
		record.Release.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "managed PostgreSQL Backing release authority is invalid")
	}
	return nil
}

func Encode(record Record) ([]byte, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backing_postgres_release", record)
}

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "backing_postgres_release")
	if err != nil || Validate(record) != nil {
		return Record{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
