package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const volumePrefix = "/v1/records/volumes/"

// VolumeRecord is the durable boundary for one Environment-owned managed
// Volume. Name is both its scoped label and exact Compose volume key.
type VolumeRecord struct {
	ID            string `json:"id"`
	EnvironmentID string `json:"environment_id"`
	Name          string `json:"name"`
}

func NewVolumeRecord(environmentID string, id string, name string) (VolumeRecord, error) {
	record := VolumeRecord{ID: id, EnvironmentID: environmentID, Name: name}
	if err := validateVolumeRecord(record); err != nil {
		return VolumeRecord{}, err
	}
	return record, nil
}

func volumeKey(id string) string { return volumePrefix + id }

func volumeOwnerPrefix(environmentID string) string {
	return "/v1/indexes/volumes/by-owner/environment/" + environmentID + "/"
}

func volumeOwnerKey(environmentID string, volumeID string) string {
	return volumeOwnerPrefix(environmentID) + volumeID
}

func volumeNameKey(environmentID string, name string) string {
	return "/v1/indexes/volumes/by-name/environment/" + environmentID + "/" + encodeDynamicSegment(name)
}

func validateVolumeRecord(record VolumeRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindVolume, record.ID); err != nil {
		return err
	}
	if err := validateVolumeName(record.Name); err != nil {
		return err
	}
	return nil
}

func validateVolumeName(name string) error {
	if err := composekey.Validate(name); err != nil {
		return err
	}
	if name == "." || name == ".." {
		return errs.New(
			errs.KindValidationFailed,
			"volume name must identify one direct Environment directory child",
		)
	}
	return nil
}

func encodeVolumeRecord(record VolumeRecord) ([]byte, error) {
	if err := validateVolumeRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("volume", record)
}

func decodeVolumeRecord(value []byte) (VolumeRecord, error) {
	record, err := decodeEnvelope[VolumeRecord](value, "volume")
	if err != nil {
		return VolumeRecord{}, err
	}
	if err := validateVolumeRecord(record); err != nil {
		return VolumeRecord{}, corruptRecord()
	}
	return record, nil
}
