package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
)

const volumePrefix = "/v1/records/volumes/"

// VolumeRecord is the desired identity of one Environment-owned managed
// Volume. Slug is the renamable operator label. Key is the immutable authored
// Compose key and direct child below the Environment volume directory.
type VolumeRecord struct {
	ID            string `json:"id"`
	EnvironmentID string `json:"environment_id"`
	Slug          string `json:"slug"`
	Key           string `json:"key"`
}

func NewVolumeRecord(environmentID string, id string, volumeSlug string, key string) (VolumeRecord, error) {
	record := VolumeRecord{ID: id, EnvironmentID: environmentID, Slug: volumeSlug, Key: key}
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

func volumeSlugKey(environmentID string, volumeSlug string) string {
	return "/v1/indexes/volumes/by-slug/environment/" + environmentID + "/" + encodeDynamicSegment(volumeSlug)
}

func volumeComposeKey(environmentID string, key string) string {
	return "/v1/indexes/volumes/by-compose-key/environment/" + environmentID + "/" + encodeDynamicSegment(key)
}

func validateVolumeRecord(record VolumeRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindVolume, record.ID); err != nil {
		return err
	}
	if err := slug.Validate("volume slug", record.Slug); err != nil {
		return err
	}
	return validateVolumeComposeKey(record.Key)
}

func validateVolumeComposeKey(key string) error {
	return volumeidentity.ValidateKey(key)
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
