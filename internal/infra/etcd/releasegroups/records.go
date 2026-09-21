package releasegroups

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
)

const (
	ReleaseGroupRecordPrefix = "/v1/records/release-groups/"
	ReleaseGroupOwnerPrefix  = "/v1/indexes/release-groups/by-owner/environment/"
	releaseGroupNamePrefix   = "/v1/indexes/release-groups/by-name/environment/"
)

type releaseGroupStoredRecord struct {
	ID            string           `json:"id"`
	EnvironmentID string           `json:"environment_id"`
	Name          string           `json:"name"`
	ServiceIDs    []string         `json:"service_ids"`
	Order         []string         `json:"order"`
	DefaultTag    string           `json:"default_tag,omitempty"`
	OnFailure     domain.OnFailure `json:"on_failure"`
}

func ReleaseGroupRecordKey(id string) string { return ReleaseGroupRecordPrefix + id }
func ReleaseGroupOwnerKey(environmentID, id string) string {
	return ReleaseGroupOwnerPrefix + environmentID + "/" + id
}
func ReleaseGroupNameKey(environmentID, name string) string {
	return releaseGroupNamePrefix + environmentID + "/" + recordcodec.EncodeKeySegment(name)
}

func DecodeReleaseGroupStored(value []byte) (domain.Group, error) {
	record, err := recordcodec.Decode[releaseGroupStoredRecord](value, "release_group")
	if err != nil {
		return domain.Group{}, err
	}
	group, err := domain.New(domain.Input{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Name: record.Name,
		ServiceIDs: record.ServiceIDs, Order: record.Order, DefaultTag: record.DefaultTag, OnFailure: record.OnFailure,
	})
	if err != nil {
		return domain.Group{}, recordcodec.CorruptRecord()
	}
	return group, nil
}
