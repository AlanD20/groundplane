package etcd

import (
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// Fixture encodings seed historical state directly so deletion and drift tests
// do not depend on running the same creation path whose races they exercise.
func encodeReleaseGroupFixture(group domain.Group) ([]byte, error) {
	return recordcodec.Encode("release_group", struct {
		ID            string           `json:"id"`
		EnvironmentID string           `json:"environment_id"`
		Name          string           `json:"name"`
		ServiceIDs    []string         `json:"service_ids"`
		Order         []string         `json:"order"`
		DefaultTag    string           `json:"default_tag,omitempty"`
		OnFailure     domain.OnFailure `json:"on_failure"`
	}{group.ID, group.EnvironmentID, group.Name, group.ServiceIDs, group.Order, group.DefaultTag, group.OnFailure})
}

func encodeReleaseGroupEpochFixture(environmentID string) ([]byte, error) {
	return recordcodec.Encode("release-group-collection-epoch", map[string]string{"environment_id": environmentID})
}
