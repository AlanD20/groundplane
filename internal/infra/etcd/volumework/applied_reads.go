package volumework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
)

// ReadAppliedVolumes reads a bounded page at the desired inventory's revision.
// Missing applied state means unproven storage, never implicit success.
func (repository *InputRepository) ReadAppliedVolumes(
	ctx context.Context,
	volumeIDs []string,
	revision int64,
) (map[string]AppliedVolume, error) {
	if len(volumeIDs) > 200 || revision <= 0 {
		return nil, conflict("applied Volume page requires a bounded revisioned inventory")
	}
	result := make(map[string]AppliedVolume, len(volumeIDs))
	if len(volumeIDs) == 0 {
		return result, nil
	}
	keys := make([]string, len(volumeIDs))
	seen := make(map[string]bool, len(volumeIDs))
	for index, id := range volumeIDs {
		if ids.Validate(ids.KindVolume, id) != nil || seen[id] {
			return nil, conflict("applied Volume page contains invalid or duplicate identities")
		}
		seen[id], keys[index] = true, AppliedKey(id)
	}
	read, err := recordquery.GetManyBatchedAtRevision(ctx, repository.store, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil {
			continue
		}
		if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return nil, conflict("applied Volume page has inconsistent storage revisions")
		}
		state, err := decodeApplied(value.Value)
		if err != nil {
			return nil, err
		}
		if state.VolumeID != volumeIDs[index] {
			return nil, conflict("applied Volume identity differs from its inventory key")
		}
		result[state.VolumeID] = state
	}
	return result, nil
}
