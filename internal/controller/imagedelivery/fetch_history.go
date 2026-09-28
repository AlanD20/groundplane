package imagedelivery

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type retainedFetch struct {
	plan        imagefetch.Plan
	taskID      string
	requestedAt time.Time
	status      taskjournal.TaskStatus
}

// Match immutable content, never a mutable tag. Multiple manifests can share a
// configuration digest, so require the exact local repository digest.
func (retention imageRetention) history(image imagefetch.LocalImage) []apiTypes.ImageFetchRecord {
	result := make([]apiTypes.ImageFetchRecord, 0)
	digests := make(map[string]bool, len(image.Digests))
	for _, digest := range image.Digests {
		digests[canonicalImageReference(digest)] = true
	}
	for _, fetch := range retention.fetches {
		if !digests[fetch.plan.Reference()] {
			continue
		}
		result = append(result, apiTypes.ImageFetchRecord{
			Requested: fetch.plan.Requested, Image: fetch.plan.Reference(), TaskID: fetch.taskID,
			RequestedAt: fetch.requestedAt.Format(time.RFC3339Nano), Status: apiTypes.TaskStatus(fetch.status),
		})
	}
	slices.SortFunc(result, func(left, right apiTypes.ImageFetchRecord) int {
		// Task IDs are time-ordered and provide a stable tie breaker.
		if left.TaskID < right.TaskID {
			return 1
		}
		if left.TaskID > right.TaskID {
			return -1
		}
		return 0
	})
	return result
}
