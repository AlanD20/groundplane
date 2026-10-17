package volumeexecution

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutionBudget is shared by initial admission and explicit retries. Retrying
// renews this bound; it never inherits an ever-growing Task lifetime.
func ExecutionBudget(action agentpb.VolumeWorkAction) (time.Duration, error) {
	switch action {
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY:
		return 2 * time.Minute, nil
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE:
		return 6 * time.Hour, nil
	default:
		return 0, invalidProgress()
	}
}
