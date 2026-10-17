package networkexecution

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutionBudget bounds one bridge operation. Network removal never drains
// consumers or deletes their data; an attached endpoint makes removal fail.
func ExecutionBudget(action agentpb.NetworkWorkAction) (time.Duration, error) {
	switch action {
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE, agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE:
		return 2 * time.Minute, nil
	default:
		return 0, invalidProgress()
	}
}
