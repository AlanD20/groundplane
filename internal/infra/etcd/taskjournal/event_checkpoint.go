package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"time"
)

// TaskEventCheckpoint retains bounded step progress and mutation facts, not a
// second history. Its identity is the replay watermark of evicted events.
type TaskEventCheckpoint struct {
	ControllerOperation  *taskprogress.ControllerOperation `json:"controller_operation,omitempty"`
	Failure              *taskprogress.Failure             `json:"failure,omitempty"`
	Identity             TaskEventIdentity                 `json:"identity"`
	Sequence             uint64                            `json:"sequence"`
	PayloadSHA256        string                            `json:"payload_sha256"`
	State                TaskEventState                    `json:"state"`
	Running              bool                              `json:"running"`
	Completed            bool                              `json:"completed"`
	EffectPossible       bool                              `json:"effect_possible"`
	HealthWait           *taskprogress.Readiness           `json:"health_wait,omitempty"`
	HealthWaitOrdinal    uint64                            `json:"health_wait_ordinal,omitempty"`
	HealthWaitReceivedAt *time.Time                        `json:"health_wait_received_at,omitempty"`
}

func CloneEventCheckpoints(values []TaskEventCheckpoint) []TaskEventCheckpoint {
	cloned := append([]TaskEventCheckpoint(nil), values...)
	for index := range cloned {
		cloned[index].ControllerOperation = taskprogress.CloneControllerOperation(values[index].ControllerOperation)
		cloned[index].Failure = taskprogress.CloneFailure(values[index].Failure)
		if value := cloned[index].HealthWait; value != nil {
			owned := *value
			cloned[index].HealthWait = &owned
		}
		if value := cloned[index].HealthWaitReceivedAt; value != nil {
			owned := *value
			cloned[index].HealthWaitReceivedAt = &owned
		}
	}
	return cloned
}
