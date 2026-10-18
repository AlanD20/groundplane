package api

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"time"
)

// TaskEvent is a bounded, Task-ordered stage observation. Execution authority
// and workload output are separate from this diagnostic timeline.
type TaskEvent struct {
	Sequence        uint64              `json:"sequence" minimum:"1"`
	WorkItemID      string              `json:"work_item_id"`
	AttemptID       string              `json:"attempt_id"`
	AttemptSequence uint64              `json:"attempt_sequence" minimum:"1"`
	RecordedAt      time.Time           `json:"recorded_at"`
	Stage           workexecution.Stage `json:"stage"`
}

type TaskControllerOperation struct {
	Stage string `json:"stage"`
	Name  string `json:"name"`
}
