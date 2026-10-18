package api

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type TaskAttemptEvent struct {
	Sequence   uint64              `json:"sequence"`
	RecordedAt time.Time           `json:"recorded_at"`
	Stage      workexecution.Stage `json:"stage"`
}

type TaskAttemptEvents struct {
	FinishedAt *time.Time            `json:"finished_at,omitempty" doc:"Recorded attempt completion, independent of diagnostic availability"`
	Outcome    workexecution.Outcome `json:"outcome,omitempty"`
	Items      []TaskAttemptEvent    `json:"items" nullable:"false"`
	First      uint64                `json:"first" doc:"First retained sequence, or zero when no events are retained"`
	Last       uint64                `json:"last" doc:"Last retained sequence, local to this attempt"`
	Expired    bool                  `json:"expired" doc:"The requested cursor precedes retained history; returned events do not cover the missing interval"`
	Incomplete bool                  `json:"incomplete" doc:"Some diagnostics could not be recorded; execution outcomes remain independent"`
	Revision   int64                 `json:"revision"`
}
