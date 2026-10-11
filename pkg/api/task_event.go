package api

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"time"
)

// TaskEvent is one durable, Controller-sequenced step transition. Output
// chunks use resource log streams and never enter this public journal shape.
type TaskEvent struct {
	ControllerOperation *TaskControllerOperation `json:"controller_operation,omitempty"`
	Sequence            uint64                   `json:"sequence" minimum:"1"`
	StepID              string                   `json:"step_id"`
	State               TaskStatus               `json:"state" enum:"pending,running,completed,failed,aborted,timed_out"`
	Attempt             uint32                   `json:"attempt" minimum:"1"`
	Ordinal             uint64                   `json:"ordinal" minimum:"1"`
	ReceivedAt          time.Time                `json:"received_at"`
	HealthWait          *taskprogress.Readiness  `json:"health_wait,omitempty"`
	Failure             *TaskFailure             `json:"failure,omitempty"`
}

type TaskControllerOperation struct {
	Stage string `json:"stage"`
	Name  string `json:"name"`
}
