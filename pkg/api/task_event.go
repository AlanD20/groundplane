package api

import "time"

// TaskEvent is one durable, Controller-sequenced step transition. Output
// chunks use resource log streams and never enter this public journal shape.
type TaskEvent struct {
	Sequence   uint64     `json:"sequence" minimum:"1" maximum:"1000"`
	StepID     string     `json:"step_id"`
	State      TaskStatus `json:"state"`
	Attempt    uint32     `json:"attempt" minimum:"1"`
	Ordinal    uint64     `json:"ordinal" minimum:"1"`
	ReceivedAt time.Time  `json:"received_at"`
}
