package api

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type TaskImageExecution struct {
	TaskID              string                `json:"task_id"`
	WorkItemID          string                `json:"work_item_id"`
	AttemptID           string                `json:"attempt_id,omitempty"`
	Requested           string                `json:"requested,omitempty"`
	Manifest            string                `json:"manifest,omitempty"`
	ImageID             string                `json:"image_id"`
	Platform            string                `json:"platform,omitempty"`
	Phase               string                `json:"phase" enum:"ready,fetching,tagging,removing,uncertain,finished"`
	UncertainPhase      string                `json:"uncertain_phase,omitempty"`
	Outcome             workexecution.Outcome `json:"outcome,omitempty"`
	Summary             string                `json:"summary"`
	Progress            *imagefetch.Progress  `json:"progress,omitempty"`
	ProgressUnavailable bool                  `json:"progress_unavailable"`
	UpdatedAt           time.Time             `json:"updated_at"`
	FinishedAt          *time.Time            `json:"finished_at,omitempty"`
	Revision            int64                 `json:"revision"`
}
