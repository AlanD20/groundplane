package api

import "time"

type ControllerUpdateRequest struct {
	Release string `json:"release" minLength:"71" maxLength:"71" pattern:"^sha256:[0-9a-f]{64}$"`
}

type ControllerRelease struct {
	Release           string `json:"release"`
	ControllerSHA256  string `json:"controller_sha256"`
	ControllerVersion string `json:"controller_version"`
	AgentImage        string `json:"agent_image"`
	StorageEpoch      int    `json:"storage_epoch"`
	ChannelSchema     int    `json:"channel_schema"`
}

type ControllerUpdateSummary struct {
	TaskID    string    `json:"task_id"`
	Release   string    `json:"release"`
	Status    string    `json:"status"`
	Phase     string    `json:"phase"`
	CreatedAt time.Time `json:"created_at"`
}

type ControllerUpdateState struct {
	RunningSHA256 string                   `json:"running_sha256"`
	Available     bool                     `json:"available"`
	Error         string                   `json:"error"`
	Candidate     *ControllerRelease       `json:"candidate"`
	LastUpdate    *ControllerUpdateSummary `json:"last_update"`
}
