package api

import "time"

type RunnerScopedMigrationRequest struct {
	ConnectionID string `json:"connection_id" pattern:"^ghc_[0-9A-HJKMNP-TV-Z]{26}$"`
	RepositoryID int64  `json:"repository_id" minimum:"1"`
}

type RunnerScopedMigrationStatus struct {
	RunnerID    string     `json:"runner_id"`
	TaskID      string     `json:"task_id,omitempty"`
	Revision    int64      `json:"revision"`
	Available   bool       `json:"available"`
	Phase       string     `json:"phase" enum:"legacy,registration,pending,cutover,complete"`
	AcceptedAt  *time.Time `json:"accepted_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
