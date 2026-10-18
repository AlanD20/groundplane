package api

import "time"

// Retry selects captured work at its recorded attempt count. Zero identifies
// failed work that never acquired an execution assignment.
type TaskRetryRequest struct {
	WorkItemID      string     `json:"work_item_id" pattern:"^work_[0-9A-HJKMNP-TV-Z]{26}$"`
	Attempt         uint64     `json:"attempt" minimum:"0"`
	QueuedFailureAt *time.Time `json:"queued_failure_at,omitempty" doc:"Required only when attempt is zero; copy updated_at from the failed work item"`
}
