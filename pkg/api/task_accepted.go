package api

// TaskAccepted links an accepted action to its one operator-visible Task.
type TaskAccepted struct {
	TaskID string `json:"task_id"`
}
