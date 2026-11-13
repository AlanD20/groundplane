package api

type RunnerVersion struct {
	Version      string `json:"version"`
	Architecture string `json:"architecture"`
}
type RunnerVersions struct {
	Items []RunnerVersion `json:"items" nullable:"false"`
}
type RunnerUpdateRequest struct {
	Revision int64  `json:"revision"`
	Version  string `json:"version"`
}
type RunnerUpdateStage struct {
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Failure string `json:"failure,omitempty"`
}
type RunnerUpdateResult struct {
	TaskID     string              `json:"task_id"`
	Version    string              `json:"version"`
	Phase      string              `json:"phase"`
	Outcome    string              `json:"outcome,omitempty"`
	Failure    string              `json:"failure,omitempty"`
	Stages     []RunnerUpdateStage `json:"stages" nullable:"false"`
	CreatedAt  string              `json:"created_at"`
	FinishedAt string              `json:"finished_at,omitempty"`
}
