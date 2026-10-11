package api

type RunnerCheckRequest struct {
	Revision int64 `json:"revision" required:"true" minimum:"1"`
}

type RunnerCheckConfiguration struct {
	RunnerID           string `json:"runner_id"`
	RunnerRevision     int64  `json:"runner_revision"`
	RuntimeEpoch       uint64 `json:"runtime_epoch"`
	ImageReference     string `json:"image_reference"`
	GitHubURL          string `json:"github_url"`
	ControllerEndpoint string `json:"controller_endpoint"`
}

type RunnerCheckStage struct {
	Name                string `json:"name" enum:"github,rootless_docker,gp_api,registry"`
	Outcome             string `json:"outcome" enum:"passed,failed"`
	Failure             string `json:"failure,omitempty" enum:"connection,timeout,canceled,tls,authentication,permission,http_response,protocol,not_rootless,credentials_unavailable"`
	HTTPStatus          int    `json:"http_status,omitempty"`
	ElapsedMilliseconds int64  `json:"elapsed_ms"`
}

type RunnerCheckResult struct {
	TaskID            string                   `json:"task_id"`
	ExecutionLocation string                   `json:"execution_location"`
	Configuration     RunnerCheckConfiguration `json:"configuration"`
	Stale             bool                     `json:"stale"`
	Phase             string                   `json:"phase" enum:"ready,checking,finished"`
	Outcome           string                   `json:"outcome,omitempty" enum:"passed,failed,interrupted"`
	Failure           string                   `json:"failure,omitempty"`
	Stages            []RunnerCheckStage       `json:"stages" nullable:"false"`
	CreatedAt         string                   `json:"created_at"`
	StartedAt         string                   `json:"started_at,omitempty"`
	FinishedAt        string                   `json:"finished_at,omitempty"`
	ObservedAt        string                   `json:"observed_at,omitempty"`
}
