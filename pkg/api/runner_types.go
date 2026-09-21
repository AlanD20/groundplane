package api

type Runner struct {
	ID           string          `json:"id"`
	Slug         string          `json:"slug"`
	TenantID     string          `json:"tenant_id"`
	ProjectID    string          `json:"project_id,omitempty"`
	GitHubURL    string          `json:"github_url"`
	Name         string          `json:"name"`
	Labels       []string        `json:"labels,omitempty"`
	Lifecycle    RunnerLifecycle `json:"lifecycle" enum:"provisioning,ready,failed,deleting"`
	CreateTaskID string          `json:"create_task_id"`
	RemoveTaskID *string         `json:"remove_task_id"`
	Online       bool            `json:"online"`
	ObservedAt   *string         `json:"observed_at"`
	CreatedAt    string          `json:"created_at"`
}

type RunnerLifecycle string

const (
	RunnerLifecycleProvisioning RunnerLifecycle = "provisioning"
	RunnerLifecycleReady        RunnerLifecycle = "ready"
	RunnerLifecycleFailed       RunnerLifecycle = "failed"
	RunnerLifecycleDeleting     RunnerLifecycle = "deleting"
)

type RunnerPage struct {
	Items      []Runner `json:"items"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

type RunnerCreateRequest struct {
	Slug              string   `json:"slug"`
	TenantID          string   `json:"tenant_id,omitempty"`
	ProjectID         string   `json:"project_id,omitempty"`
	GitHubURL         string   `json:"github_url"`
	Labels            []string `json:"labels,omitempty"`
	RegistrationToken string   `json:"registration_token"` // discarded by the Controller after registration
}

type RunnerRetryRequest struct {
	RegistrationToken string `json:"registration_token"` // discarded by the Controller after registration
}

type RunnerEditRequest struct {
	Slug string `json:"slug"`
}

// --- Task / activity ---
