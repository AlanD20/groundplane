package api

import "time"

type GitHubConnectionScope struct {
	Kind string `json:"kind" enum:"tenant,project,environment"`
	ID   string `json:"id" pattern:"^(tnt|prj|env)_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type GitHubConnection struct {
	ID             string                `json:"id"`
	Scope          GitHubConnectionScope `json:"scope"`
	Name           string                `json:"name"`
	AppID          int64                 `json:"app_id"`
	InstallationID int64                 `json:"installation_id"`
	Generation     int64                 `json:"generation"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	RevokedAt      *time.Time            `json:"revoked_at,omitempty"`
}

type GitHubConnectionCredentialsWrite struct {
	PrivateKeyPEM string `json:"private_key_pem" minLength:"1" maxLength:"16384" format:"password" writeOnly:"true"`
	WebhookSecret string `json:"webhook_secret,omitempty" minLength:"32" maxLength:"256" format:"password" writeOnly:"true"`
}

type GitHubConnectionCreateRequest struct {
	Scope          GitHubConnectionScope `json:"scope"`
	Name           string                `json:"name" minLength:"1" maxLength:"100"`
	AppID          int64                 `json:"app_id" minimum:"1"`
	InstallationID int64                 `json:"installation_id" minimum:"1"`
	GitHubConnectionCredentialsWrite
}

type GitHubRepository struct {
	ID            int64  `json:"id"`
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
}

type GitRunnerSelector struct {
	Label     string `json:"label"`
	Group     string `json:"group,omitempty"`
	GroupID   int64  `json:"group_id,omitempty"`
	Matching  int    `json:"matching"`
	Available int    `json:"available"`
}

type GitDeploymentBindingWrite struct {
	ConnectionID         string `json:"connection_id" pattern:"^ghc_[0-9A-HJKMNP-TV-Z]{26}$"`
	RepositoryID         int64  `json:"repository_id" minimum:"1"`
	WorkflowID           int64  `json:"workflow_id" minimum:"1"`
	WorkflowRef          string `json:"workflow_ref" minLength:"1" maxLength:"200"`
	WorkflowCommit       string `json:"workflow_commit,omitempty" pattern:"^[a-f0-9]{40}$"`
	Branch               string `json:"branch" minLength:"1" maxLength:"200"`
	Dockerfile           string `json:"dockerfile" minLength:"1" maxLength:"250"`
	BuildContext         string `json:"build_context" minLength:"1" maxLength:"250"`
	InheritRunnerDefault bool   `json:"inherit_runner_default,omitempty"`
	RunnerLabel          string `json:"runner_label,omitempty" minLength:"1" maxLength:"100"`
	RunnerGroup          string `json:"runner_group,omitempty" minLength:"1" maxLength:"100"`
	RunnerGroupID        int64  `json:"runner_group_id,omitempty" minimum:"1"`
	Automatic            bool   `json:"automatic"`
	RequireApproval      bool   `json:"require_approval"`
}

type GitDeploymentBinding struct {
	ServiceID     string `json:"service_id"`
	EnvironmentID string `json:"environment_id"`
	ProjectID     string `json:"project_id"`
	TenantID      string `json:"tenant_id"`
	GitDeploymentBindingWrite
}

type GitDeploymentBindingState struct {
	Binding  *GitDeploymentBinding `json:"binding,omitempty"`
	Revision int64                 `json:"revision" minimum:"0"`
}

type GitDeploymentStartRequest struct {
	ExpectedCommit string `json:"expected_commit,omitempty" pattern:"^[a-f0-9]{40}$"`
}

type GitDeployment struct {
	ID               string               `json:"id"`
	Binding          GitDeploymentBinding `json:"binding"`
	BindingRevision  int64                `json:"binding_revision"`
	Commit           string               `json:"commit"`
	Phase            string               `json:"phase" enum:"queued,dispatching,building,awaiting_approval,fetch_pending,fetching,deploy_pending,deploying,succeeded,failed,superseded,attention_required"`
	GitHubRunID      int64                `json:"github_run_id,omitempty"`
	GitHubRunAttempt int64                `json:"github_run_attempt,omitempty"`
	GitHubURL        string               `json:"github_url,omitempty"`
	GitHubRunnerID   int64                `json:"github_runner_id,omitempty"`
	ImageDigest      string               `json:"image_digest,omitempty"`
	FetchTaskID      string               `json:"fetch_task_id,omitempty"`
	DeployTaskID     string               `json:"deploy_task_id,omitempty"`
	ReleaseID        string               `json:"release_id,omitempty"`
	ErrorCode        string               `json:"error_code,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
}
