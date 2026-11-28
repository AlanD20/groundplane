package api

type GitRepositoryRunnerDefaultWrite struct {
	Label   string `json:"label" minLength:"1" maxLength:"100"`
	Group   string `json:"group,omitempty" minLength:"1" maxLength:"100"`
	GroupID int64  `json:"group_id,omitempty" minimum:"1"`
}

type GitRepositoryRunnerDefault struct {
	Scope        GitHubConnectionScope `json:"scope"`
	ConnectionID string                `json:"connection_id"`
	RepositoryID int64                 `json:"repository_id"`
	GitRepositoryRunnerDefaultWrite
}

type GitRepositoryRunnerDefaultState struct {
	Default  *GitRepositoryRunnerDefault `json:"default,omitempty"`
	Revision int64                       `json:"revision" minimum:"0"`
}

type GitHubWorkflow struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Path  string `json:"path"`
	State string `json:"state"`
}
