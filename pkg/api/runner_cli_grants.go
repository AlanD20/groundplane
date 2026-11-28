package api

import "time"

type RunnerCLIGrantScope struct {
	TenantID      string `json:"tenant_id" pattern:"^tnt_[0-9A-HJKMNP-TV-Z]{26}$"`
	ProjectID     string `json:"project_id" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	EnvironmentID string `json:"environment_id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type RunnerCLIGrantAction struct {
	Operation string   `json:"operation" enum:"service.read,task.read,service.image.fetch,service.deploy"`
	TargetID  string   `json:"target_id" minLength:"30" maxLength:"31"`
	Image     string   `json:"image,omitempty" maxLength:"512"`
	TaskIDs   []string `json:"task_ids,omitempty" maxItems:"16" uniqueItems:"true"`
}

type RunnerCLIGrantWrite struct {
	Scope           RunnerCLIGrantScope    `json:"scope"`
	Actions         []RunnerCLIGrantAction `json:"actions" minItems:"1" maxItems:"16"`
	LifetimeSeconds int                    `json:"lifetime_seconds" minimum:"1" maximum:"3600"`
}

// RunnerCLIGrantStatus describes operator-approved actions without exposing
// credential material or granting any permission by itself.
type RunnerCLIGrantStatus struct {
	RunnerID  string                 `json:"runner_id"`
	Revision  int64                  `json:"revision"`
	Active    bool                   `json:"active"`
	Available bool                   `json:"available"`
	Scope     *RunnerCLIGrantScope   `json:"scope,omitempty"`
	Actions   []RunnerCLIGrantAction `json:"actions" nullable:"false"`
	ExpiresAt *time.Time             `json:"expires_at,omitempty"`
}
