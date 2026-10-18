package api

import "github.com/AlanD20/groundplane/internal/common/backingaccess"

type BackingCheckRequest struct {
	AttachID string `json:"attach_id" required:"true" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
	Revision int64  `json:"revision" required:"true" minimum:"1"`
	GrantID  string `json:"grant_id,omitempty" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type BackingCheckConfiguration struct {
	AttachID           string                       `json:"attach_id"`
	CredentialAttachID string                       `json:"credential_attach_id"`
	ServiceID          string                       `json:"service_id"`
	BackingServiceID   string                       `json:"backing_service_id"`
	GrantID            string                       `json:"grant_id,omitempty"`
	Family             backingaccess.Family         `json:"family"`
	Authentication     backingaccess.Authentication `json:"authentication"`
	Host               string                       `json:"host"`
	Port               uint16                       `json:"port"`
	Database           string                       `json:"database,omitempty"`
	Username           string                       `json:"username,omitempty"`
	AttachRevision     int64                        `json:"attach_revision"`
	CredentialRevision int64                        `json:"credential_revision"`
	FactsRevision      int64                        `json:"facts_revision"`
	GrantRevision      int64                        `json:"grant_revision,omitempty"`
	AgentID            string                       `json:"agent_id"`
	ContainerID        string                       `json:"container_id"`
	RuntimeRevision    int64                        `json:"runtime_revision"`
}

type BackingCheckStage struct {
	Name                backingaccess.StageName `json:"name"`
	Outcome             backingaccess.Outcome   `json:"outcome"`
	Failure             backingaccess.Failure   `json:"failure,omitempty"`
	ElapsedMilliseconds int64                   `json:"elapsed_ms"`
}

// No private request, password or raw driver response crosses the human API.
type BackingCheckResult struct {
	TaskID            string                    `json:"task_id"`
	EnvironmentID     string                    `json:"environment_id"`
	ExecutionLocation string                    `json:"execution_location"`
	Configuration     BackingCheckConfiguration `json:"configuration"`
	Stale             bool                      `json:"stale"`
	Phase             string                    `json:"phase" enum:"ready,checking,cleaning,finished,cleanup_required"`
	Outcome           string                    `json:"outcome,omitempty" enum:"passed,failed,interrupted"`
	Failure           string                    `json:"failure,omitempty"`
	Stages            []BackingCheckStage       `json:"stages"`
	CleanupRequired   bool                      `json:"cleanup_required"`
	HelperName        string                    `json:"helper_name,omitempty"`
	CreatedAt         string                    `json:"created_at"`
	StartedAt         string                    `json:"started_at,omitempty"`
	FinishedAt        string                    `json:"finished_at,omitempty"`
}
