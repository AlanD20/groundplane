package api

import "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"

type EdgeCheckRequest struct {
	RouteID     string `json:"route_id" required:"true" pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
	Revision    int64  `json:"revision" required:"true" minimum:"1"`
	ConnectorID string `json:"connector_id" required:"true" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Path        string `json:"path,omitempty" maxLength:"2048" doc:"Concrete request path matching this Route; required for wildcard paths."`
}

type TunnelCheckRequest struct {
	Revision        int64  `json:"revision" required:"true" minimum:"1" doc:"Current Tunnel Component revision."`
	OriginServiceID string `json:"origin_service_id" required:"true" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	OriginPort      uint16 `json:"origin_port" required:"true" minimum:"1" maximum:"65535" doc:"An HTTP port exposed as TCP by the serving Service."`
	Hostname        string `json:"hostname" required:"true" minLength:"1" maxLength:"253" doc:"Concrete public hostname configured by the operator at the Tunnel provider; GP does not read or change provider ingress."`
	Path            string `json:"path" required:"true" minLength:"1" maxLength:"2048" doc:"Concrete HTTP request path without query or fragment."`
}

type EdgeCheckConfiguration struct {
	RouteID                  string                           `json:"route_id"`
	BlueprintRevision        int64                            `json:"blueprint_revision"`
	OriginServiceID          string                           `json:"origin_service_id,omitempty"`
	OriginRuntimeRevision    int64                            `json:"origin_runtime_revision,omitempty"`
	OriginRuntimeSHA256      string                           `json:"origin_runtime_sha256,omitempty"`
	RouteRevision            int64                            `json:"route_revision"`
	RouteObservationRevision int64                            `json:"route_observation_revision"`
	ConnectorID              string                           `json:"connector_id"`
	ConnectorServiceID       string                           `json:"connector_service_id"`
	ConnectorRevision        int64                            `json:"connector_revision"`
	ConnectorAppliedRevision int64                            `json:"connector_applied_revision"`
	ConnectorContainerID     string                           `json:"connector_container_id"`
	RouterID                 string                           `json:"router_id,omitempty"`
	RouterRevision           int64                            `json:"router_revision,omitempty"`
	RouterAppliedRevision    int64                            `json:"router_applied_revision,omitempty"`
	OriginAddress            string                           `json:"origin_address"`
	Target                   edgeconnectioncheck.PublicTarget `json:"target"`
	RuntimeSHA256            string                           `json:"runtime_sha256"`
	TokenSecretID            string                           `json:"token_secret_id"`
	TokenSecretRevision      int64                            `json:"token_secret_revision"`
	TokenValueRevision       int64                            `json:"token_value_revision" doc:"Token reference revision at capture; readiness does not verify remote provider ingress."`
	AgentID                  string                           `json:"agent_id"`
	AgentGeneration          uint64                           `json:"agent_generation"`
}

type EdgeCheckStage struct {
	Name                edgeconnectioncheck.StageName `json:"name"`
	Outcome             edgeconnectioncheck.Outcome   `json:"outcome"`
	Failure             edgeconnectioncheck.Failure   `json:"failure,omitempty"`
	HTTPStatus          int                           `json:"http_status,omitempty"`
	ElapsedMilliseconds int64                         `json:"elapsed_ms"`
	ExecutionLocation   string                        `json:"execution_location"`
}

// Public evidence excludes provider bodies, token bytes and captured Compose.
// Separate locations avoid presenting Controller egress as an external monitor.
type EdgeCheckResult struct {
	TaskID               string                 `json:"task_id"`
	EnvironmentID        string                 `json:"environment_id"`
	Configuration        EdgeCheckConfiguration `json:"configuration"`
	Stale                bool                   `json:"stale"`
	Phase                string                 `json:"phase" enum:"ready,private_checking,public_checking,cleaning,cleanup_required,finished,transferred"`
	Outcome              string                 `json:"outcome,omitempty" enum:"passed,protected,failed,interrupted"`
	Failure              string                 `json:"failure,omitempty"`
	PrivateFailure       string                 `json:"private_failure,omitempty"`
	PublicFailure        string                 `json:"public_failure,omitempty"`
	Stages               []EdgeCheckStage       `json:"stages"`
	Addresses            []string               `json:"addresses"`
	SelectedAddress      string                 `json:"selected_address,omitempty"`
	TLSVersion           uint16                 `json:"tls_version,omitempty"`
	CertificateExpiresAt string                 `json:"certificate_expires_at,omitempty"`
	CleanupRequired      bool                   `json:"cleanup_required"`
	HelperName           string                 `json:"helper_name,omitempty"`
	CleanupTaskID        string                 `json:"cleanup_task_id,omitempty"`
	CreatedAt            string                 `json:"created_at"`
	StartedAt            string                 `json:"started_at,omitempty"`
	FinishedAt           string                 `json:"finished_at,omitempty"`
}
