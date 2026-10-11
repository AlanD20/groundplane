package api

import "github.com/AlanD20/groundplane/internal/common/s3connector"

// ConnectorCheckRequest checks either an unsaved draft or one saved Connector
// revision with optional unsaved edits. Neither mode saves those settings.
type ConnectorCheckRequest struct {
	ConnectorID string                  `json:"connector_id,omitempty"`
	Revision    int64                   `json:"revision,omitempty"`
	Draft       *ConnectorCreateRequest `json:"draft,omitempty"`
	Changes     *ConnectorEditRequest   `json:"changes,omitempty"`
}

type ConnectorCheckSecretSource struct {
	Credential string `json:"credential"`
	SecretID   string `json:"secret_id"`
	Revision   int64  `json:"revision"`
}

type ConnectorCheckStage struct {
	Name                s3connector.ConnectionCheckStageName `json:"name"`
	ElapsedMilliseconds int64                                `json:"elapsed_ms"`
	Succeeded           bool                                 `json:"succeeded"`
	Failure             s3connector.ConnectionCheckFailure   `json:"failure,omitempty"`
}

// ConnectorCheckResult contains safe execution evidence, never ciphertext,
// credential values, provider response bodies or cleanup ownership payloads.
type ConnectorCheckResult struct {
	TaskID            string                             `json:"task_id"`
	EnvironmentID     string                             `json:"environment_id"`
	ExecutionLocation string                             `json:"execution_location"`
	Configuration     Connector                          `json:"configuration"`
	ConnectorRevision int64                              `json:"connector_revision"`
	Draft             bool                               `json:"draft"`
	Stale             bool                               `json:"stale"`
	Phase             string                             `json:"phase" enum:"ready,checking,cleaning,finished,cleanup_required,transferred"`
	Outcome           string                             `json:"outcome,omitempty" enum:"passed,failed,interrupted"`
	Failure           s3connector.ConnectionCheckFailure `json:"failure,omitempty"`
	Stages            []ConnectorCheckStage              `json:"stages"`
	SecretSources     []ConnectorCheckSecretSource       `json:"secret_sources"`
	CleanupRequired   bool                               `json:"cleanup_required"`
	ObjectKey         string                             `json:"object_key,omitempty"`
	MultipartKey      string                             `json:"multipart_key,omitempty"`
	UploadIDs         []string                           `json:"upload_ids"`
	CleanupTaskID     string                             `json:"cleanup_task_id,omitempty"`
	CreatedAt         string                             `json:"created_at"`
	StartedAt         string                             `json:"started_at,omitempty"`
	FinishedAt        string                             `json:"finished_at,omitempty"`
}
