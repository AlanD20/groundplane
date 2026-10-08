package api

type SoftwareSelection string

const (
	SoftwareController SoftwareSelection = "controller"
	SoftwareAgent      SoftwareSelection = "agent"
	SoftwareBoth       SoftwareSelection = "both"
)

type SoftwareSourceKind string

const (
	SoftwareSourceRef SoftwareSourceKind = "source_ref"
	SoftwareRelease   SoftwareSourceKind = "release"
)

// SoftwarePreparationRequest selects software, never a command or build path.
// Both source builds resolve one commit; Both releases select matching
// controller/vX.Y.Z and agent/vX.Y.Z releases from the requested vX.Y.Z label.
type SoftwarePreparationRequest struct {
	Selection  SoftwareSelection  `json:"selection" enum:"controller,agent,both"`
	SourceKind SoftwareSourceKind `json:"source_kind" enum:"source_ref,release"`
	Ref        string             `json:"ref" minLength:"1" maxLength:"256"`
}

type SoftwareReleaseChoice struct {
	Ref         string            `json:"ref"`
	Selection   SoftwareSelection `json:"selection" enum:"controller,agent,both"`
	Name        string            `json:"name"`
	PublishedAt string            `json:"published_at" format:"date-time"`
}

type SoftwareReleaseCatalog struct {
	Items []SoftwareReleaseChoice `json:"items" nullable:"false"`
}

type SoftwareProvenance struct {
	Component string `json:"component" enum:"controller,agent"`
	Ref       string `json:"ref"`
	Commit    string `json:"commit"`
}

type PreparedSoftwareArtifact struct {
	Component      string `json:"component" enum:"controller,agent"`
	Reference      string `json:"reference"`
	ManifestDigest string `json:"manifest_digest"`
	OS             string `json:"os"`
	Architecture   string `json:"architecture"`
}

type SoftwarePreparation struct {
	TaskID      string                     `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Selection   SoftwareSelection          `json:"selection" enum:"controller,agent,both"`
	SourceKind  SoftwareSourceKind         `json:"source_kind" enum:"source_ref,release"`
	Ref         string                     `json:"ref"`
	Phase       string                     `json:"phase" enum:"accepted,preparing,agent_published,controller_published,verified,failed"`
	CreatedAt   string                     `json:"created_at" format:"date-time"`
	Provenance  []SoftwareProvenance       `json:"provenance" nullable:"false"`
	Artifacts   []PreparedSoftwareArtifact `json:"artifacts" nullable:"false"`
	ErrorCode   string                     `json:"error_code,omitempty"`
	ErrorDetail string                     `json:"error_detail,omitempty"`
}

type SoftwarePreparationPage struct {
	Items      []SoftwarePreparation `json:"items" nullable:"false"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

// SoftwareActivation reports component outcomes separately. Controller success
// is not rolled back merely because the subsequent Agent update failed.
type SoftwareActivation struct {
	TaskID            string            `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	PreparationTaskID string            `json:"preparation_task_id"`
	Selection         SoftwareSelection `json:"selection" enum:"controller,agent,both"`
	Phase             string            `json:"phase"`
	ControllerTaskID  string            `json:"controller_task_id,omitempty"`
	AgentTaskID       string            `json:"agent_task_id,omitempty"`
	ControllerApplied bool              `json:"controller_applied"`
	AgentApplied      bool              `json:"agent_applied"`
	ErrorCode         string            `json:"error_code,omitempty"`
	ErrorDetail       string            `json:"error_detail,omitempty"`
}
