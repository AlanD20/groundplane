package api

type SoftwareSelection string

const (
	SoftwareController SoftwareSelection = "controller"
	SoftwareAgent      SoftwareSelection = "agent"
	SoftwareConsole    SoftwareSelection = "console"
	SoftwareBoth       SoftwareSelection = "both"
	SoftwareAll        SoftwareSelection = "all"
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
	After      []string           `json:"after,omitempty" maxItems:"16" uniqueItems:"true"`
	Selection  SoftwareSelection  `json:"selection" enum:"controller,agent,console,both,all"`
	SourceKind SoftwareSourceKind `json:"source_kind" enum:"source_ref,release"`
	Ref        string             `json:"ref" minLength:"1" maxLength:"256"`
}

type SoftwareReleaseChoice struct {
	Ref         string            `json:"ref"`
	Selection   SoftwareSelection `json:"selection" enum:"controller,agent,console,both,all"`
	Name        string            `json:"name"`
	PublishedAt string            `json:"published_at" format:"date-time"`
}

type SoftwareReleaseCatalog struct {
	Items []SoftwareReleaseChoice `json:"items" nullable:"false"`
}

type SoftwareProvenance struct {
	Component string `json:"component" enum:"controller,agent,console"`
	Ref       string `json:"ref"`
	Commit    string `json:"commit"`
}

type PreparedSoftwareArtifact struct {
	Component      string `json:"component" enum:"controller,agent,console"`
	Reference      string `json:"reference"`
	ManifestDigest string `json:"manifest_digest"`
	OS             string `json:"os"`
	Architecture   string `json:"architecture"`
}

type SoftwarePreparation struct {
	TaskID      string                     `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Selection   SoftwareSelection          `json:"selection" enum:"controller,agent,console,both,all"`
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
	TaskID               string            `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	PreparationTaskID    string            `json:"preparation_task_id"`
	Selection            SoftwareSelection `json:"selection" enum:"controller,agent,console,both,all"`
	Phase                string            `json:"phase"`
	ControllerWorkItemID string            `json:"controller_work_item_id,omitempty"`
	AgentWorkItemID      string            `json:"agent_work_item_id,omitempty"`
	ConsoleWorkItemID    string            `json:"console_work_item_id,omitempty"`
	ControllerApplied    bool              `json:"controller_applied"`
	AgentApplied         bool              `json:"agent_applied"`
	ConsoleApplied       bool              `json:"console_applied"`
	ErrorCode            string            `json:"error_code,omitempty"`
	ErrorDetail          string            `json:"error_detail,omitempty"`
}
