package environmentchanges

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const attachRemovalIntentPrefix = "/v1/records/authored-attach-removal-intents/"

// AttachRemovalSource records one exact Attach member used to derive the
// complete network-withdrawal projection. Records are sorted by stable id.
type AttachRemovalSource struct {
	Record   attachrecord.Record `json:"record"`
	Revision int64               `json:"revision"`
}

// AttachRemovalIntent is the immutable Controller authority for one authored
// Detach. It retains ciphertext, but never decrypted fact or hook values.
type AttachRemovalIntent struct {
	TaskID                     string                                        `json:"task_id"`
	ParentTaskID               string                                        `json:"parent_task_id"`
	EnvironmentID              string                                        `json:"environment_id"`
	AttachID                   string                                        `json:"attach_id"`
	ExpectedHeadRevision       int64                                         `json:"expected_head_revision"`
	AppliedProjectionRevision  int64                                         `json:"applied_projection_revision"`
	SourceRevision             int64                                         `json:"source_revision"`
	FactsRevision              int64                                         `json:"facts_revision"`
	TenantRevision             int64                                         `json:"tenant_revision"`
	ProjectRevision            int64                                         `json:"project_revision"`
	EnvironmentRevision        int64                                         `json:"environment_revision"`
	BackingProjectRevision     int64                                         `json:"backing_project_revision"`
	BackingEnvironmentRevision int64                                         `json:"backing_environment_revision"`
	BackingDesiredKey          string                                        `json:"backing_desired_key"`
	BackingDesiredRevision     int64                                         `json:"backing_desired_revision"`
	BackingRuntimeRevision     int64                                         `json:"backing_runtime_revision"`
	Source                     attachrecord.Record                           `json:"source"`
	Detaching                  attachrecord.Record                           `json:"detaching"`
	Facts                      *attachrecord.EncryptedFacts                  `json:"facts,omitempty"`
	Topology                   []AttachRemovalSource                         `json:"topology"`
	AppliedProjection          projectionrecord.EnvironmentComposeProjection `json:"applied_projection"`
	RenderInput                attachrender.AttachTaskRenderInput            `json:"render_input"`
	Status                     taskjournal.TaskStatus                        `json:"status"`
	CreatedAt                  time.Time                                     `json:"created_at"`
	TerminalAt                 *time.Time                                    `json:"terminal_at,omitempty"`
}

func AttachRemovalIntentKey(taskID string) string { return attachRemovalIntentPrefix + taskID }

func TerminalAttachRemovalIntent(
	intent AttachRemovalIntent, status taskjournal.TaskStatus, at time.Time,
) (AttachRemovalIntent, error) {
	if intent.Status != taskjournal.TaskStatusPending || !taskjournal.IsTerminalTaskStatus(status) {
		return AttachRemovalIntent{}, errs.New(errs.KindStateConflict, "authored Attach removal intent is not pending")
	}
	terminal := CloneAttachRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(at)
	if err := ValidateAttachRemovalIntent(terminal); err != nil {
		ClearAttachRemovalIntent(&terminal)
		return AttachRemovalIntent{}, err
	}
	return terminal, nil
}

func ValidateAttachRemovalIntent(intent AttachRemovalIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindTask, intent.ParentTaskID) != nil || intent.TaskID == intent.ParentTaskID ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindAttach, intent.AttachID) != nil ||
		intent.ExpectedHeadRevision <= 0 || intent.AppliedProjectionRevision <= 0 || intent.SourceRevision <= 0 ||
		intent.FactsRevision < 0 || intent.TenantRevision <= 0 || intent.ProjectRevision <= 0 ||
		intent.EnvironmentRevision <= 0 || intent.BackingProjectRevision <= 0 ||
		intent.BackingEnvironmentRevision <= 0 || intent.BackingDesiredKey == "" ||
		intent.BackingDesiredRevision <= 0 || intent.BackingRuntimeRevision <= 0 ||
		recordcodec.ValidateTimestamp("authored Attach removal created_at", intent.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "authored Attach removal identity is invalid")
	}
	if attachrecord.ValidateAttachRecord(intent.Source) != nil ||
		attachrecord.ValidateAttachRecord(intent.Detaching) != nil ||
		intent.Source.ID != intent.AttachID || intent.Source.EnvironmentID != intent.EnvironmentID ||
		intent.Detaching.ID != intent.AttachID || intent.Detaching.EnvironmentID != intent.EnvironmentID ||
		intent.Detaching.Status != core.AttachDetaching || intent.Detaching.Operation != attachrecord.AttachOperationDetach ||
		intent.Detaching.TaskID != intent.TaskID || !sameAttachRemovalStableRecord(intent.Source, intent.Detaching) {
		return errs.New(errs.KindValidationFailed, "authored Attach removal predecessor changed")
	}
	if (intent.Facts == nil) != (intent.FactsRevision == 0) {
		return errs.New(errs.KindValidationFailed, "authored Attach removal fact authority is invalid")
	}
	if intent.Facts != nil && (attachrecord.ValidateAttachEncryptedFacts(*intent.Facts) != nil ||
		intent.Facts.AttachID != intent.AttachID) {
		return errs.New(errs.KindValidationFailed, "authored Attach removal facts changed")
	}
	if projectionrecord.ValidateEnvironmentComposeProjection(intent.AppliedProjection) != nil ||
		intent.AppliedProjection.EnvironmentID != intent.EnvironmentID ||
		attachrender.ValidateAttachTaskRenderInput(intent.RenderInput) != nil ||
		intent.RenderInput.AttachID != intent.AttachID || intent.RenderInput.EnvironmentID != intent.EnvironmentID ||
		intent.RenderInput.DesiredRevisionID != intent.AppliedProjection.RevisionID ||
		intent.RenderInput.RenderGeneration != intent.AppliedProjection.RenderGeneration ||
		intent.RenderInput.PlanID == "" || intent.RenderInput.BackingServiceID != intent.Source.BackingServiceID ||
		!slices.Equal(intent.RenderInput.ConsumerServiceIDs, []string{intent.Source.ServiceID}) ||
		!slices.Equal(intent.RenderInput.GrantAttachIDs, intent.Source.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "authored Attach removal render authority changed")
	}
	foundSource := false
	previous := ""
	for _, source := range intent.Topology {
		if source.Revision <= 0 || attachrecord.ValidateAttachRecord(source.Record) != nil ||
			source.Record.EnvironmentID != intent.EnvironmentID || source.Record.ID <= previous {
			return errs.New(errs.KindValidationFailed, "authored Attach removal topology is invalid")
		}
		if source.Record.ID == intent.AttachID {
			foundSource = source.Revision == intent.SourceRevision &&
				sameAttachRemovalRecord(source.Record, intent.Source)
		}
		previous = source.Record.ID
	}
	if !foundSource {
		return errs.New(errs.KindValidationFailed, "authored Attach removal topology omitted its source")
	}
	if intent.Status == taskjournal.TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending authored Attach removal has terminal time")
		}
	} else if !taskjournal.IsTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
		intent.TerminalAt.Before(intent.CreatedAt) ||
		recordcodec.ValidateTimestamp("authored Attach removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "authored Attach removal terminal state is invalid")
	}
	return nil
}

func EncodeAttachRemovalIntent(intent AttachRemovalIntent) ([]byte, error) {
	if err := ValidateAttachRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("authored_attach_removal_intent", intent)
}

func DecodeAttachRemovalIntent(value []byte) (AttachRemovalIntent, error) {
	intent, err := recordcodec.Decode[AttachRemovalIntent](value, "authored_attach_removal_intent")
	if err != nil || ValidateAttachRemovalIntent(intent) != nil {
		ClearAttachRemovalIntent(&intent)
		return AttachRemovalIntent{}, errs.New(errs.KindInternal, "authored Attach removal intent is corrupt")
	}
	return intent, nil
}

func CloneAttachRemovalIntent(intent AttachRemovalIntent) AttachRemovalIntent {
	clone := intent
	clone.Source = attachrecord.CloneAttachRecord(intent.Source)
	clone.Detaching = attachrecord.CloneAttachRecord(intent.Detaching)
	if intent.Facts != nil {
		facts := *intent.Facts
		facts.Ciphertext = append([]byte(nil), intent.Facts.Ciphertext...)
		clone.Facts = &facts
	}
	clone.Topology = make([]AttachRemovalSource, len(intent.Topology))
	for index, source := range intent.Topology {
		clone.Topology[index] = source
		clone.Topology[index].Record = attachrecord.CloneAttachRecord(source.Record)
	}
	clone.AppliedProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.AppliedProjection)
	clone.RenderInput = cloneAttachRemovalRenderInput(intent.RenderInput)
	clone.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return clone
}

func ClearAttachRemovalIntent(intent *AttachRemovalIntent) {
	if intent == nil {
		return
	}
	if intent.Facts != nil {
		clear(intent.Facts.Ciphertext)
	}
	clear(intent.AppliedProjection.ComposeArtifact)
	clear(intent.RenderInput.RuntimeProjection.ComposeArtifact)
	*intent = AttachRemovalIntent{}
}

func sameAttachRemovalStableRecord(left, right attachrecord.Record) bool {
	return left.ID == right.ID && left.EnvironmentID == right.EnvironmentID && left.Name == right.Name &&
		left.BackingProjectID == right.BackingProjectID &&
		left.BackingEnvironmentID == right.BackingEnvironmentID &&
		left.BackingServiceID == right.BackingServiceID && left.BackingNetworkID == right.BackingNetworkID &&
		left.ServiceID == right.ServiceID && left.CredentialAttachID == right.CredentialAttachID &&
		slices.Equal(
			left.GrantAttachIDs,
			right.GrantAttachIDs,
		) && sameAttachRemovalFactSets(left.FactSets, right.FactSets) &&
		left.HookBundle == right.HookBundle && left.CreatedAt.Equal(right.CreatedAt)
}

func sameAttachRemovalFactSets(left, right []attachrecord.FactSetMetadata) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].GrantAttachID != right[index].GrantAttachID ||
			!slices.Equal(left[index].Facts, right[index].Facts) {
			return false
		}
	}
	return true
}

func sameAttachRemovalRecord(left, right attachrecord.Record) bool {
	return sameAttachRemovalStableRecord(left, right) && left.Status == right.Status &&
		left.Operation == right.Operation && left.TaskID == right.TaskID
}

func cloneAttachRemovalRenderInput(input attachrender.AttachTaskRenderInput) attachrender.AttachTaskRenderInput {
	value, err := attachrender.EncodeAttachTaskRenderInput(input)
	if err != nil {
		return attachrender.AttachTaskRenderInput{}
	}
	defer clear(value)
	cloned, err := attachrender.DecodeAttachTaskRenderInput(value)
	if err != nil {
		return attachrender.AttachTaskRenderInput{}
	}
	return cloned
}
