package environmentchanges

import (
	"time"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceRemovalIntentPrefix = "/v1/records/service-removal-intents/"

// ServiceRemovalIntent owns one sealed desired candidate while the active
// Service and desired head remain public until Agent cleanup succeeds.
type ServiceRemovalIntent struct {
	TaskID                      string                                        `json:"task_id"`
	EnvironmentID               string                                        `json:"environment_id"`
	ServiceID                   string                                        `json:"service_id"`
	ServiceName                 string                                        `json:"service_name"`
	ServiceRevision             int64                                         `json:"service_revision"`
	RuntimeRevision             int64                                         `json:"runtime_revision"`
	AcknowledgedRuntimeRevision int64                                         `json:"acknowledged_runtime_revision"`
	AcknowledgedRuntime         *serviceruntimerecord.Record                  `json:"acknowledged_runtime,omitempty"`
	CurrentProjectionRevision   int64                                         `json:"current_projection_revision"`
	ExpectedHeadRevision        int64                                         `json:"expected_head_revision"`
	Claim                       blueprints.EnvironmentBlueprintStageClaim     `json:"claim"`
	Authored                    *ServiceRemovalAuthoredRevision               `json:"authored,omitempty"`
	CurrentProjection           projectionrecord.EnvironmentComposeProjection `json:"current_projection"`
	CandidateProjection         projectionrecord.EnvironmentComposeProjection `json:"candidate_projection"`
	Status                      taskjournal.TaskStatus                        `json:"status"`
	CreatedAt                   time.Time                                     `json:"created_at"`
	TerminalAt                  *time.Time                                    `json:"terminal_at,omitempty"`
}

// ServiceRemovalAuthoredRevision binds private host cleanup to the visible
// Blueprint parent that omitted this Service.
type ServiceRemovalAuthoredRevision struct {
	ParentTaskID     string `json:"parent_task_id"`
	RenderGeneration uint64 `json:"render_generation"`
}

func NewServiceRemovalIntent(
	taskID string,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	expectedHeadRevision int64,
	claim blueprints.EnvironmentBlueprintStageClaim,
	candidate projectionrecord.EnvironmentComposeProjection,
	createdAt time.Time,
) (ServiceRemovalIntent, error) {
	intent := ServiceRemovalIntent{
		TaskID: taskID, EnvironmentID: service.Record.EnvironmentID,
		ServiceID: service.Record.Desired.ID, ServiceName: service.Record.Desired.Name,
		ServiceRevision: service.Revision, RuntimeRevision: servicerecord.ServiceRuntimeRevision(service),
		CurrentProjectionRevision: projection.Revision,
		ExpectedHeadRevision:      expectedHeadRevision, Claim: claim,
		CurrentProjection:   projectionrecord.CloneEnvironmentComposeProjection(projection.Record),
		CandidateProjection: projectionrecord.CloneEnvironmentComposeProjection(candidate),
		Status:              taskjournal.TaskStatusPending, CreatedAt: createdAt,
	}
	if err := ValidateServiceRemovalIntent(intent); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return CloneServiceRemovalIntent(intent), nil
}

func NewAuthoredServiceRemovalIntent(
	taskID string,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	acknowledged etcdstore.Versioned[serviceruntimerecord.Record],
	expectedHeadRevision int64,
	parentTaskID string,
	renderGeneration uint64,
	candidate projectionrecord.EnvironmentComposeProjection,
	createdAt time.Time,
) (ServiceRemovalIntent, error) {
	if service.ReadRevision != projection.ReadRevision || acknowledged.ReadRevision != projection.ReadRevision ||
		acknowledged.Revision <= 0 {
		return ServiceRemovalIntent{}, errs.New(
			errs.KindStateConflict, "authored Service removal sources are not one fixed revision",
		)
	}
	intent := ServiceRemovalIntent{
		TaskID: taskID, EnvironmentID: service.Record.EnvironmentID,
		ServiceID: service.Record.Desired.ID, ServiceName: service.Record.Desired.Name,
		ServiceRevision: service.Revision, RuntimeRevision: servicerecord.ServiceRuntimeRevision(service),
		AcknowledgedRuntimeRevision: acknowledged.Revision,
		CurrentProjectionRevision:   projection.Revision,
		ExpectedHeadRevision:        expectedHeadRevision,
		Authored: &ServiceRemovalAuthoredRevision{
			ParentTaskID: parentTaskID, RenderGeneration: renderGeneration,
		},
		CurrentProjection:   projectionrecord.CloneEnvironmentComposeProjection(projection.Record),
		CandidateProjection: projectionrecord.CloneEnvironmentComposeProjection(candidate),
		Status:              taskjournal.TaskStatusPending, CreatedAt: createdAt,
	}
	if acknowledged.Revision > 0 {
		runtime := acknowledged.Record
		intent.AcknowledgedRuntime = &runtime
	}
	if err := ValidateServiceRemovalIntent(intent); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return CloneServiceRemovalIntent(intent), nil
}

func ServiceRemovalIntentKey(taskID string) string { return serviceRemovalIntentPrefix + taskID }

func TerminalServiceRemovalIntent(
	intent ServiceRemovalIntent,
	status taskjournal.TaskStatus,
	at time.Time,
) (ServiceRemovalIntent, error) {
	if intent.Status != taskjournal.TaskStatusPending || !taskjournal.IsTerminalTaskStatus(status) {
		return ServiceRemovalIntent{}, errs.New(errs.KindStateConflict, "Service removal intent is not pending")
	}
	terminal := CloneServiceRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(at)
	if err := ValidateServiceRemovalIntent(terminal); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return terminal, nil
}

func ValidateServiceRemovalIntent(intent ServiceRemovalIntent) error {
	if intent.AcknowledgedRuntimeRevision < 0 ||
		(intent.AcknowledgedRuntime == nil) != (intent.AcknowledgedRuntimeRevision == 0) {
		return errs.New(errs.KindValidationFailed, "Service removal runtime source is invalid")
	}
	if source := intent.AcknowledgedRuntime; source != nil &&
		(source.EnvironmentID != intent.EnvironmentID || source.Runtime.ServiceID != intent.ServiceID ||
			serviceruntimerecord.Validate(*source) != nil) {
		return errs.New(errs.KindValidationFailed, "Service removal runtime source changed")
	}
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, intent.ServiceID) != nil ||
		intent.ServiceName == "" ||
		intent.ServiceRevision <= 0 ||
		intent.RuntimeRevision < 0 ||
		intent.CurrentProjectionRevision <= 0 ||
		intent.ExpectedHeadRevision <= 0 ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		projectionrecord.ValidateEnvironmentComposeProjection(intent.CurrentProjection) != nil ||
		projectionrecord.ValidateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		recordcodec.ValidateTimestamp("Service removal created_at", intent.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal intent identity is invalid")
	}
	if intent.Authored == nil {
		if blueprints.ValidateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
			intent.Claim.EnvironmentID != intent.EnvironmentID ||
			intent.Claim.RevisionID != intent.Claim.TaskID ||
			intent.Claim.BaselineHeadRevision != intent.ExpectedHeadRevision ||
			intent.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
			intent.CurrentProjection.RevisionID == intent.CandidateProjection.RevisionID ||
			intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
			intent.CurrentProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
			intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration {
			return errs.New(errs.KindValidationFailed, "Service removal desired revision is invalid")
		}
	} else if intent.Claim.TaskID != "" ||
		ids.Validate(ids.KindTask, intent.Authored.ParentTaskID) != nil ||
		intent.Authored.RenderGeneration == 0 ||
		intent.CandidateProjection.RevisionID != intent.Authored.ParentTaskID ||
		intent.CandidateProjection.RenderGeneration != intent.Authored.RenderGeneration ||
		intent.Authored.RenderGeneration < intent.CurrentProjection.RenderGeneration ||
		intent.Authored.RenderGeneration == intent.CurrentProjection.RenderGeneration &&
			intent.CurrentProjection.RevisionID != intent.Authored.ParentTaskID ||
		intent.AcknowledgedRuntime == nil || intent.AcknowledgedRuntimeRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Service removal authored revision is invalid")
	}
	if intent.Status == taskjournal.TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Service removal intent has terminal time")
		}
	} else if !taskjournal.IsTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) ||
		recordcodec.ValidateTimestamp("Service removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal terminal state is invalid")
	}
	expected := projectionrecord.CloneEnvironmentComposeProjection(intent.CurrentProjection)
	expected.RevisionID = intent.CandidateProjection.RevisionID
	expected.RenderGeneration = intent.CandidateProjection.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), intent.CandidateProjection.ComposeArtifact...)
	expected.NormalizedCompose = append([]byte(nil), intent.CandidateProjection.NormalizedCompose...)
	expected.DesiredServices = nil
	removedDesired := false
	for _, desired := range intent.CurrentProjection.DesiredServices {
		if desired.Desired.ID == intent.ServiceID && desired.Desired.Name == intent.ServiceName {
			removedDesired = true
			continue
		}
		expected.DesiredServices = append(expected.DesiredServices, desired)
	}
	expected.VolumeMounts = nil
	for _, mount := range intent.CurrentProjection.VolumeMounts {
		if mount.ServiceID != intent.ServiceID {
			expected.VolumeMounts = append(expected.VolumeMounts, mount)
		}
	}
	expected.ServiceDependencyPlans = expected.ServiceDependencyPlans.WithoutService(intent.ServiceName)
	delete(expected.ServiceExtensions, intent.ServiceName)
	if len(expected.ServiceExtensions) == 0 {
		expected.ServiceExtensions = nil
	}
	if !removedDesired || !SameServiceRemovalProjection(expected, intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Service removal candidate projection changed")
	}
	if intent.Authored != nil {
		if _, err := ResolveAuthoredServiceRemovalRecovery(intent); err != nil {
			return err
		}
	}
	return nil
}

func EncodeServiceRemovalIntent(intent ServiceRemovalIntent) ([]byte, error) {
	if err := ValidateServiceRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("service_removal_intent", intent)
}

func DecodeServiceRemovalIntent(value []byte) (ServiceRemovalIntent, error) {
	intent, err := recordcodec.Decode[ServiceRemovalIntent](value, "service_removal_intent")
	if err != nil || ValidateServiceRemovalIntent(intent) != nil {
		return ServiceRemovalIntent{}, CorruptServiceRemovalIntent()
	}
	return intent, nil
}

func CloneServiceRemovalIntent(intent ServiceRemovalIntent) ServiceRemovalIntent {
	if intent.Authored != nil {
		authored := *intent.Authored
		intent.Authored = &authored
	}
	if intent.AcknowledgedRuntime != nil {
		source := *intent.AcknowledgedRuntime
		source.Runtime.CurrentArtifact = append([]byte(nil), source.Runtime.CurrentArtifact...)
		source.Runtime.RetainedPriorArtifact = append([]byte(nil), source.Runtime.RetainedPriorArtifact...)
		source.Runtime.ProxyConfigSHA256 = append([]byte(nil), source.Runtime.ProxyConfigSHA256...)
		intent.AcknowledgedRuntime = &source
	}
	intent.Claim.Intent.Ciphertext = append([]byte(nil), intent.Claim.Intent.Ciphertext...)
	intent.CurrentProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.CurrentProjection)
	intent.CandidateProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.CandidateProjection)
	intent.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return intent
}

func CorruptServiceRemovalIntent() error {
	return errs.New(errs.KindInternal, "Service removal intent is corrupt")
}
