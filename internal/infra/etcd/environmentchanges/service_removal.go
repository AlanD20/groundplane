package environmentchanges

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceRemovalIntentPrefix = "/v1/records/service-removal-intents/"

// ServiceRemovalIntent owns one sealed desired candidate while the active
// Service and desired head remain public until Agent cleanup succeeds.
type ServiceRemovalIntent struct {
	TaskID                    string                                        `json:"task_id"`
	EnvironmentID             string                                        `json:"environment_id"`
	ServiceID                 string                                        `json:"service_id"`
	ServiceName               string                                        `json:"service_name"`
	ServiceRevision           int64                                         `json:"service_revision"`
	RuntimeRevision           int64                                         `json:"runtime_revision"`
	CurrentProjectionRevision int64                                         `json:"current_projection_revision"`
	ExpectedHeadRevision      int64                                         `json:"expected_head_revision"`
	Claim                     blueprints.EnvironmentBlueprintStageClaim     `json:"claim"`
	CurrentProjection         projectionrecord.EnvironmentComposeProjection `json:"current_projection"`
	CandidateProjection       projectionrecord.EnvironmentComposeProjection `json:"candidate_projection"`
	Status                    taskjournal.TaskStatus                        `json:"status"`
	CreatedAt                 time.Time                                     `json:"created_at"`
	TerminalAt                *time.Time                                    `json:"terminal_at,omitempty"`
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
	return cloneServiceRemovalIntent(intent), nil
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
	terminal := cloneServiceRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(at)
	if err := ValidateServiceRemovalIntent(terminal); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return terminal, nil
}

func ValidateServiceRemovalIntent(intent ServiceRemovalIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, intent.ServiceID) != nil ||
		intent.ServiceName == "" ||
		intent.ServiceRevision <= 0 ||
		intent.RuntimeRevision < 0 ||
		intent.CurrentProjectionRevision <= 0 ||
		intent.ExpectedHeadRevision <= 0 ||
		blueprints.ValidateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
		intent.Claim.EnvironmentID != intent.EnvironmentID ||
		intent.Claim.RevisionID != intent.Claim.TaskID ||
		intent.Claim.BaselineHeadRevision != intent.ExpectedHeadRevision ||
		intent.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CurrentProjection.RevisionID == intent.CandidateProjection.RevisionID ||
		intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
		intent.CurrentProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
		intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration ||
		projectionrecord.ValidateEnvironmentComposeProjection(intent.CurrentProjection) != nil ||
		projectionrecord.ValidateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		recordcodec.ValidateTimestamp("Service removal created_at", intent.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal intent identity is invalid")
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
	expected.VolumeMounts = expected.VolumeMounts[:0]
	for _, mount := range intent.CurrentProjection.VolumeMounts {
		if mount.ServiceID != intent.ServiceID {
			expected.VolumeMounts = append(expected.VolumeMounts, mount)
		}
	}
	expected.ServiceDependencyPlans = expected.ServiceDependencyPlans.WithoutService(intent.ServiceName)
	delete(expected.ServiceExtensions, intent.ServiceName)
	if !removedDesired || !SameServiceRemovalProjection(expected, intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Service removal candidate projection changed")
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

func cloneServiceRemovalIntent(intent ServiceRemovalIntent) ServiceRemovalIntent {
	intent.Claim.Intent.Ciphertext = append([]byte(nil), intent.Claim.Intent.Ciphertext...)
	intent.CurrentProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.CurrentProjection)
	intent.CandidateProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.CandidateProjection)
	intent.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return intent
}

func CorruptServiceRemovalIntent() error {
	return errs.New(errs.KindInternal, "Service removal intent is corrupt")
}
