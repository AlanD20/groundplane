package etcd

import (
	"context"
	"reflect"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceRemovalIntentPrefix = "/v1/records/service-removal-intents/"

// ServiceRemovalIntent owns one sealed desired candidate while the active
// Service and desired head remain public until Agent cleanup succeeds.
type ServiceRemovalIntent struct {
	TaskID                    string                         `json:"task_id"`
	EnvironmentID             string                         `json:"environment_id"`
	ServiceID                 string                         `json:"service_id"`
	ServiceName               string                         `json:"service_name"`
	ServiceRevision           int64                          `json:"service_revision"`
	CurrentProjectionRevision int64                          `json:"current_projection_revision"`
	ExpectedHeadRevision      int64                          `json:"expected_head_revision"`
	Claim                     EnvironmentBlueprintStageClaim `json:"claim"`
	CurrentProjection         EnvironmentComposeProjection   `json:"current_projection"`
	CandidateProjection       EnvironmentComposeProjection   `json:"candidate_projection"`
	Status                    TaskStatus                     `json:"status"`
	CreatedAt                 time.Time                      `json:"created_at"`
	TerminalAt                *time.Time                     `json:"terminal_at,omitempty"`
}

func NewServiceRemovalIntent(
	taskID string,
	service Versioned[ServiceRecord],
	projection Versioned[EnvironmentComposeProjection],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	candidate EnvironmentComposeProjection,
	createdAt time.Time,
) (ServiceRemovalIntent, error) {
	intent := ServiceRemovalIntent{
		TaskID: taskID, EnvironmentID: service.Record.EnvironmentID,
		ServiceID: service.Record.Desired.ID, ServiceName: service.Record.Desired.Name,
		ServiceRevision: service.Revision, CurrentProjectionRevision: projection.Revision,
		ExpectedHeadRevision: expectedHeadRevision, Claim: claim,
		CurrentProjection:   cloneEnvironmentComposeProjection(projection.Record),
		CandidateProjection: cloneEnvironmentComposeProjection(candidate),
		Status:              TaskStatusPending, CreatedAt: createdAt,
	}
	if err := validateServiceRemovalIntent(intent); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return cloneServiceRemovalIntent(intent), nil
}

func serviceRemovalIntentKey(taskID string) string { return serviceRemovalIntentPrefix + taskID }

func (repository *ServiceRepository) GetServiceRemovalIntent(
	ctx context.Context,
	taskID string,
) (Versioned[ServiceRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ServiceRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return Versioned[ServiceRemovalIntent]{}, false, errs.New(errs.KindValidationFailed, "Service removal Task id is invalid")
	}
	result, err := repository.store.Get(ctx, serviceRemovalIntentKey(taskID))
	if err != nil {
		return Versioned[ServiceRemovalIntent]{}, false, err
	}
	if result == nil {
		return Versioned[ServiceRemovalIntent]{}, false, errs.New(errs.KindInternal, "Service removal intent read is empty")
	}
	if result.Entry == nil {
		return Versioned[ServiceRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeServiceRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return Versioned[ServiceRemovalIntent]{}, false, corruptServiceRemovalIntent()
	}
	return Versioned[ServiceRemovalIntent]{Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision}, true, nil
}

func terminalServiceRemovalIntent(intent ServiceRemovalIntent, status TaskStatus, at time.Time) (ServiceRemovalIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return ServiceRemovalIntent{}, errs.New(errs.KindStateConflict, "Service removal intent is not pending")
	}
	terminal := cloneServiceRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(at)
	if err := validateServiceRemovalIntent(terminal); err != nil {
		return ServiceRemovalIntent{}, err
	}
	return terminal, nil
}

func validateServiceRemovalIntent(intent ServiceRemovalIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil || ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, intent.ServiceID) != nil || intent.ServiceName == "" || intent.ServiceRevision <= 0 ||
		intent.CurrentProjectionRevision <= 0 || intent.ExpectedHeadRevision <= 0 ||
		validateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
		intent.Claim.EnvironmentID != intent.EnvironmentID || intent.Claim.RevisionID != intent.Claim.TaskID ||
		intent.Claim.BaselineHeadRevision != intent.ExpectedHeadRevision ||
		intent.Claim.SourceKind != EnvironmentBlueprintSourceMutation ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CurrentProjection.RevisionID == intent.CandidateProjection.RevisionID ||
		intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
		intent.CurrentProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
		intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration ||
		validateEnvironmentComposeProjection(intent.CurrentProjection) != nil ||
		validateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		validateTimestamp("Service removal created_at", intent.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal intent identity is invalid")
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Service removal intent has terminal time")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) ||
		validateTimestamp("Service removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal terminal state is invalid")
	}
	expected := cloneEnvironmentComposeProjection(intent.CurrentProjection)
	expected.RevisionID = intent.CandidateProjection.RevisionID
	expected.RenderGeneration = intent.CandidateProjection.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), intent.CandidateProjection.ComposeArtifact...)
	expected.Services = expected.Services[:0]
	removed := false
	for _, identity := range intent.CurrentProjection.Services {
		if identity.ID == intent.ServiceID && identity.Name == intent.ServiceName {
			removed = true
			continue
		}
		expected.Services = append(expected.Services, identity)
	}
	expected.VolumeMounts = expected.VolumeMounts[:0]
	for _, mount := range intent.CurrentProjection.VolumeMounts {
		if mount.ServiceID != intent.ServiceID {
			expected.VolumeMounts = append(expected.VolumeMounts, mount)
		}
	}
	expected.ServiceDependencyPlans = expected.ServiceDependencyPlans.WithoutService(intent.ServiceName)
	if !removed || !slices.Equal(expected.Services, intent.CandidateProjection.Services) ||
		!reflect.DeepEqual(expected, intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Service removal candidate projection changed")
	}
	return nil
}

func validateServiceRemovalTaskOwner(task TaskRecord, intent ServiceRemovalIntent) error {
	if task.ID != intent.TaskID || task.Executor != TaskExecutorAgent || task.Type != TaskRemove ||
		task.Target != intent.ServiceID || !task.CreatedAt.Equal(intent.CreatedAt) || len(task.Params) != 4 ||
		task.Params[TaskResourceKindParam] != TaskResourceService ||
		task.Params[TaskServiceEnvironmentParam] != intent.EnvironmentID ||
		task.Params[EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		task.Params[TaskComposeArtifactParam] == "" {
		return errs.New(errs.KindStateConflict, "Service removal intent does not belong to its Task")
	}
	return nil
}

func encodeServiceRemovalIntent(intent ServiceRemovalIntent) ([]byte, error) {
	if err := validateServiceRemovalIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("service_removal_intent", intent)
}

func decodeServiceRemovalIntent(value []byte) (ServiceRemovalIntent, error) {
	intent, err := decodeEnvelope[ServiceRemovalIntent](value, "service_removal_intent")
	if err != nil || validateServiceRemovalIntent(intent) != nil {
		return ServiceRemovalIntent{}, corruptServiceRemovalIntent()
	}
	return intent, nil
}

func cloneServiceRemovalIntent(intent ServiceRemovalIntent) ServiceRemovalIntent {
	intent.Claim.Intent.Ciphertext = append([]byte(nil), intent.Claim.Intent.Ciphertext...)
	intent.CurrentProjection = cloneEnvironmentComposeProjection(intent.CurrentProjection)
	intent.CandidateProjection = cloneEnvironmentComposeProjection(intent.CandidateProjection)
	intent.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return intent
}

func corruptServiceRemovalIntent() error {
	return errs.New(errs.KindInternal, "Service removal intent is corrupt")
}
