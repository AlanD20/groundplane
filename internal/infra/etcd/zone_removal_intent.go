package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const zoneRemovalIntentPrefix = "/v1/records/zone-removal-intents/"

// ZoneRemovalIntent is the operation-stable authority for one staged Zone
// removal. Attempts may change, but the desired claim and candidate do not.
type ZoneRemovalIntent struct {
	OperationID               string                         `json:"operation_id"`
	ActiveTaskID              string                         `json:"active_task_id"`
	ActiveTaskCreatedAt       time.Time                      `json:"active_task_created_at"`
	EnvironmentID             string                         `json:"environment_id"`
	ZoneID                    string                         `json:"zone_id"`
	ZoneName                  string                         `json:"zone_name"`
	ZoneRevision              int64                          `json:"zone_revision"`
	DesiredHeadRevision       int64                          `json:"desired_head_revision"`
	AppliedProjectionRevision int64                          `json:"applied_projection_revision"`
	Claim                     EnvironmentBlueprintStageClaim `json:"claim"`
	DesiredProjection         EnvironmentComposeProjection   `json:"desired_projection"`
	AppliedProjection         EnvironmentComposeProjection   `json:"applied_projection"`
	CandidateProjection       EnvironmentComposeProjection   `json:"candidate_projection"`
	AffectedServiceIDs        []string                       `json:"affected_service_ids"`
	Status                    TaskStatus                     `json:"status"`
	CreatedAt                 time.Time                      `json:"created_at"`
	UpdatedAt                 time.Time                      `json:"updated_at"`
	TerminalAt                *time.Time                     `json:"terminal_at,omitempty"`
}

func NewZoneRemovalIntent(
	operationID string,
	taskID string,
	zone Versioned[ZoneRecord],
	authorities EnvironmentZoneRemovalAuthorities,
	claim EnvironmentBlueprintStageClaim,
	candidate EnvironmentComposeProjection,
	affected []string,
	createdAt time.Time,
) (ZoneRemovalIntent, error) {
	intent := ZoneRemovalIntent{
		OperationID: operationID, ActiveTaskID: taskID, ActiveTaskCreatedAt: createdAt,
		EnvironmentID: zone.Record.EnvironmentID, ZoneID: zone.Record.Desired.ID,
		ZoneName: zone.Record.Desired.Name, ZoneRevision: zone.Revision,
		DesiredHeadRevision:       authorities.Desired.Revision,
		AppliedProjectionRevision: authorities.Applied.Revision,
		Claim:                     cloneEnvironmentBlueprintStageClaim(claim),
		DesiredProjection:         cloneEnvironmentComposeProjection(authorities.Desired.Record),
		AppliedProjection:         cloneEnvironmentComposeProjection(authorities.Applied.Record),
		CandidateProjection:       cloneEnvironmentComposeProjection(candidate),
		AffectedServiceIDs:        append([]string(nil), affected...),
		Status:                    TaskStatusPending, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if err := validateZoneRemovalIntent(intent); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return cloneZoneRemovalIntent(intent), nil
}

func zoneRemovalIntentKey(operationID string) string { return zoneRemovalIntentPrefix + operationID }

func (repository *HierarchyRepository) GetZoneRemovalIntent(
	ctx context.Context,
	operationID string,
) (Versioned[ZoneRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ZoneRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindOperation, operationID) != nil {
		return Versioned[ZoneRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Zone removal operation id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, zoneRemovalIntentKey(operationID))
	if err != nil {
		return Versioned[ZoneRemovalIntent]{}, false, err
	}
	if result == nil {
		return Versioned[ZoneRemovalIntent]{}, false, errs.New(errs.KindInternal, "Zone removal intent read is empty")
	}
	if result.Entry == nil {
		return Versioned[ZoneRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeZoneRemovalIntent(result.Entry.Value)
	if err != nil || intent.OperationID != operationID {
		return Versioned[ZoneRemovalIntent]{}, false, corruptZoneRemovalIntent()
	}
	return Versioned[ZoneRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func TransferZoneRemovalIntent(
	intent ZoneRemovalIntent,
	taskID string,
	createdAt time.Time,
) (ZoneRemovalIntent, error) {
	if !isTerminalTaskStatus(intent.Status) && intent.Status != TaskStatusPending {
		return ZoneRemovalIntent{}, errs.New(errs.KindStateConflict, "Zone removal intent cannot transfer")
	}
	next := cloneZoneRemovalIntent(intent)
	next.ActiveTaskID = taskID
	next.ActiveTaskCreatedAt = createdAt
	next.Status = TaskStatusPending
	next.UpdatedAt = createdAt
	next.TerminalAt = nil
	if err := validateZoneRemovalIntent(next); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return next, nil
}

func terminalZoneRemovalIntent(intent ZoneRemovalIntent, status TaskStatus, at time.Time) (ZoneRemovalIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return ZoneRemovalIntent{}, errs.New(errs.KindStateConflict, "Zone removal intent is not pending")
	}
	next := cloneZoneRemovalIntent(intent)
	next.Status = status
	next.UpdatedAt = at
	next.TerminalAt = timePointer(at)
	if err := validateZoneRemovalIntent(next); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return next, nil
}

func validateZoneRemovalIntent(intent ZoneRemovalIntent) error {
	if ids.Validate(ids.KindOperation, intent.OperationID) != nil ||
		ids.Validate(ids.KindTask, intent.ActiveTaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindNetwork, intent.ZoneID) != nil ||
		intent.ZoneName == "" ||
		intent.ZoneRevision <= 0 ||
		intent.DesiredHeadRevision <= 0 ||
		intent.AppliedProjectionRevision <= 0 ||
		validateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
		intent.Claim.EnvironmentID != intent.EnvironmentID ||
		intent.Claim.RevisionID != intent.Claim.TaskID ||
		intent.Claim.BaselineHeadRevision != intent.DesiredHeadRevision ||
		intent.Claim.SourceKind != EnvironmentBlueprintSourceMutation ||
		intent.DesiredProjection.EnvironmentID != intent.EnvironmentID ||
		intent.AppliedProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
		intent.DesiredProjection.RevisionID == intent.CandidateProjection.RevisionID ||
		intent.DesiredProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
		intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration ||
		validateEnvironmentComposeProjection(intent.DesiredProjection) != nil ||
		validateEnvironmentComposeProjection(intent.AppliedProjection) != nil ||
		validateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		recordcodec.ValidateTimestamp("Zone removal created_at", intent.CreatedAt) != nil ||
		recordcodec.ValidateTimestamp("Zone removal active Task created_at", intent.ActiveTaskCreatedAt) != nil ||
		recordcodec.ValidateTimestamp("Zone removal updated_at", intent.UpdatedAt) != nil ||
		intent.ActiveTaskCreatedAt.Before(intent.CreatedAt) ||
		intent.UpdatedAt.Before(intent.ActiveTaskCreatedAt) {
		return errs.New(errs.KindValidationFailed, "Zone removal intent identity is invalid")
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Zone removal intent has terminal time")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
		!intent.TerminalAt.Equal(intent.UpdatedAt) || recordcodec.ValidateTimestamp("Zone removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "Zone removal terminal state is invalid")
	}
	expected := cloneEnvironmentComposeProjection(intent.DesiredProjection)
	expected.RevisionID = intent.CandidateProjection.RevisionID
	expected.RenderGeneration = intent.CandidateProjection.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), intent.CandidateProjection.ComposeArtifact...)
	expected.NormalizedCompose = append([]byte(nil), intent.CandidateProjection.NormalizedCompose...)
	expected.DesiredZones = nil
	foundZone := false
	for _, zone := range intent.DesiredProjection.DesiredZones {
		if zone.Desired.ID == intent.ZoneID && zone.Desired.Name == intent.ZoneName {
			if foundZone {
				return corruptZoneRemovalIntent()
			}
			foundZone = true
			continue
		}
		expected.DesiredZones = append(expected.DesiredZones, zone)
	}
	expected.DesiredServices = append([]EnvironmentServiceProjection(nil), intent.DesiredProjection.DesiredServices...)
	affected := make([]string, 0)
	for index, service := range intent.DesiredProjection.DesiredServices {
		expected.DesiredServices[index] = service
		expected.DesiredServices[index].Desired.Zones = nil
		removed := false
		for _, name := range service.Desired.Zones {
			if name == intent.ZoneName {
				removed = true
				continue
			}
			expected.DesiredServices[index].Desired.Zones = append(expected.DesiredServices[index].Desired.Zones, name)
		}
		if removed {
			affected = append(affected, service.Desired.ID)
		}
	}
	sort.Strings(affected)
	if !foundZone || !sort.StringsAreSorted(intent.AffectedServiceIDs) ||
		!slices.Equal(affected, intent.AffectedServiceIDs) {
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection changed")
	}
	if !sameZoneRemovalProjection(expected, intent.CandidateProjection) {
		return zoneRemovalProjectionChanged(expected, intent.CandidateProjection)
	}
	return nil
}

func zoneRemovalProjectionChanged(expected, candidate EnvironmentComposeProjection) error {
	switch {
	case expected.EnvironmentID != candidate.EnvironmentID || expected.RevisionID != candidate.RevisionID ||
		expected.RenderGeneration != candidate.RenderGeneration:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection identity changed")
	case !sameServiceRemovalBytes(expected.ComposeArtifact, candidate.ComposeArtifact) ||
		!sameServiceRemovalBytes(expected.NormalizedCompose, candidate.NormalizedCompose):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection artifact changed")
	case !sameServiceRemovalDesiredZones(expected.DesiredZones, candidate.DesiredZones):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Zones changed")
	case !sameServiceRemovalDesiredServices(expected.DesiredServices, candidate.DesiredServices):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Services changed")
	default:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection dependencies changed")
	}
}

func sameZoneRemovalProjection(left, right EnvironmentComposeProjection) bool {
	return sameServiceRemovalProjection(left, right)
}

func validateZoneRemovalTaskOwner(task TaskRecord, intent ZoneRemovalIntent) error {
	if task.ID != intent.ActiveTaskID || task.Target != intent.ZoneID || task.Type != TaskRemove ||
		!task.CreatedAt.Equal(
			intent.ActiveTaskCreatedAt,
		) || task.Params[TaskZoneRemovalOperationParam] != intent.OperationID ||
		task.Params[TaskZoneEnvironmentParam] != intent.EnvironmentID ||
		task.Params[EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID {
		return errs.New(errs.KindStateConflict, "Zone removal intent does not belong to its Task")
	}
	if task.Executor == TaskExecutorAgent {
		if len(task.Params) != 4 || ids.Validate(ids.KindConfig, task.Params[TaskComposeArtifactParam]) != nil {
			return errs.New(errs.KindStateConflict, "Zone removal Agent Task input changed")
		}
		return nil
	}
	if task.Executor != TaskExecutorController || len(task.Params) != 5 ||
		task.Params[TaskResourceKindParam] != TaskResourceBackingZone || !recordcodec.ValidSHA256(task.Params[TaskZoneImpactTokenParam]) {
		return errs.New(errs.KindStateConflict, "Zone removal Controller Task input changed")
	}
	return nil
}

func encodeZoneRemovalIntent(intent ZoneRemovalIntent) ([]byte, error) {
	if err := validateZoneRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("zone_removal_intent", intent)
}

func decodeZoneRemovalIntent(value []byte) (ZoneRemovalIntent, error) {
	intent, err := recordcodec.Decode[ZoneRemovalIntent](value, "zone_removal_intent")
	if err != nil || validateZoneRemovalIntent(intent) != nil {
		return ZoneRemovalIntent{}, corruptZoneRemovalIntent()
	}
	return intent, nil
}

func cloneZoneRemovalIntent(intent ZoneRemovalIntent) ZoneRemovalIntent {
	clone := intent
	clone.Claim = cloneEnvironmentBlueprintStageClaim(intent.Claim)
	clone.DesiredProjection = cloneEnvironmentComposeProjection(intent.DesiredProjection)
	clone.AppliedProjection = cloneEnvironmentComposeProjection(intent.AppliedProjection)
	clone.CandidateProjection = cloneEnvironmentComposeProjection(intent.CandidateProjection)
	clone.AffectedServiceIDs = append([]string(nil), intent.AffectedServiceIDs...)
	clone.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return clone
}

func corruptZoneRemovalIntent() error {
	return errs.New(errs.KindInternal, "Zone removal intent is corrupt")
}
