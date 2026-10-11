package environmentchanges

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
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
	OperationID               string                                        `json:"operation_id"`
	ActiveTaskID              string                                        `json:"active_task_id"`
	ActiveTaskCreatedAt       time.Time                                     `json:"active_task_created_at"`
	EnvironmentID             string                                        `json:"environment_id"`
	ZoneID                    string                                        `json:"zone_id"`
	ZoneName                  string                                        `json:"zone_name"`
	ZoneRevision              int64                                         `json:"zone_revision"`
	DesiredHeadRevision       int64                                         `json:"desired_head_revision"`
	AppliedProjectionRevision int64                                         `json:"applied_projection_revision"`
	Claim                     blueprints.EnvironmentBlueprintStageClaim     `json:"claim"`
	Authored                  *ZoneRemovalAuthoredRevision                  `json:"authored,omitempty"`
	DesiredProjection         projectionrecord.EnvironmentComposeProjection `json:"desired_projection"`
	AppliedProjection         projectionrecord.EnvironmentComposeProjection `json:"applied_projection"`
	CandidateProjection       projectionrecord.EnvironmentComposeProjection `json:"candidate_projection"`
	AffectedServiceIDs        []string                                      `json:"affected_service_ids"`
	Status                    taskjournal.TaskStatus                        `json:"status"`
	CreatedAt                 time.Time                                     `json:"created_at"`
	UpdatedAt                 time.Time                                     `json:"updated_at"`
	TerminalAt                *time.Time                                    `json:"terminal_at,omitempty"`
}

// ZoneRemovalAuthoredRevision binds private bridge cleanup to the visible
// Blueprint parent whose desired input omitted this Zone.
type ZoneRemovalAuthoredRevision struct {
	ParentTaskID     string `json:"parent_task_id"`
	RenderGeneration uint64 `json:"render_generation"`
}

func NewZoneRemovalIntent(
	operationID string,
	taskID string,
	zone etcdstore.Versioned[zonerecord.Record],
	authorities EnvironmentZoneRemovalAuthorities,
	claim blueprints.EnvironmentBlueprintStageClaim,
	candidate projectionrecord.EnvironmentComposeProjection,
	affected []string,
	createdAt time.Time,
) (ZoneRemovalIntent, error) {
	intent := ZoneRemovalIntent{
		OperationID: operationID, ActiveTaskID: taskID, ActiveTaskCreatedAt: createdAt,
		EnvironmentID: zone.Record.EnvironmentID, ZoneID: zone.Record.Desired.ID,
		ZoneName: zone.Record.Desired.Name, ZoneRevision: zone.Revision,
		DesiredHeadRevision:       authorities.Desired.Revision,
		AppliedProjectionRevision: authorities.Applied.Revision,
		Claim:                     blueprints.CloneEnvironmentBlueprintStageClaim(claim),
		DesiredProjection:         projectionrecord.CloneEnvironmentComposeProjection(authorities.Desired.Record),
		AppliedProjection:         projectionrecord.CloneEnvironmentComposeProjection(authorities.Applied.Record),
		CandidateProjection:       projectionrecord.CloneEnvironmentComposeProjection(candidate),
		AffectedServiceIDs:        append([]string(nil), affected...),
		Status:                    taskjournal.TaskStatusPending, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if err := ValidateZoneRemovalIntent(intent); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return CloneZoneRemovalIntent(intent), nil
}

func NewAuthoredZoneRemovalIntent(
	operationID string,
	taskID string,
	zone etcdstore.Versioned[zonerecord.Record],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	expectedHeadRevision int64,
	parentTaskID string,
	renderGeneration uint64,
	candidate projectionrecord.EnvironmentComposeProjection,
	affected []string,
	createdAt time.Time,
) (ZoneRemovalIntent, error) {
	if zone.ReadRevision != projection.ReadRevision || zone.Revision != projection.Revision {
		return ZoneRemovalIntent{}, errs.New(
			errs.KindStateConflict, "authored Zone removal sources are not one fixed revision",
		)
	}
	intent := ZoneRemovalIntent{
		OperationID: operationID, ActiveTaskID: taskID, ActiveTaskCreatedAt: createdAt,
		EnvironmentID: zone.Record.EnvironmentID, ZoneID: zone.Record.Desired.ID,
		ZoneName: zone.Record.Desired.Name, ZoneRevision: zone.Revision,
		DesiredHeadRevision:       expectedHeadRevision,
		AppliedProjectionRevision: projection.Revision,
		Authored: &ZoneRemovalAuthoredRevision{
			ParentTaskID: parentTaskID, RenderGeneration: renderGeneration,
		},
		AppliedProjection:   projectionrecord.CloneEnvironmentComposeProjection(projection.Record),
		CandidateProjection: projectionrecord.CloneEnvironmentComposeProjection(candidate),
		AffectedServiceIDs:  append([]string(nil), affected...),
		Status:              taskjournal.TaskStatusPending,
		CreatedAt:           createdAt,
		UpdatedAt:           createdAt,
	}
	if err := ValidateZoneRemovalIntent(intent); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return CloneZoneRemovalIntent(intent), nil
}

func ZoneRemovalDesiredRevisionID(intent ZoneRemovalIntent) string {
	if intent.Authored != nil {
		return intent.Authored.ParentTaskID
	}
	return intent.Claim.RevisionID
}

func ZoneRemovalIntentKey(operationID string) string { return zoneRemovalIntentPrefix + operationID }

func TransferZoneRemovalIntent(
	intent ZoneRemovalIntent,
	taskID string,
	createdAt time.Time,
) (ZoneRemovalIntent, error) {
	if !taskjournal.IsTerminalTaskStatus(intent.Status) && intent.Status != taskjournal.TaskStatusPending {
		return ZoneRemovalIntent{}, errs.New(errs.KindStateConflict, "Zone removal intent cannot transfer")
	}
	next := CloneZoneRemovalIntent(intent)
	next.ActiveTaskID = taskID
	next.ActiveTaskCreatedAt = createdAt
	next.Status = taskjournal.TaskStatusPending
	next.UpdatedAt = createdAt
	next.TerminalAt = nil
	if err := ValidateZoneRemovalIntent(next); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return next, nil
}

func TerminalZoneRemovalIntent(
	intent ZoneRemovalIntent,
	status taskjournal.TaskStatus,
	at time.Time,
) (ZoneRemovalIntent, error) {
	if intent.Status != taskjournal.TaskStatusPending || !taskjournal.IsTerminalTaskStatus(status) {
		return ZoneRemovalIntent{}, errs.New(errs.KindStateConflict, "Zone removal intent is not pending")
	}
	next := CloneZoneRemovalIntent(intent)
	next.Status = status
	next.UpdatedAt = at
	next.TerminalAt = timePointer(at)
	if err := ValidateZoneRemovalIntent(next); err != nil {
		return ZoneRemovalIntent{}, err
	}
	return next, nil
}

func ValidateZoneRemovalIntent(intent ZoneRemovalIntent) error {
	if ids.Validate(ids.KindOperation, intent.OperationID) != nil ||
		ids.Validate(ids.KindTask, intent.ActiveTaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		ids.Validate(ids.KindNetwork, intent.ZoneID) != nil ||
		intent.ZoneName == "" ||
		intent.ZoneRevision <= 0 ||
		intent.DesiredHeadRevision <= 0 ||
		!validZoneRemovalAppliedProjection(intent) ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		projectionrecord.ValidateEnvironmentComposeProjection(intent.CandidateProjection) != nil ||
		recordcodec.ValidateTimestamp("Zone removal created_at", intent.CreatedAt) != nil ||
		recordcodec.ValidateTimestamp("Zone removal active Task created_at", intent.ActiveTaskCreatedAt) != nil ||
		recordcodec.ValidateTimestamp("Zone removal updated_at", intent.UpdatedAt) != nil ||
		intent.ActiveTaskCreatedAt.Before(intent.CreatedAt) ||
		intent.UpdatedAt.Before(intent.ActiveTaskCreatedAt) {
		return errs.New(errs.KindValidationFailed, "Zone removal intent identity is invalid")
	}
	if intent.Authored == nil {
		if blueprints.ValidateEnvironmentBlueprintStageClaim(intent.Claim) != nil ||
			intent.Claim.EnvironmentID != intent.EnvironmentID ||
			intent.Claim.RevisionID != intent.Claim.TaskID ||
			intent.Claim.BaselineHeadRevision != intent.DesiredHeadRevision ||
			intent.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
			intent.DesiredProjection.EnvironmentID != intent.EnvironmentID ||
			intent.CandidateProjection.RevisionID != intent.Claim.RevisionID ||
			intent.DesiredProjection.RevisionID == intent.CandidateProjection.RevisionID ||
			intent.DesiredProjection.RenderGeneration+1 != intent.CandidateProjection.RenderGeneration ||
			intent.CandidateProjection.RenderGeneration != intent.Claim.RenderGeneration ||
			projectionrecord.ValidateEnvironmentComposeProjection(intent.DesiredProjection) != nil {
			return errs.New(errs.KindValidationFailed, "Zone removal desired revision is invalid")
		}
	} else if intent.Claim.TaskID != "" ||
		ids.Validate(ids.KindTask, intent.Authored.ParentTaskID) != nil ||
		intent.Authored.RenderGeneration == 0 ||
		!SameServiceRemovalProjection(intent.DesiredProjection, projectionrecord.EnvironmentComposeProjection{}) ||
		intent.CandidateProjection.RevisionID != intent.Authored.ParentTaskID ||
		intent.CandidateProjection.RenderGeneration != intent.Authored.RenderGeneration ||
		intent.Authored.RenderGeneration < intent.AppliedProjection.RenderGeneration ||
		intent.Authored.RenderGeneration == intent.AppliedProjection.RenderGeneration &&
			intent.AppliedProjection.RevisionID != intent.Authored.ParentTaskID {
		return errs.New(errs.KindValidationFailed, "Zone removal authored revision is invalid")
	}
	if intent.Status == taskjournal.TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Zone removal intent has terminal time")
		}
	} else if !taskjournal.IsTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
		!intent.TerminalAt.Equal(intent.UpdatedAt) || recordcodec.ValidateTimestamp("Zone removal terminal_at", *intent.TerminalAt) != nil {
		return errs.New(errs.KindValidationFailed, "Zone removal terminal state is invalid")
	}
	source := intent.DesiredProjection
	if intent.Authored != nil {
		source = intent.AppliedProjection
	}
	expected := projectionrecord.CloneEnvironmentComposeProjection(source)
	expected.RevisionID = intent.CandidateProjection.RevisionID
	expected.RenderGeneration = intent.CandidateProjection.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), intent.CandidateProjection.ComposeArtifact...)
	expected.NormalizedCompose = append([]byte(nil), intent.CandidateProjection.NormalizedCompose...)
	expected.DesiredZones = nil
	foundZone := false
	for _, zone := range source.DesiredZones {
		if zone.Desired.ID == intent.ZoneID && zone.Desired.Name == intent.ZoneName {
			if foundZone {
				return CorruptZoneRemovalIntent()
			}
			foundZone = true
			continue
		}
		expected.DesiredZones = append(expected.DesiredZones, zone)
	}
	expected.DesiredServices = append(
		[]servicerecord.EnvironmentServiceProjection(nil),
		source.DesiredServices...)
	affected := make([]string, 0)
	for index, service := range source.DesiredServices {
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

func zoneRemovalProjectionChanged(expected, candidate projectionrecord.EnvironmentComposeProjection) error {
	switch {
	case expected.EnvironmentID != candidate.EnvironmentID || expected.RevisionID != candidate.RevisionID ||
		expected.RenderGeneration != candidate.RenderGeneration:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection identity changed")
	case !SameServiceRemovalBytes(expected.ComposeArtifact, candidate.ComposeArtifact) ||
		!SameServiceRemovalBytes(expected.NormalizedCompose, candidate.NormalizedCompose):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection artifact changed")
	case !SameServiceRemovalDesiredZones(expected.DesiredZones, candidate.DesiredZones):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Zones changed")
	case !SameServiceRemovalDesiredServices(expected.DesiredServices, candidate.DesiredServices):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Services changed")
	default:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection dependencies changed")
	}
}

func sameZoneRemovalProjection(left, right projectionrecord.EnvironmentComposeProjection) bool {
	return SameServiceRemovalProjection(left, right)
}

func EncodeZoneRemovalIntent(intent ZoneRemovalIntent) ([]byte, error) {
	if err := ValidateZoneRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("zone_removal_intent", intent)
}

func DecodeZoneRemovalIntent(value []byte) (ZoneRemovalIntent, error) {
	intent, err := recordcodec.Decode[ZoneRemovalIntent](value, "zone_removal_intent")
	if err != nil || ValidateZoneRemovalIntent(intent) != nil {
		return ZoneRemovalIntent{}, CorruptZoneRemovalIntent()
	}
	return intent, nil
}

func CloneZoneRemovalIntent(intent ZoneRemovalIntent) ZoneRemovalIntent {
	clone := intent
	if intent.Authored != nil {
		authored := *intent.Authored
		clone.Authored = &authored
	}
	clone.Claim = blueprints.CloneEnvironmentBlueprintStageClaim(intent.Claim)
	clone.DesiredProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.DesiredProjection)
	clone.AppliedProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.AppliedProjection)
	clone.CandidateProjection = projectionrecord.CloneEnvironmentComposeProjection(intent.CandidateProjection)
	clone.AffectedServiceIDs = append([]string(nil), intent.AffectedServiceIDs...)
	clone.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return clone
}

func CorruptZoneRemovalIntent() error {
	return errs.New(errs.KindInternal, "Zone removal intent is corrupt")
}
