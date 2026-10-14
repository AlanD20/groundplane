package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintZoneRemovalPublication struct {
	intent environmentchanges.ZoneRemovalIntent
}

func NewBlueprintZoneRemovalPublication(
	task TaskRecord, intent environmentchanges.ZoneRemovalIntent,
) (BlueprintZoneRemovalPublication, error) {
	if intent.Authored == nil || validateZoneRemovalTaskOwner(task, intent) != nil {
		return BlueprintZoneRemovalPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Zone removal publication is invalid",
		)
	}
	return BlueprintZoneRemovalPublication{intent: environmentchanges.CloneZoneRemovalIntent(intent)}, nil
}

func (publication BlueprintZoneRemovalPublication) IsZero() bool {
	return publication.intent.OperationID == ""
}

func (publication *BlueprintZoneRemovalPublication) Clear() {
	if publication == nil {
		return
	}
	clear(publication.intent.AppliedProjection.ComposeArtifact)
	clear(publication.intent.AppliedProjection.NormalizedCompose)
	clear(publication.intent.CandidateProjection.ComposeArtifact)
	clear(publication.intent.CandidateProjection.NormalizedCompose)
	*publication = BlueprintZoneRemovalPublication{}
}

func (publication BlueprintZoneRemovalPublication) prepare(
	ctx context.Context, repository *TaskRepository, task TaskRecord,
	unit blueprintunits.Unit, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	intent := publication.intent
	if repository == nil || publication.IsZero() || intent.Authored == nil ||
		unit.Target.Kind != ids.KindNetwork || !unit.Removal || unit.Target.ID != intent.ZoneID ||
		validateZoneRemovalTaskOwner(task, intent) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Zone removal publication changed")
	}
	checkCondition, err := edgechecks.RequireZoneIdle(ctx, repository.store, intent.ZoneID, revision)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		networkreservations.ZonePoolRegistryKey(intent.EnvironmentID),
		networkreservations.ComponentAddressRegistryKey(intent.ZoneID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), intent.ZoneID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		environmentchanges.ZoneRemovalIntentKey(intent.OperationID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[4] != nil || read.Values[5] != nil || read.Values[6] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Blueprint Zone removal authority is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0].ModRevision != intent.AppliedProjectionRevision ||
		!environmentchanges.ZoneRemovalAppliedEvidenceMatches(intent, read.Values[0]) {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone applied projection changed")
	}
	if read.Values[1].ModRevision != intent.DesiredHeadRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone removal head changed")
	}
	headTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[1].Value)
	if err != nil || headTaskID != intent.Authored.ParentTaskID {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone removal parent changed")
	}
	zone, err := projectedZoneRemovalTarget(intent, revision)
	if err != nil {
		return nil, nil, err
	}
	if err := validateAuthoredZoneRemovalHierarchy(ctx, repository, zone, revision); err != nil {
		return nil, nil, err
	}
	// The retained applied projection is recovery input, not current Component
	// ownership after preceding omission units have acknowledged their teardown.
	currentComponents, err := readEnvironmentBlueprintRuntimeComponents(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if err != nil {
		return nil, nil, err
	}
	componentConditions := make([]keyvalue.Condition, 0, len(currentComponents)*2)
	for _, current := range currentComponents {
		if selectedByEnabledComponent([]components.Record{current.Component.Record}, intent.ZoneID) {
			return nil, nil, errs.New(errs.KindResourceInUse,
				"Zone is selected by an enabled Component; change or disable the Component first")
		}
		componentConditions = append(componentConditions,
			keyvalue.Condition{Key: components.RecordKey(current.Component.Record.Desired.ID),
				ModRevision: current.Component.Revision},
			keyvalue.Condition{Key: componentplanning.RuntimeKey(current.Component.Record.Desired.ID),
				ModRevision: current.Runtime.Revision})
	}
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](read.Values[2].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[intent.ZoneID] != zone.Desired.Subnet {
		return nil, nil, errs.New(errs.KindInternal, "Zone subnet reservation is inconsistent")
	}
	if read.Values[3] != nil {
		addresses, decodeErr := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
			read.Values[3].Value, "component_address_registry",
		)
		if decodeErr != nil || networkreservations.ValidateComponentAddressRegistry(zone, addresses) != nil ||
			len(addresses.Reservations) != 0 {
			return nil, nil, errs.New(
				errs.KindResourceInUse,
				"Zone is selected by an enabled Caddy Component; move or disable Caddy first",
			)
		}
	}
	if zone.Desired.OwnerKind == core.ZoneOwnerBackingProject {
		attaches, err := attachrecord.NewReader(repository.store).ListAttachesByBackingNetworkAtRevision(
			ctx, zone.Desired.OwnerID, intent.ZoneID, revision,
		)
		if err != nil {
			return nil, nil, err
		}
		for _, attached := range attaches {
			if attached.Record.EnvironmentID != intent.EnvironmentID {
				return nil, nil, errs.New(
					errs.KindResourceInUse,
					"Backing Zone has active external Attaches; use the impact-approved Remove action",
				)
			}
		}
	} else if zone.Desired.OwnerKind != core.ZoneOwnerEnvironment || zone.Desired.OwnerID != intent.EnvironmentID {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Zone ownership changed")
	}
	dnsConditions, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(
		ctx, "", intent.ZoneID, revision,
	)
	if err != nil {
		return nil, nil, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return nil, nil, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetZone, TargetID: intent.ZoneID,
		TargetRevision: intent.ZoneRevision, TaskID: task.ID,
		Phase: deletionrecord.DeletionPhaseHostEffects, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	})
	if err != nil {
		clear(epoch.Value)
		return nil, nil, err
	}
	intentValue, err := environmentchanges.EncodeZoneRemovalIntent(intent)
	if err != nil {
		clear(epoch.Value)
		clear(tombstoneValue)
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision},
		{Key: keys[3], ModRevision: keyvalue.RevisionOf(read.Values[3])},
		{Key: keys[4]}, {Key: keys[5]}, {Key: keys[6]},
	}
	conditions = append(conditions, dnsConditions...)
	conditions = append(conditions, componentConditions...)
	conditions = append(conditions, checkCondition)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[4], Value: tombstoneValue},
		{Type: keyvalue.MutationPut, Key: keys[6], Value: intentValue},
		{Type: keyvalue.MutationPut, Key: keys[5], Value: []byte(task.ID)},
		epoch,
	}
	return conditions, mutations, nil
}

func validateAuthoredZoneRemovalHierarchy(
	ctx context.Context,
	repository *TaskRepository,
	zone zonerecord.Record,
	revision int64,
) error {
	environmentKey := hierarchyrecord.EnvironmentKey(zone.EnvironmentID)
	environmentRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{environmentKey}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if environmentRead == nil || environmentRead.ReadRevision != revision ||
		len(environmentRead.Values) != 1 || environmentRead.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint Zone Environment ownership is unavailable")
	}
	defer keyvalue.ClearValues(environmentRead.Values)
	environment, err := hierarchyrecord.DecodeEnvironment(environmentRead.Values[0].Value)
	if err != nil || environment.ID != zone.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Zone Environment ownership changed")
	}
	projectRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(environment.ProjectID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if projectRead == nil || projectRead.ReadRevision != revision || len(projectRead.Values) != 1 ||
		projectRead.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint Zone Project ownership is unavailable")
	}
	defer keyvalue.ClearValues(projectRead.Values)
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return errs.New(errs.KindStateConflict, "Blueprint Zone Project ownership changed")
	}
	ordinary := zone.Desired.OwnerKind == core.ZoneOwnerEnvironment &&
		zone.Desired.OwnerID == environment.ID && project.Kind == hierarchyrecord.ProjectKindTenant
	backing := zone.Desired.OwnerKind == core.ZoneOwnerBackingProject &&
		zone.Desired.OwnerID == project.ID && project.Kind == hierarchyrecord.ProjectKindBacking && project.TenantID == ""
	if !ordinary && !backing {
		return errs.New(errs.KindStateConflict, "Blueprint Zone hierarchy ownership changed")
	}
	return nil
}
