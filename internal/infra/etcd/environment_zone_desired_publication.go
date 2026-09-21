package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/core"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// EnvironmentZoneDesiredPublication is the complete authority for one direct
// Zone create. The immutable Environment revision remains the only desired
// record; the pool registry is an operational reservation committed beside it.
type EnvironmentZoneDesiredPublication struct {
	Project              etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment          etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	ExpectedHeadRevision int64
	Claim                blueprints.EnvironmentBlueprintStageClaim
	Revision             blueprints.EnvironmentDesiredRevisionIdentity
	Projection           projectionrecord.EnvironmentComposeProjection
	Zone                 zonerecord.Record
	Marker               idempotencyrecord.IdempotencyMarker
}

// PublishEnvironmentZoneDesiredRevisionDirect atomically advances the sole
// desired head, reserves the Zone subnet, and publishes exact replay evidence.
// It deliberately writes no flat Zone primary, name, or owner index.
func (repository *HierarchyRepository) PublishEnvironmentZoneDesiredRevisionDirect(
	ctx context.Context,
	input EnvironmentZoneDesiredPublication,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDirectZoneDesiredPublicationInput(input); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, input.Marker); err != nil ||
		found {
		return existing, err
	}

	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, input.Project, input.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previous, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx, input.Environment.Record.ID, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if (input.ExpectedHeadRevision == 0 && found) ||
		(input.ExpectedHeadRevision > 0 && (!found || previous.Revision != input.ExpectedHeadRevision)) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if err := validateDirectZoneProjection(previous.Record, found, input); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	zones, err := newZoneRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	registry, err := zones.getZonePoolRegistryAtRevision(
		ctx, input.Environment.Record.ID, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	nextRegistry, err := registry.Record.Reserve(input.Environment.Record, input.Zone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	registryValue, err := recordcodec.Encode("zone_pool_registry", nextRegistry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(registryValue)

	publication, err := repository.prepareEnvironmentDirectPublication(
		ctx, input.Claim, input.Revision, input.Projection, input.Marker, input.ExpectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	headReference, err := idempotencyrecord.EncodeTaskReference(input.Revision.RevisionID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(headReference)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)

	conditions := []etcdstore.Condition{
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(input.Revision.EnvironmentID, input.Revision.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(input.Revision.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		{Key: networkreservations.ZonePoolRegistryKey(input.Environment.Record.ID), ModRevision: registry.Revision},
	}
	removalLockIndex := len(conditions)
	conditions = append(conditions, etcdstore.Condition{Key: removalrecord.EnvironmentLockKey(input.Environment.Record.ID)})
	fenceOffset := len(conditions)
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		{Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(input.Revision.EnvironmentID), Value: headReference},
		{Type: etcdstore.MutationPut, Key: networkreservations.ZonePoolRegistryKey(input.Environment.Record.ID), Value: registryValue},
		epochMutation,
	}
	classifier := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "direct Zone publication compare evidence is incomplete")
		}
		if values[removalLockIndex] != nil {
			return errs.New(errs.KindStateConflict, "Environment Volume removal is in progress")
		}
		for index := 0; index < 3; index++ {
			if values[index] == nil {
				return errs.New(errs.KindStateConflict, "Environment sealed staging evidence changed")
			}
		}
		if (input.ExpectedHeadRevision == 0 && values[3] != nil) ||
			(input.ExpectedHeadRevision > 0 && (values[3] == nil || values[3].ModRevision != input.ExpectedHeadRevision)) {
			return errs.New(errs.KindStateConflict, "Environment desired state changed")
		}
		if (registry.Revision == 0 && values[4] != nil) ||
			(registry.Revision > 0 && (values[4] == nil || values[4].ModRevision != registry.Revision)) {
			return errs.New(errs.KindStateConflict, "Zone pool registry changed")
		}
		if conflict := fence.classifyCAS(values[fenceOffset:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "direct Zone desired publication raced")
	}
	plan, err := NewIdempotencyMutationPlan(conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(validateEnvironmentDesiredPublicationBudget); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, input.Marker, plan)
}

func validateDirectZoneDesiredPublicationInput(input EnvironmentZoneDesiredPublication) error {
	if err := hierarchyrecord.ValidateProject(input.Project.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(input.Environment.Record); err != nil {
		return err
	}
	if err := zonerecord.ValidateRecord(input.Zone); err != nil {
		return err
	}
	if input.Project.Revision <= 0 || input.Environment.Revision <= 0 ||
		input.Project.ReadRevision < input.Project.Revision || input.Environment.ReadRevision < input.Environment.Revision ||
		input.Environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		input.Environment.Record.ProjectID != input.Project.Record.ID ||
		input.Zone.EnvironmentID != input.Environment.Record.ID || input.ExpectedHeadRevision <= 0 ||
		input.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		input.Revision.EnvironmentID != input.Environment.Record.ID ||
		input.Revision.RevisionID != input.Claim.RevisionID || input.Claim.TaskID != input.Claim.RevisionID ||
		input.Projection.EnvironmentID != input.Revision.EnvironmentID ||
		input.Projection.RevisionID != input.Revision.RevisionID {
		return errs.New(errs.KindValidationFailed, "direct Zone desired publication identity is invalid")
	}
	if input.Project.Record.Kind == hierarchyrecord.ProjectKindTenant &&
		(input.Zone.Desired.OwnerKind != core.ZoneOwnerEnvironment ||
			input.Zone.Desired.OwnerID != input.Environment.Record.ID) {
		return errs.New(errs.KindValidationFailed, "tenant Project Zone ownership is invalid")
	}
	if input.Project.Record.Kind == hierarchyrecord.ProjectKindBacking &&
		(input.Zone.Desired.OwnerKind != core.ZoneOwnerBackingProject ||
			input.Zone.Desired.OwnerID != input.Project.Record.ID) {
		return errs.New(errs.KindValidationFailed, "backing Project Zone ownership is invalid")
	}
	if input.Marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || input.Marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		input.Marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		input.Marker.Locator.ScopeID != input.Environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "direct Zone desired publication marker is invalid")
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(input.Marker); err != nil {
		return err
	}
	if input.Marker.Locator != input.Claim.Locator ||
		!blueprints.SameBlueprintProtectedIntent(input.Marker.Intent, input.Claim.Intent) ||
		!input.Marker.CreatedAt.Equal(input.Claim.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "direct Zone desired publication marker is invalid")
	}
	return nil
}

func validateDirectZoneProjection(
	previous projectionrecord.EnvironmentComposeProjection,
	hasPrevious bool,
	input EnvironmentZoneDesiredPublication,
) error {
	if !hasPrevious {
		return errs.New(errs.KindStateConflict, "Zone desired revision is missing")
	}
	if err := validateEnvironmentComposeProjectionAdvance(previous, true, input.Projection); err != nil {
		return err
	}
	if len(input.Projection.DesiredZones) != len(previous.DesiredZones)+1 {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate does not add exactly one Zone")
	}

	stripped := projectionrecord.CloneEnvironmentComposeProjection(input.Projection)
	zoneFound := false
	stripped.DesiredZones = stripped.DesiredZones[:0]
	for _, projected := range input.Projection.DesiredZones {
		if projected.EnvironmentID == input.Zone.EnvironmentID && projected.Desired == input.Zone.Desired {
			if zoneFound {
				return errs.New(errs.KindValidationFailed, "direct Zone candidate duplicates the projected Zone")
			}
			zoneFound = true
			continue
		}
		stripped.DesiredZones = append(stripped.DesiredZones, projected)
	}
	if !zoneFound {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate projection is incomplete")
	}
	if err := validateZoneArtifactAddition(previous.ComposeArtifact, input.Projection.ComposeArtifact, input.Zone); err != nil {
		return err
	}

	stripped.RevisionID = previous.RevisionID
	stripped.RenderGeneration = previous.RenderGeneration
	stripped.ComposeArtifact = append([]byte(nil), previous.ComposeArtifact...)
	stripped.NormalizedCompose = append([]byte(nil), previous.NormalizedCompose...)
	if !sameDirectZoneFinalProjection(stripped, previous) {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate changed unrelated desired state")
	}
	return nil
}

func sameDirectZoneFinalProjection(left projectionrecord.EnvironmentComposeProjection, right projectionrecord.EnvironmentComposeProjection) bool {
	return left.EnvironmentID == right.EnvironmentID && left.RevisionID == right.RevisionID &&
		left.RenderGeneration == right.RenderGeneration &&
		environmentchanges.SameServiceRemovalBytes(left.ComposeArtifact, right.ComposeArtifact) &&
		environmentchanges.SameServiceRemovalBytes(left.NormalizedCompose, right.NormalizedCompose) &&
		environmentchanges.SameServiceRemovalBlueprintFiles(left.RuntimeFiles, right.RuntimeFiles) &&
		environmentchanges.SameServiceRemovalServiceExtensions(left.ServiceExtensions, right.ServiceExtensions) &&
		environmentchanges.SameServiceRemovalDesiredZones(left.DesiredZones, right.DesiredZones) &&
		environmentchanges.SameServiceRemovalDesiredServices(left.DesiredServices, right.DesiredServices) &&
		environmentchanges.SameServiceRemovalDesiredRoutes(left.DesiredRoutes, right.DesiredRoutes) &&
		environmentchanges.SameServiceRemovalComparableSlices(left.Volumes, right.Volumes) &&
		environmentchanges.SameServiceRemovalComparableSlices(left.VolumeMounts, right.VolumeMounts) &&
		environmentchanges.SameServiceRemovalComponents(left.Components, right.Components) &&
		environmentchanges.SameServiceRemovalEntries(left.Entries, right.Entries) &&
		environmentchanges.SameServiceRemovalDependencyPlans(left.ServiceDependencyPlans, right.ServiceDependencyPlans)
}

func validateZoneArtifactAddition(currentValue []byte, candidateValue []byte, zone zonerecord.Record) error {
	current := &agentpb.ComposeArtifact{}
	candidate := &agentpb.ComposeArtifact{}
	options := proto.UnmarshalOptions{DiscardUnknown: false}
	if options.Unmarshal(currentValue, current) != nil || options.Unmarshal(candidateValue, candidate) != nil ||
		candidate.GetOwnerKind() != current.GetOwnerKind() || candidate.GetOwnerId() != current.GetOwnerId() ||
		len(candidate.GetNetworks()) != len(current.GetNetworks())+1 {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate artifact is invalid")
	}
	stripped := proto.Clone(candidate).(*agentpb.ComposeArtifact)
	stripped.Networks = stripped.Networks[:0]
	dockerName, err := networkname.New(zone.Desired.ID)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate artifact has an invalid Network id")
	}
	found := false
	for _, network := range candidate.GetNetworks() {
		if network.GetNetworkId() == zone.Desired.ID && network.GetComposeName() == zone.Desired.Name &&
			network.GetDockerName() == dockerName {
			if found {
				return errs.New(errs.KindValidationFailed, "direct Zone candidate artifact duplicates the Zone")
			}
			found = true
			continue
		}
		stripped.Networks = append(stripped.Networks, proto.Clone(network).(*agentpb.ComposeNetwork))
	}
	if !found {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate artifact omits the Zone")
	}
	stripped.ArtifactId = current.GetArtifactId()
	stripped.CanonicalYaml = append([]byte(nil), current.GetCanonicalYaml()...)
	stripped.YamlSha256 = append([]byte(nil), current.GetYamlSha256()...)
	for index := range stripped.Services {
		if index < len(current.Services) {
			stripped.Services[index].ExpectedLabels = cloneDirectZoneLabelPairs(
				current.Services[index].GetExpectedLabels(),
			)
		}
	}
	if !proto.Equal(stripped, current) {
		return errs.New(errs.KindValidationFailed, "direct Zone candidate artifact changed unrelated resources")
	}
	return nil
}

func cloneDirectZoneLabelPairs(values []*agentpb.LabelPair) []*agentpb.LabelPair {
	result := make([]*agentpb.LabelPair, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = proto.Clone(value).(*agentpb.LabelPair)
		}
	}
	return result
}
