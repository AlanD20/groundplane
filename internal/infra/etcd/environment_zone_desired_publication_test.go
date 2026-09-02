package etcd

import (
	"context"
	"crypto/sha256"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: direct Zone create must publish through the sole Environment
// desired head while reserving its subnet and exact replay evidence.
func TestEnvironmentZoneDesiredPublicationCommitsHeadPoolAndReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)

	baseTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 14000)
	baseRevision := environmentBlueprintTestRevision(environment.Record.ID, baseTask, "services: {}\n")
	baseProjection := environmentBlueprintTestProjection(environment.Record.ID, baseTask, 1)
	baseMarker := environmentBlueprintTestMarker(baseTask, environment.Record.ID)
	baseClaim := stageEnvironmentBlueprintForPublicationTest(t, repository, 0, baseRevision, baseProjection, baseMarker)
	baseResult, err := publishEnvironmentBlueprintClaimTest(repository,
		ctx, project, environment, 0, baseClaim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.Record.ID, RevisionID: baseTask.ID},
		baseProjection,
		environmentBlueprintTestZoneChanges(t, repository, baseProjection),
		environmentBlueprintTestServiceChanges(t, repository, baseProjection),
		environmentBlueprintTestRouteChanges(t, repository, baseProjection),
		ReleaseGroupBlueprintPreparedMutation{}, ComponentTaskPreparation{}, BlueprintAttachTaskPreparation{},
		baseTask, baseMarker,
	)
	if err != nil {
		t.Fatalf("publish base desired revision: %v", err)
	}
	if outcome, _, conflict, classifyErr := baseResult.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("base publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	current, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentComposeProjection(base) = %#v/%v/%v", current, found, err)
	}

	at := time.Date(2026, time.August, 31, 22, 0, 0, 0, time.UTC)
	revisionID := ids.NewAt(ids.KindTask, at, 1)
	zoneID := ids.NewAt(ids.KindNetwork, at, 2)
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: zoneID, Name: "frontend", Subnet: "10.40.20.0/24", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := cloneEnvironmentComposeProjection(current.Record)
	candidate.RevisionID = revisionID
	candidate.RenderGeneration++
	candidate.DesiredZones = append(candidate.DesiredZones, EnvironmentZoneProjection{
		EnvironmentID: environment.Record.ID, Desired: zone.Desired,
	})
	candidate = zonePublicationTestArtifact(t, candidate, zone)

	locator := IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPost, Route: "/zones", Key: "zone-publication-key",
	}
	intent := validEnvironmentBlueprintProtectedIntentForTest("zone-create")
	claim := stageZoneDesiredPublicationTest(t, store, EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(revisionID, "task_"), EnvironmentID: environment.Record.ID,
		RevisionID: revisionID, TaskID: revisionID, Locator: locator, Intent: intent,
		BaselineHeadRevision: current.Revision, SourceKind: EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: EnvironmentDesiredProjectionSchema,
		CreatedAt: at,
	}, current.Record.RevisionID, candidate, zone)
	response := IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{"id":"` + zoneID + `"}`),
	}
	marker, err := NewCompletedDirectIdempotencyMarker(locator, intent, response, at)
	if err != nil {
		t.Fatal(err)
	}
	input := EnvironmentZoneDesiredPublication{
		Project: project, Environment: environment, ExpectedHeadRevision: current.Revision,
		Claim:      claim,
		Revision:   EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.Record.ID, RevisionID: revisionID},
		Projection: candidate, Zone: zone, Marker: marker,
	}
	result, err := repository.PublishEnvironmentZoneDesiredRevisionDirect(ctx, input)
	if err != nil {
		t.Fatalf("PublishEnvironmentZoneDesiredRevisionDirect() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Zone publication = %v/%v/%v", outcome, conflict, classifyErr)
	}

	published, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || published.Record.RevisionID != revisionID ||
		len(published.Record.DesiredZones) != len(candidate.DesiredZones) {
		t.Fatalf("published projection = %#v/%v/%v", published, found, err)
	}
	publishedZone := false
	for _, desired := range published.Record.DesiredZones {
		if desired.Desired.ID == zoneID {
			publishedZone = true
			break
		}
	}
	if !publishedZone {
		t.Fatalf("published projection is missing Zone %q: %#v", zoneID, published.Record.DesiredZones)
	}
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := zones.ListZoneSubnetReservationsAtRevision(ctx, environment.Record.ID, published.ReadRevision)
	expectedReservations := make([]string, 0, len(candidate.DesiredZones))
	for _, desired := range candidate.DesiredZones {
		expectedReservations = append(expectedReservations, desired.Desired.Subnet)
	}
	slices.Sort(reservations)
	slices.Sort(expectedReservations)
	if err != nil || !slices.Equal(reservations, expectedReservations) {
		t.Fatalf("Zone reservations = %#v/%v", reservations, err)
	}
	headRevision := published.Revision
	replayed, err := repository.PublishEnvironmentZoneDesiredRevisionDirect(ctx, input)
	if err != nil {
		t.Fatalf("PublishEnvironmentZoneDesiredRevisionDirect(replay) error = %v", err)
	}
	outcome, existing, conflict, classifyErr := replayed.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		existing.Response.Status != response.Status || string(existing.Response.Body) != string(response.Body) {
		t.Fatalf("Zone replay = %v/%#v/%v/%v", outcome, existing, conflict, classifyErr)
	}
	after, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || after.Revision != headRevision {
		t.Fatalf("head changed on replay = %#v/%v/%v, want revision %d", after, found, err, headRevision)
	}
}

func zonePublicationTestArtifact(
	t *testing.T,
	projection EnvironmentComposeProjection,
	zone ZoneRecord,
) EnvironmentComposeProjection {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	canonical := []byte("networks:\n  frontend:\n    internal: true\n    ipam:\n      config:\n        - subnet: 10.40.20.0/24\nservices: {}\n")
	digest := sha256.Sum256(canonical)
	artifact.ArtifactId = ids.NewAt(ids.KindConfig, time.Date(2026, time.August, 31, 22, 0, 0, 0, time.UTC), 3)
	artifact.CanonicalYaml = canonical
	artifact.YamlSha256 = digest[:]
	artifact.Networks = append(artifact.Networks, &agentpb.ComposeNetwork{
		NetworkId: zone.Desired.ID, ComposeName: zone.Desired.Name, DockerName: "gp_net_" + zone.Desired.ID,
	})
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact = value
	projection.NormalizedCompose = append([]byte(nil), canonical...)
	return projection
}

func stageZoneDesiredPublicationTest(
	t *testing.T,
	store *memoryHierarchyStore,
	claim EnvironmentBlueprintStageClaim,
	baseRevisionID string,
	projection EnvironmentComposeProjection,
	zone ZoneRecord,
) EnvironmentBlueprintStageClaim {
	t.Helper()
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := buildEnvironmentBlueprintStreams(EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &EnvironmentDesiredMutationAudit{Zone: &EnvironmentZoneMutationAudit{
			Action: EnvironmentZoneMutationCreate, BaseRevisionID: baseRevisionID, ZoneID: zone.Desired.ID,
			Request: &EnvironmentZoneMutationRequest{
				EnvironmentID: zone.EnvironmentID, Name: zone.Desired.Name,
				Subnet: zone.Desired.Subnet, Internal: zone.Desired.Internal,
			},
		}},
		Projection: projection, DependencyDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	descriptor.State = EnvironmentBlueprintStageSealed
	descriptor.NextAuditChunk = descriptor.AuditChunks
	descriptor.NextProjectionChunk = descriptor.ProjectionChunks
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(descriptorValue)
	intentDigest, err := protectedBlueprintIntentDigest(claim.Intent)
	if err != nil {
		t.Fatal(err)
	}
	locatorValue, err := encodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(locatorValue)
	rootValue, err := encodeEnvironmentBlueprintSeal(environmentBlueprintSealFromDescriptor(descriptor))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(rootValue)
	locatorKey, _, err := environmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue},
		{Type: MutationPut, Key: environmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID), Value: rootValue},
		{Type: MutationPut, Key: locatorKey, Value: locatorValue},
	}
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < chunkCount32(len(family.value)); index++ {
			from := int(index) * EnvironmentBlueprintChunkBytes
			to := from + EnvironmentBlueprintChunkBytes
			if to > len(family.value) {
				to = len(family.value)
			}
			data := family.value[from:to]
			value, encodeErr := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
				Family: family.id, Sequence: index, LogicalOffset: uint64(from),
				LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
			})
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			mutations = append(mutations, Mutation{
				Type:  MutationPut,
				Key:   environmentBlueprintChunkKeyFor(claim.EnvironmentID, claim.RevisionID, family.id, index),
				Value: value,
			})
			defer clear(value)
		}
	}
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("stage Zone mutation = %#v/%v", result, err)
	}
	return claim
}
