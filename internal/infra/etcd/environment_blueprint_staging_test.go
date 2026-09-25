package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	strings "strings"
	testing "testing"
)

func stageEnvironmentBlueprintForPublicationTest(
	t *testing.T,
	repository *HierarchyRepository,
	expectedHeadRevision int64,
	revision testblueprints.EnvironmentBlueprintRevision,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	marker testidempotency.IdempotencyMarker,
) testblueprints.EnvironmentBlueprintStageClaim {
	t.Helper()
	dependencyDigest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(marker.TaskID, "task_"),
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		TaskID: marker.TaskID, Locator: marker.Locator, Intent: marker.Intent,
		BaselineHeadRevision: expectedHeadRevision, SourceKind: testblueprints.EnvironmentBlueprintSourceApply,
		RenderGeneration: projection.RenderGeneration, ProjectionSchema: testblueprints.EnvironmentDesiredInputSchema,
		CreatedAt: revision.CreatedAt,
	}
	streams, err := testblueprints.BuildEnvironmentBlueprintStreams(testblueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &revision, DesiredInput: desiredInputForProjectionFixture(projection), DependencyDigest: dependencyDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	descriptor.State = testblueprints.EnvironmentBlueprintStageSealed
	descriptor.NextAuditChunk = descriptor.AuditChunks
	descriptor.NextProjectionChunk = descriptor.ProjectionChunks
	descriptorValue, err := testblueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(descriptorValue)
	intentDigest, err := testblueprints.ProtectedBlueprintIntentDigest(claim.Intent)
	if err != nil {
		t.Fatal(err)
	}
	locatorValue, err := testblueprints.EncodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(locatorValue)
	rootValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(
		testblueprints.EnvironmentBlueprintSealFromDescriptor(descriptor),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(rootValue)
	mutations := []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID),
			Value: descriptorValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID),
			Value: rootValue,
		},
	}
	locatorKey, _, err := testblueprints.EnvironmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations,
		testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: locatorKey, Value: locatorValue},
	)
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: testblueprints.EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: testblueprints.EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < testblueprints.ChunkCount32(len(family.value)); index++ {
			from := int(index) * testblueprints.EnvironmentBlueprintChunkBytes
			to := from + testblueprints.EnvironmentBlueprintChunkBytes
			if to > len(family.value) {
				to = len(family.value)
			}
			data := family.value[from:to]
			chunkValue, encodeErr := testblueprints.EncodeEnvironmentBlueprintChunk(
				testblueprints.EnvironmentBlueprintChunk{
					Family: family.id, Sequence: index,
					LogicalOffset: uint64(from), LogicalLength: uint32(len(data)),
					Digest: sha256.Sum256(data), Data: data,
				},
			)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut,
				Key: testblueprints.EnvironmentBlueprintChunkKeyFor(
					claim.EnvironmentID,
					claim.RevisionID,
					family.id,
					index,
				),
				Value: chunkValue})
		}
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	result, err := repository.store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("persist sealed desired revision = %#v, %v", result, err)
	}
	return claim
}

func desiredInputForProjectionFixture(
	projection testenvironmentprojection.EnvironmentComposeProjection,
) testenvironmentprojection.EnvironmentDesiredInput {
	normalized := projection.NormalizedCompose
	if len(normalized) == 0 {
		normalized = []byte("services: {}\n")
	}
	serviceNames := make(map[string]string, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		serviceNames[service.Desired.ID] = service.Desired.Name
	}
	routes := make([]core.RouteSpec, len(projection.DesiredRoutes))
	for index, route := range projection.DesiredRoutes {
		routes[index] = core.RouteSpec{
			Hostname: route.Desired.Host, Path: route.Desired.Path,
			Target:     serviceNames[route.Desired.TargetServiceID],
			TargetPort: route.Desired.TargetPort, Exposure: route.Desired.Exposure,
		}
	}
	return testenvironmentprojection.EnvironmentDesiredInput{
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID,
		RenderGeneration: projection.RenderGeneration,
		Input: core.BlueprintDesiredInput{
			NormalizedCompose: append([]byte(nil), normalized...),
			RuntimeFiles:      projection.RuntimeFiles,
			ServiceExtensions: projection.ServiceExtensions,
			NetworkPool:       "10.40.0.0/16",
			Routes:            routes,
		},
	}
}

// Manual fixture heads represent an already materialized desired revision.
// Staging alone intentionally does not publish these two runtime authorities.
func seedEffectiveBlueprintFixture(
	t *testing.T,
	store interface {
		Transact(context.Context, []testkeyvalue.Condition, []testkeyvalue.Mutation) (testkeyvalue.TransactionResult, error)
	},
	projection testenvironmentprojection.EnvironmentComposeProjection,
) {
	t.Helper()
	effective, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(effective)
	identities, err := testenvironmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := testenvironmentprojection.EncodeEnvironmentOwnedIdentities(identities)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(owned)
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut,
			Key: testblueprints.EnvironmentBlueprintEffectiveProjectionKey(
				projection.EnvironmentID,
				projection.RevisionID,
			),
			Value: effective},
		{Type: testkeyvalue.MutationPut,
			Key: testblueprints.EnvironmentBlueprintOwnedIdentitiesKey(
				projection.EnvironmentID,
				projection.RevisionID,
			),
			Value: owned},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed effective Blueprint fixture = %#v, %v", result, err)
	}
}
