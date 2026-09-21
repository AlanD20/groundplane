package etcd

import (
	context "context"
	sha256 "crypto/sha256"
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
		RenderGeneration: projection.RenderGeneration, ProjectionSchema: 1,
		CreatedAt: revision.CreatedAt,
	}
	streams, err := testblueprints.BuildEnvironmentBlueprintStreams(testblueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &revision, Projection: projection, DependencyDigest: dependencyDigest,
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
