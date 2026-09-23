package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	http "net/http"
	strings "strings"
	testing "testing"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

func seedDesiredServiceFixture(
	t *testing.T,
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	desired core.Service,
	backingNetworkID string,
	seed int64,
	includeRuntime bool,
	publishHead bool,
) desiredServiceFixture {
	t.Helper()
	record, err := testservices.NewServiceRecord(environmentID, desired, backingNetworkID)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	revisionID := ids.NewAt(ids.KindTask, testAttachTime, seed)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: desired,
		}},
	}
	canonicalYAML := []byte("services:\n  " + desired.Name + ":\n    image: " + desired.Image + "\n")
	projection.NormalizedCompose = append([]byte(nil), canonicalYAML...)
	digest := sha256.Sum256(canonicalYAML)
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId:          ids.NewAt(ids.KindConfig, testAttachTime, seed+1),
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environmentID,
		ProjectName:         "gp-" + strings.ToLower(environmentID),
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: "/srv/groundplane",
		Services:            []*agentpb.ComposeService{{ServiceId: desired.ID, ComposeName: desired.Name}},
	})
	if err != nil {
		t.Fatalf("marshal Environment Compose artifact: %v", err)
	}
	dependencyDigest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest() error = %v", err)
	}
	intentCiphertext := []byte("desired-service-fixture-intent-" + desired.ID)
	intentDigest := sha256.Sum256(intentCiphertext)
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(revisionID, "task_"),
		EnvironmentID: environmentID, RevisionID: revisionID, TaskID: revisionID,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: http.MethodPost, Route: "/blueprints", Key: "desired-service-fixture-" + desired.ID,
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: intentCiphertext,
		},
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 1,
		ProjectionSchema: 1, CreatedAt: testAttachTime,
	}
	blueprint := testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files: []testblueprints.EnvironmentBlueprintFile{
			{Path: "blueprint.yaml", Content: canonicalYAML},
		}, CreatedAt: testAttachTime,
	}
	streams, err := testblueprints.BuildEnvironmentBlueprintStreams(testblueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &blueprint, Projection: projection, DependencyDigest: dependencyDigest,
	})
	if err != nil {
		t.Fatalf("buildEnvironmentBlueprintStreams() error = %v", err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	rootValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(
		testblueprints.EnvironmentBlueprintSealFromDescriptor(streams.Descriptor),
	)
	if err != nil {
		t.Fatalf("encodeEnvironmentBlueprintSeal() error = %v", err)
	}
	defer clear(rootValue)
	mutations := []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintRootKey(environmentID, revisionID), Value: rootValue,
	}}
	if publishHead {
		headValue, encodeErr := testidempotency.EncodeTaskReference(revisionID)
		if encodeErr != nil {
			t.Fatalf("encodeTaskReference() error = %v", encodeErr)
		}
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headValue,
		})
	} else {
		descriptor := streams.Descriptor
		descriptor.State = testblueprints.EnvironmentBlueprintStageSealed
		descriptor.NextAuditChunk = descriptor.AuditChunks
		descriptor.NextProjectionChunk = descriptor.ProjectionChunks
		descriptorValue, encodeErr := testblueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
		if encodeErr != nil {
			t.Fatalf("encodeEnvironmentBlueprintStageDescriptor() error = %v", encodeErr)
		}
		intentDigest, encodeErr := testblueprints.ProtectedBlueprintIntentDigest(claim.Intent)
		if encodeErr != nil {
			clear(descriptorValue)
			t.Fatalf("protectedBlueprintIntentDigest() error = %v", encodeErr)
		}
		locatorValue, encodeErr := testblueprints.EncodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
		if encodeErr != nil {
			clear(descriptorValue)
			t.Fatalf("encodeEnvironmentBlueprintStageLocator() error = %v", encodeErr)
		}
		locatorKey, _, encodeErr := testblueprints.EnvironmentBlueprintLocatorKey(claim.Locator)
		if encodeErr != nil {
			clear(descriptorValue)
			clear(locatorValue)
			t.Fatalf("environmentBlueprintLocatorKey() error = %v", encodeErr)
		}
		mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue}, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: locatorKey, Value: locatorValue})
	}
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: testblueprints.EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: testblueprints.EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < testblueprints.ChunkCount32(len(family.value)); index++ {
			from := int(index) * testblueprints.EnvironmentBlueprintChunkBytes
			to := min(from+testblueprints.EnvironmentBlueprintChunkBytes, len(family.value))
			data := family.value[from:to]
			chunkValue, encodeErr := testblueprints.EncodeEnvironmentBlueprintChunk(
				testblueprints.EnvironmentBlueprintChunk{
					Family: family.id, Sequence: index, LogicalOffset: uint64(from),
					LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
				},
			)
			if encodeErr != nil {
				t.Fatalf("encodeEnvironmentBlueprintChunk() error = %v", encodeErr)
			}
			mutations = append(mutations, testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testblueprints.EnvironmentBlueprintChunkKeyFor(environmentID, revisionID, family.id, index),
				Value: chunkValue,
			})
		}
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	result, err := store.Transact(ctx, nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed sealed desired projection = %#v, %v", result, err)
	}
	rootRevision := result.Revision
	runtimeRevision := int64(0)
	if includeRuntime {
		runtimeValue, encodeErr := testservices.EncodeServiceRuntimeRecord(testservices.NewServiceRuntimeRecord(record))
		if encodeErr != nil {
			t.Fatalf("encodeServiceRuntimeRecord() error = %v", encodeErr)
		}
		runtimeResult, transactErr := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(desired.ID), Value: runtimeValue,
		}})
		clear(runtimeValue)
		if transactErr != nil || !runtimeResult.Succeeded {
			t.Fatalf("seed Service runtime sidecar = %#v, %v", runtimeResult, transactErr)
		}
		runtimeRevision = runtimeResult.Revision
	}
	joined, err := testservices.ReadJoined(ctx, store, testservices.DesiredSelection{
		Services: projection.DesiredServices, Revision: rootRevision, ReadRevision: max(rootRevision, runtimeRevision),
	}, desired.ID, testblueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		t.Fatalf("read seeded Service: %v", err)
	}
	return desiredServiceFixture{
		Service: joined,
		Projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: projection, Revision: rootRevision, ReadRevision: rootRevision,
		},
		Blueprint: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{
			Record: blueprint, Revision: rootRevision, ReadRevision: rootRevision,
		},
		Claim: claim,
	}
}

func seedAttachScope(t *testing.T, ctx context.Context, store *attachTestStore) AttachCreateScope {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{
		ID: ids.NewAt(ids.KindTenant, testAttachTime, 1), Slug: "acme", Name: "Acme",
	}
	tenantVersion, err := hierarchy.CreateTenant(ctx, tenant)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 2), TenantID: tenant.ID, Slug: "app", Name: "App",
		Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	backingProject, err := hierarchy.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 3), Slug: "postgres", Name: "Postgres",
		Kind: testhierarchy.ProjectKindBacking,
	})
	if err != nil {
		t.Fatalf("CreateProject(backing) error = %v", err)
	}
	environmentRecord, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		project.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 4),
		"production",
		"10.32.0.0/16",
		ids.NewAt(ids.KindTask, testAttachTime, 5),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	backingEnvironmentRecord, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		backingProject.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 6),
		"main",
		"10.33.0.0/16",
		ids.NewAt(ids.KindTask, testAttachTime, 7),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment(backing) error = %v", err)
	}
	backingEnvironmentValue, err := testhierarchy.EncodeEnvironment(backingEnvironmentRecord)
	if err != nil {
		t.Fatalf("encodeEnvironment(backing Environment) error = %v", err)
	}
	defer clear(backingEnvironmentValue)
	backingEnvironmentResult, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(backingEnvironmentRecord.ID), Value: backingEnvironmentValue,
	}})
	if err != nil || !backingEnvironmentResult.Succeeded {
		t.Fatalf("seed backing Environment = %#v, %v", backingEnvironmentResult, err)
	}
	backingEnvironment := testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record: backingEnvironmentRecord, Revision: backingEnvironmentResult.Revision,
		ReadRevision: backingEnvironmentResult.Revision,
	}
	serviceID := ids.NewAt(ids.KindService, testAttachTime, 8)
	serviceFixture := seedDesiredServiceFixture(t, ctx, store, environment.Record.ID, core.Service{
		ID: serviceID, Name: "api", Image: "example/api:1",
	}, "", 10, true, true)
	service := serviceFixture.Service
	backingServiceFixture := seedDesiredServiceFixture(t, ctx, store, backingEnvironment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 9), Name: "postgres", Image: "postgres:16-alpine",
		Adapter: "postgres:16",
	}, ids.NewAt(ids.KindNetwork, testAttachTime, 200), 11, true, true)
	backingService := backingServiceFixture.Service
	environment, err = hierarchy.GetEnvironment(ctx, environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment(after projection) error = %v", err)
	}
	return AttachCreateScope{
		Tenant: tenantVersion, Project: project, Environment: environment,
		DesiredHead: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
			Record: testblueprints.EnvironmentBlueprintHead{
				EnvironmentID: environment.Record.ID,
				RevisionID:    serviceFixture.Projection.Record.RevisionID,
			},
			Revision:     serviceFixture.Projection.Revision,
			ReadRevision: serviceFixture.Projection.ReadRevision,
		},
		ComposeProjection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: serviceFixture.Projection.Record, Revision: serviceFixture.Projection.Revision,
			ReadRevision: serviceFixture.Projection.ReadRevision,
		},
		Services:       []testkeyvalue.Versioned[testservices.ServiceRecord]{service},
		BackingProject: backingProject, BackingEnvironment: backingEnvironment, BackingService: backingService,
	}
}

func testPendingAttach(
	t *testing.T,
	scope AttachCreateScope,
	seed int64,
	name string,
	grants []testkeyvalue.Versioned[testattachments.Record],
) (testattachments.Record, testattachments.EncryptedFacts) {
	t.Helper()
	grantIDs := make([]string, 0, len(grants))
	factSets := []testattachments.FactSetMetadata{{Facts: []testattachments.FactDefinition{
		{Key: "pg16_DATABASE"},
		{Key: "pg16_PASSWORD", Secret: true},
		{Key: "pg16_URL", Secret: true},
	}}}
	for _, grant := range grants {
		grantIDs = append(grantIDs, grant.Record.ID)
		factSets = append(factSets, testattachments.FactSetMetadata{
			GrantAttachID: grant.Record.ID,
			Facts: []testattachments.FactDefinition{
				{Key: "pg16_DATABASE"},
				{Key: "pg16_PASSWORD", Secret: true},
				{Key: "pg16_URL", Secret: true},
			},
		})
	}
	id := ids.NewAt(ids.KindAttach, testAttachTime, seed)
	record, err := testattachments.NewPendingAttachRecord(
		id,
		scope.Environment.Record.ID,
		name,
		scope.BackingProject.Record.ID,
		scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID,
		scope.BackingService.Record.BackingNetworkID,
		scope.Services[0].Record.Desired.ID,
		id,
		grantIDs,
		factSets,
		ids.NewAt(ids.KindTask, testAttachTime, seed+100),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	facts, err := testattachments.NewAttachEncryptedFacts(
		record.ID,
		1,
		"age-x25519",
		"sha256",
		[]byte("encrypted-facts"),
	)
	if err != nil {
		t.Fatalf("NewAttachEncryptedFacts() error = %v", err)
	}
	return record, facts
}

func advanceAttachReady(
	ctx context.Context,
	repository *AttachRepository,
	current testkeyvalue.Versioned[testattachments.Record],
) (testkeyvalue.Versioned[testattachments.Record], error) {
	provisioning, err := testattachments.MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		return testkeyvalue.Versioned[testattachments.Record]{}, err
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		return testkeyvalue.Versioned[testattachments.Record]{}, err
	}
	ready, err := testattachments.CompleteAttachProvisioning(current.Record, current.Record.TaskID, true)
	if err != nil {
		return testkeyvalue.Versioned[testattachments.Record]{}, err
	}
	return repository.ReplaceLifecycle(ctx, current, ready)
}

func attachTestEpochRevision(
	t *testing.T,
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
) int64 {
	t.Helper()
	result, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("read Attach Environment epoch = %#v, %v", result, err)
	}
	return result.Entry.ModRevision
}
