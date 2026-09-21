package etcd

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestServiceRepositoryReadsAndPagesDesiredProjectionWithSidecar(t *testing.T) {
	// Rationale: Service reads select immutable desired fields from the current
	// Environment projection and join mutable runtime state from its sidecar.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := serviceRepositoryTestHierarchy(t)
	desired := []core.Service{
		serviceRepositoryTestDesired(810, "api"),
		serviceRepositoryTestDesired(811, "worker"),
	}
	projection := serviceRecordTestProjection(t, environment.Record.ID, desired...)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	apiRuntimeRevision := seedServiceRepositoryTestRuntime(t, store, testservices.ServiceRuntimeRecord{
		EnvironmentID: environment.Record.ID, ServiceID: desired[0].ID,
		Runtime: core.ServiceRuntime{ServiceID: desired[0].ID, RuntimeIntent: core.ServiceRuntimeIntentStopped},
	})

	stored, err := repository.GetService(ctx, desired[0].ID)
	if err != nil || stored.Record.Desired.Name != "api" ||
		stored.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped ||
		testservices.ServiceRuntimeRevision(stored) != apiRuntimeRevision {
		t.Fatalf("GetService() = %#v, %v", stored, err)
	}
	first, err := repository.ListServices(ctx, environment.Record.ID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" || first.Items[0].Record.Desired.Name != "api" {
		t.Fatalf("ListServices(first) = %#v, %v", first, err)
	}
	second, err := repository.ListServices(
		ctx,
		environment.Record.ID, testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision ||
		second.Items[0].Record.Desired.Name != "worker" ||
		second.Items[0].Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("ListServices(second) = %#v, %v", second, err)
	}
}

func TestServiceRepositoryScopesLookupsToDesiredProjection(t *testing.T) {
	// Rationale: the current Environment projection is the scoped Service
	// index; names and stable ids outside it must not become visible reads.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := serviceRepositoryTestHierarchy(t)
	desired := serviceRepositoryTestDesired(820, "api")
	projection := serviceRecordTestProjection(t, environment.Record.ID, desired)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)

	byName, err := repository.GetServiceByName(ctx, environment.Record.ID, "api")
	if err != nil || byName.Record.Desired.ID != desired.ID {
		t.Fatalf("GetServiceByName(api) = %#v, %v", byName, err)
	}
	if _, err := repository.GetServiceByName(ctx, environment.Record.ID, "missing"); !isKind(
		err,
		errs.KindServiceNotFound,
	) {
		t.Fatalf("GetServiceByName(missing) error = %v", err)
	}
	otherID := ids.NewAt(ids.KindService, serviceRecordTestTime(), 821)
	if _, err := repository.GetService(ctx, otherID); !isKind(err, errs.KindServiceNotFound) {
		t.Fatalf("GetService(outside projection) error = %v", err)
	}
}

func TestServiceRepositoryHidesComponentGeneratedServicesFromOrdinaryReads(t *testing.T) {
	// Rationale: Component-generated Services remain render inputs in the immutable
	// desired revision but must not leak into ordinary Service list or lookup paths.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := serviceRepositoryTestHierarchy(t)
	authoredAPI := serviceRepositoryTestDesired(830, "api")
	generatedRouter := serviceRepositoryTestDesired(831, "caddy")
	authoredWorker := serviceRepositoryTestDesired(832, "worker")
	component, err := testcomponents.NewRecord(core.Component{
		ID:    ids.NewAt(ids.KindComponent, serviceRecordTestTime(), 833),
		Owner: core.ComponentOwnerEnvironment, OwnerID: environment.Record.ID,
		Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 834)},
		}},
		GeneratedServices: []string{generatedRouter.ID}, PinnedIPv4: "10.34.0.2",
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID:    ids.NewAt(ids.KindComponent, serviceRecordTestTime(), 835),
		Owner: core.ComponentOwnerEnvironment, OwnerID: environment.Record.ID,
		Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	projection := serviceRecordTestProjection(t, environment.Record.ID, authoredAPI, generatedRouter, authoredWorker)
	projection.Components = []testcomponents.Record{component, tunnel}
	projection = withTestEnvironmentComposeArtifact(projection)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)

	first, err := repository.ListServices(ctx, environment.Record.ID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.Desired.ID == generatedRouter.ID ||
		first.NextCursor == "" {
		t.Fatalf("ListServices(first) = %#v, %v", first, err)
	}
	second, err := repository.ListServices(
		ctx,
		environment.Record.ID,
		testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	listed := map[string]bool{first.Items[0].Record.Desired.ID: true}
	if len(second.Items) == 1 {
		listed[second.Items[0].Record.Desired.ID] = true
	}
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.Desired.ID == generatedRouter.ID ||
		second.NextCursor != "" || !listed[authoredAPI.ID] || !listed[authoredWorker.ID] || len(listed) != 2 {
		t.Fatalf("ListServices(second) = %#v, %v", second, err)
	}
	if _, err := repository.GetService(ctx, generatedRouter.ID); !isKind(err, errs.KindServiceNotFound) {
		t.Fatalf("GetService(generated) error = %v", err)
	}
	if _, err := repository.GetServiceByName(
		ctx, environment.Record.ID, generatedRouter.Name,
	); !isKind(err, errs.KindServiceNotFound) {
		t.Fatalf("GetServiceByName(generated) error = %v", err)
	}
	revision, err := repository.GetServiceRevision(
		ctx,
		environment.Record.ID,
		projection.RevisionID,
		generatedRouter.ID,
	)
	if err != nil || revision.Record.Desired.ID != generatedRouter.ID {
		t.Fatalf("GetServiceRevision(generated) = %#v, %v", revision, err)
	}
}

func serviceRepositoryTestHierarchy(
	t *testing.T,
) (*ServiceRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	repository, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 800), Slug: "acme", Name: "Acme"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 801), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentRecord, err := testhierarchy.NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		hierarchyTestID(ids.KindEnvironment, 802),
		"production",
		"10.34.0.0/16",
		hierarchyTestID(ids.KindTask, 803),
		serviceRecordTestTime(),
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	return repository, store, environment, project
}

func serviceRepositoryTestDesired(offset int64, name string) core.Service {
	return core.Service{
		ID: ids.NewAt(ids.KindService, serviceRecordTestTime(), offset), Name: name,
		Image: "app:latest", Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack,
	}
}

func seedServiceRepositoryTestRuntime(
	t *testing.T,
	store *memoryHierarchyStore,
	record testservices.ServiceRuntimeRecord,
) int64 {
	t.Helper()
	value, err := testservices.EncodeServiceRuntimeRecord(record)
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(record.ServiceID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Service runtime sidecar = %#v, %v", result, err)
	}
	return result.Revision
}

func seedServiceRepositoryTestDesiredProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) {
	t.Helper()
	createdAt := serviceRecordTestTime()
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(projection.RevisionID, "task_"),
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID, TaskID: projection.RevisionID,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: projection.EnvironmentID,
			Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: "service-repository-seed-0001",
		},
		Intent:     validEnvironmentBlueprintProtectedIntentForTest("service-repository-seed"),
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: projection.RenderGeneration,
		ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: createdAt,
	}
	blueprint := testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files: []testblueprints.EnvironmentBlueprintFile{
			{Path: "blueprint.yaml", Content: []byte("services: {}\n")},
		},
		CreatedAt: createdAt,
	}
	digest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest() error = %v", err)
	}
	streams, err := testblueprints.BuildEnvironmentBlueprintStreams(testblueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &blueprint, Projection: projection, DependencyDigest: digest,
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
	headValue, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	defer clear(headValue)
	mutations := []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintRootKey(projection.EnvironmentID, projection.RevisionID),
			Value: rootValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID),
			Value: headValue,
		},
	}
	for index := uint32(0); index < streams.Descriptor.ProjectionChunks; index++ {
		from := int(index) * testblueprints.EnvironmentBlueprintChunkBytes
		to := from + testblueprints.EnvironmentBlueprintChunkBytes
		if to > len(streams.Projection) {
			to = len(streams.Projection)
		}
		data := streams.Projection[from:to]
		chunkValue, encodeErr := testblueprints.EncodeEnvironmentBlueprintChunk(
			testblueprints.EnvironmentBlueprintChunk{
				Family: testblueprints.EnvironmentBlueprintChunkProjection, Sequence: index,
				LogicalOffset: uint64(from), LogicalLength: uint32(len(data)),
				Digest: sha256.Sum256(data), Data: data,
			},
		)
		if encodeErr != nil {
			t.Fatalf("encodeEnvironmentBlueprintChunk() error = %v", encodeErr)
		}
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut,
			Key: testblueprints.EnvironmentBlueprintChunkKeyFor(
				projection.EnvironmentID,
				projection.RevisionID,
				testblueprints.EnvironmentBlueprintChunkProjection,
				index,
			),
			Value: chunkValue,
		})
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed desired Service projection = %#v, %v", result, err)
	}
}
