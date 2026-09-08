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
	apiRuntimeRevision := seedServiceRepositoryTestRuntime(t, store, ServiceRuntimeRecord{
		EnvironmentID: environment.Record.ID, ServiceID: desired[0].ID,
		Runtime: core.ServiceRuntime{ServiceID: desired[0].ID, RuntimeIntent: core.ServiceRuntimeIntentStopped},
	})

	stored, err := repository.GetService(ctx, desired[0].ID)
	if err != nil || stored.Record.Desired.Name != "api" ||
		stored.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped ||
		stored.Record.runtimeRevision != apiRuntimeRevision {
		t.Fatalf("GetService() = %#v, %v", stored, err)
	}
	first, err := repository.ListServices(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" || first.Items[0].Record.Desired.Name != "api" {
		t.Fatalf("ListServices(first) = %#v, %v", first, err)
	}
	second, err := repository.ListServices(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 1, Cursor: first.NextCursor},
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
	component, err := NewComponentRecord(core.Component{
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
	tunnel, err := NewComponentRecord(core.Component{
		ID:    ids.NewAt(ids.KindComponent, serviceRecordTestTime(), 835),
		Owner: core.ComponentOwnerEnvironment, OwnerID: environment.Record.ID,
		Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	projection := serviceRecordTestProjection(t, environment.Record.ID, authoredAPI, generatedRouter, authoredWorker)
	projection.Components = []ComponentRecord{component, tunnel}
	projection = withTestEnvironmentComposeArtifact(projection)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)

	first, err := repository.ListServices(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.Desired.ID == generatedRouter.ID ||
		first.NextCursor == "" {
		t.Fatalf("ListServices(first) = %#v, %v", first, err)
	}
	second, err := repository.ListServices(ctx, environment.Record.ID, PageRequest{Limit: 1, Cursor: first.NextCursor})
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
) (*ServiceRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
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
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 800), Slug: "acme", Name: "Acme"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 801), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentRecord, err := NewProvisioningEnvironment(
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
	record ServiceRuntimeRecord,
) int64 {
	t.Helper()
	value, err := encodeServiceRuntimeRecord(record)
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: serviceRuntimeKey(record.ServiceID), Value: value,
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
	projection EnvironmentComposeProjection,
) {
	t.Helper()
	createdAt := serviceRecordTestTime()
	claim := EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(projection.RevisionID, "task_"),
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID, TaskID: projection.RevisionID,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment, ScopeID: projection.EnvironmentID,
			Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: "service-repository-seed-0001",
		},
		Intent:     validEnvironmentBlueprintProtectedIntentForTest("service-repository-seed"),
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: projection.RenderGeneration,
		ProjectionSchema: EnvironmentDesiredProjectionSchema, CreatedAt: createdAt,
	}
	blueprint := EnvironmentBlueprintRevision{
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files:     []EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: []byte("services: {}\n")}},
		CreatedAt: createdAt,
	}
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest() error = %v", err)
	}
	streams, err := buildEnvironmentBlueprintStreams(EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &blueprint, Projection: projection, DependencyDigest: digest,
	})
	if err != nil {
		t.Fatalf("buildEnvironmentBlueprintStreams() error = %v", err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	rootValue, err := encodeEnvironmentBlueprintSeal(environmentBlueprintSealFromDescriptor(streams.Descriptor))
	if err != nil {
		t.Fatalf("encodeEnvironmentBlueprintSeal() error = %v", err)
	}
	defer clear(rootValue)
	headValue, err := encodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	defer clear(headValue)
	mutations := []Mutation{
		{
			Type:  MutationPut,
			Key:   environmentBlueprintRootKey(projection.EnvironmentID, projection.RevisionID),
			Value: rootValue,
		},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(projection.EnvironmentID), Value: headValue},
	}
	for index := uint32(0); index < streams.Descriptor.ProjectionChunks; index++ {
		from := int(index) * EnvironmentBlueprintChunkBytes
		to := from + EnvironmentBlueprintChunkBytes
		if to > len(streams.Projection) {
			to = len(streams.Projection)
		}
		data := streams.Projection[from:to]
		chunkValue, encodeErr := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
			Family: EnvironmentBlueprintChunkProjection, Sequence: index,
			LogicalOffset: uint64(from), LogicalLength: uint32(len(data)),
			Digest: sha256.Sum256(data), Data: data,
		})
		if encodeErr != nil {
			t.Fatalf("encodeEnvironmentBlueprintChunk() error = %v", encodeErr)
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut,
			Key: environmentBlueprintChunkKeyFor(
				projection.EnvironmentID, projection.RevisionID, EnvironmentBlueprintChunkProjection, index,
			),
			Value: chunkValue,
		})
	}
	defer clearMutationValues(mutations)
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed desired Service projection = %#v, %v", result, err)
	}
}
