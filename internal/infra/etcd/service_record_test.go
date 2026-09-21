package etcd

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func TestServiceProjectionJoinDefaultsMissingRuntimeSidecarToRunning(t *testing.T) {
	// Rationale: a desired Service is authoritative even before its mutable
	// runtime sidecar is first written, and the public view must default to
	// running without inventing a durable sidecar revision.
	t.Parallel()
	ctx := context.Background()
	service := serviceRecordTestDesired()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1)
	projection := serviceRecordTestProjection(t, environmentID, service)
	store := newMemoryHierarchyStore()
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: "service-record-test-revision", Value: []byte("revision"),
	}}); err != nil {
		t.Fatalf("seed revision: %v", err)
	}
	read, err := store.Get(ctx, "service-record-test-revision")
	if err != nil || read.Entry == nil {
		t.Fatalf("read revision: %#v, %v", read, err)
	}

	joined, err := joinServiceFixture(
		ctx,
		store,
		testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: projection, Revision: read.Entry.ModRevision, ReadRevision: read.ReadRevision,
		},
		service.ID,
		testblueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("joinServiceFixture() error = %v", err)
	}
	if joined.Record.Desired.ID != service.ID || joined.Record.Desired.Name != service.Name ||
		joined.Record.Runtime.ServiceID != service.ID ||
		joined.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning ||
		testservices.ServiceRuntimeRevision(joined) != 0 {
		t.Fatalf(
			"joined Service = %#v, runtime revision = %d",
			joined.Record,
			testservices.ServiceRuntimeRevision(joined),
		)
	}
}

func TestServiceProjectionJoinUsesMutableRuntimeSidecar(t *testing.T) {
	// Rationale: desired fields come only from the selected projection while
	// runtime intent comes only from the independently mutable sidecar.
	t.Parallel()
	ctx := context.Background()
	service := serviceRecordTestDesired()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1)
	projection := serviceRecordTestProjection(t, environmentID, service)
	store := newMemoryHierarchyStore()
	runtime := testservices.ServiceRuntimeRecord{
		EnvironmentID: projection.EnvironmentID, ServiceID: service.ID,
		Runtime: core.ServiceRuntime{ServiceID: service.ID, RuntimeIntent: core.ServiceRuntimeIntentStopped},
	}
	runtimeValue, err := testservices.EncodeServiceRuntimeRecord(runtime)
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	runtimeWrite, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(service.ID), Value: runtimeValue,
	}})
	clear(runtimeValue)
	if err != nil || !runtimeWrite.Succeeded {
		t.Fatalf("seed Service runtime sidecar = %#v, %v", runtimeWrite, err)
	}
	joined, err := joinServiceFixture(
		ctx,
		store,
		testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: projection, Revision: runtimeWrite.Revision, ReadRevision: runtimeWrite.Revision,
		},
		service.ID,
		testblueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("joinServiceFixture() error = %v", err)
	}
	if joined.Record.Desired.Image != service.Image ||
		joined.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped ||
		testservices.ServiceRuntimeRevision(joined) != runtimeWrite.Revision {
		t.Fatalf(
			"joined Service = %#v, runtime revision = %d",
			joined.Record,
			testservices.ServiceRuntimeRevision(joined),
		)
	}
}

func TestServiceProjectionAndRuntimeEnvelopesRoundTripStrictly(t *testing.T) {
	// Rationale: desired and runtime authorities have separate strict schemas;
	// corruption must not be accepted as a compatibility record.
	service := serviceRecordTestDesired()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1)
	projection := serviceRecordTestProjection(t, environmentID, service)
	encodedProjection, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decodedProjection, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(encodedProjection)
	if err != nil || decodedProjection.EnvironmentID != projection.EnvironmentID ||
		decodedProjection.DesiredServices[0].Desired.ID != projection.DesiredServices[0].Desired.ID {
		t.Fatalf("projection round trip = %#v, %v", decodedProjection, err)
	}
	for name, corrupt := range map[string][]byte{
		"unknown projection field":   bytes.Replace(encodedProjection, []byte(`"desired_services":`), []byte(`"unknown":0,"desired_services":`), 1),
		"duplicate projection field": bytes.Replace(encodedProjection, []byte(`"desired_services":`), []byte(`"desired_services":[],"desired_services":`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(corrupt); err == nil {
				t.Fatal("decodeEnvironmentComposeProjection() accepted corrupt projection")
			}
		})
	}

	runtime := testservices.ServiceRuntimeRecord{
		EnvironmentID: projection.EnvironmentID, ServiceID: projection.DesiredServices[0].Desired.ID,
		Runtime: core.ServiceRuntime{
			ServiceID: projection.DesiredServices[0].Desired.ID, RuntimeIntent: core.ServiceRuntimeIntentStopped,
		},
	}
	encodedRuntime, err := testservices.EncodeServiceRuntimeRecord(runtime)
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	decodedRuntime, err := testservices.DecodeServiceRuntimeRecord(encodedRuntime)
	if err != nil || decodedRuntime != runtime {
		t.Fatalf("runtime round trip = %#v, %v", decodedRuntime, err)
	}
	for name, corrupt := range map[string][]byte{
		"unknown runtime field":   bytes.Replace(encodedRuntime, []byte(`"runtime":`), []byte(`"unknown":0,"runtime":`), 1),
		"duplicate runtime field": bytes.Replace(encodedRuntime, []byte(`"runtime":`), []byte(`"runtime":{},"runtime":`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := testservices.DecodeServiceRuntimeRecord(corrupt); err == nil {
				t.Fatal("decodeServiceRuntimeRecord() accepted corrupt sidecar")
			}
		})
	}
}

func TestBackingServiceProjectionRequiresStableNetworkBinding(t *testing.T) {
	// Rationale: an adapter-backed desired Service carries its stable backing
	// network in the projection, never in a flat Service primary record.
	t.Parallel()
	desired := serviceRecordTestDesired()
	desired.Adapter = "postgres:16"
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1)
	projection := serviceRecordTestProjection(t, environmentID, desired)
	projection.DesiredServices[0].BackingNetworkID = ""
	if err := testenvironmentprojection.ValidateEnvironmentComposeProjection(projection); err == nil {
		t.Fatal("projection accepted adapter-backed Service without a backing network")
	}
	networkID := ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 7)
	projection.DesiredServices[0].BackingNetworkID = networkID
	projection = withTestEnvironmentComposeArtifact(projection)
	if err := testenvironmentprojection.ValidateEnvironmentComposeProjection(projection); err != nil {
		t.Fatalf("validateEnvironmentComposeProjection() error = %v", err)
	}
	projection.DesiredServices[0].Desired.Image = "postgres:16.1-alpine"
	if projection.DesiredServices[0].BackingNetworkID != networkID {
		t.Fatalf("desired projection changed backing network to %q", projection.DesiredServices[0].BackingNetworkID)
	}
}

func TestServiceProjectionKeysUseStableEnvironmentAndRuntimeAuthorities(t *testing.T) {
	// Rationale: the desired projection and mutable runtime sidecar are the
	// only durable Service authorities at the current head.
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 4)
	serviceID := serviceRecordTestDesired().ID
	if got := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID); got != "/v1/records/environment-compose-projections/"+environmentID {
		t.Fatalf("environmentComposeProjectionKey() = %q", got)
	}
	if got := testservices.ServiceRuntimeKey(serviceID); got != "/v1/records/service-runtimes/"+serviceID {
		t.Fatalf("serviceRuntimeKey() = %q", got)
	}
}

func serviceRecordTestProjection(
	t *testing.T,
	environmentID string,
	services ...core.Service,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, serviceRecordTestTime(), 2), RenderGeneration: 1,
	}
	for _, service := range services {
		projection.DesiredServices = append(projection.DesiredServices, testservices.EnvironmentServiceProjection{
			EnvironmentID: environmentID, Desired: service,
		})
	}
	sort.Slice(projection.DesiredServices, func(left, right int) bool {
		return projection.DesiredServices[left].Desired.Name < projection.DesiredServices[right].Desired.Name
	})
	return withTestEnvironmentComposeArtifact(projection)
}

func serviceRecordTestDesired() core.Service {
	return core.Service{
		ID: ids.NewAt(ids.KindService, serviceRecordTestTime(), 5), Name: "api", Image: "app:latest",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack,
	}
}

func serviceRecordTestTime() time.Time {
	return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
}
