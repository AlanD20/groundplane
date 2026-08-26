//go:build c07_network_l2

package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TestC07RealEtcdConcurrentReplayAndRestart is the C07 persistence L2 gate.
// It uses a caller-owned isolated prefix and never touches another run's keys.
func TestC07RealEtcdConcurrentReplayAndRestart(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("GROUNDPLANE_ETCD_ENDPOINT"))
	prefix := strings.TrimSpace(os.Getenv("GROUNDPLANE_C07_ETCD_PREFIX"))
	if endpoint == "" || prefix == "" {
		t.Fatal("GROUNDPLANE_ETCD_ENDPOINT and GROUNDPLANE_C07_ETCD_PREFIX are required")
	}
	if !strings.HasPrefix(prefix, "/groundplane-c07-acceptance/") || !strings.HasSuffix(prefix, "/") ||
		len(prefix) < len("/groundplane-c07-acceptance/run-/") {
		t.Fatalf("GROUNDPLANE_C07_ETCD_PREFIX %q is not an isolated C07 acceptance prefix", prefix)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := etcd.New(ctx, []string{endpoint}, prefix)
	if err != nil {
		t.Fatalf("connect real etcd: %v", err)
	}
	if err := store.Health(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("real etcd health: %v", err)
	}
	cleanup := func(current etcd.Store) {
		t.Helper()
		_, cleanupErr := current.Transact(ctx, nil, []etcd.Mutation{{
			Type: etcd.MutationDelete, Key: "/v1/", Prefix: true,
		}})
		if cleanupErr != nil {
			t.Errorf("clean isolated C07 prefix: %v", cleanupErr)
		}
		if closeErr := current.Close(); closeErr != nil {
			t.Errorf("close real etcd store: %v", closeErr)
		}
	}

	repository, _, services, environment, project := acceptanceRepository(t, store)
	targetRecord, err := etcd.NewServiceRecord(environment.Record.ID, core.Service{
		ID: ids.New(ids.KindService), Name: "api", Image: "example.invalid/app:1",
		Strategy: core.StrategyRecreate, Expose: []string{"8080/tcp"},
	}, "")
	if err != nil {
		cleanup(store)
		t.Fatalf("construct target Service: %v", err)
	}
	target, err := services.CreateService(ctx, environment, project, targetRecord)
	if err != nil {
		cleanup(store)
		t.Fatalf("create target Service: %v", err)
	}

	zoneRecord, err := etcd.NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.New(ids.KindNetwork), Name: "frontend", Subnet: "10.200.10.0/24", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		cleanup(store)
		t.Fatalf("construct Zone: %v", err)
	}
	zoneMarker := acceptanceMarker(t,
		environment.Record.ID, http.MethodPost, "/zones", "c07-zone-create-0001", zoneRecord.Desired.ID,
	)
	concurrentExactReplay(t, func() error {
		result, callErr := repository.CreateZoneIdempotent(ctx, environment, project, zoneRecord, zoneMarker)
		return acceptanceIdempotencyOutcome(result, callErr)
	})

	firstOverlap := acceptanceZoneRecord(t, environment.Record.ID, "overlap-a", "10.200.20.0/24")
	secondOverlap := acceptanceZoneRecord(t, environment.Record.ID, "overlap-b", "10.200.20.128/25")
	overlapErrors := concurrentCalls(
		func() error {
			result, callErr := repository.CreateZoneIdempotent(
				ctx, environment, project, firstOverlap,
				acceptanceMarker(t,
					environment.Record.ID, http.MethodPost, "/zones", "c07-zone-overlap-001", firstOverlap.Desired.ID,
				),
			)
			return acceptanceIdempotencyOutcome(result, callErr)
		},
		func() error {
			result, callErr := repository.CreateZoneIdempotent(
				ctx, environment, project, secondOverlap,
				acceptanceMarker(t,
					environment.Record.ID, http.MethodPost, "/zones", "c07-zone-overlap-002", secondOverlap.Desired.ID,
				),
			)
			return acceptanceIdempotencyOutcome(result, callErr)
		},
	)
	assertOneStateConflict(t, overlapErrors)

	routeRecord, err := etcd.NewRouteRecord(environment.Record.ID, core.Route{
		ID: ids.New(ids.KindRoute), Host: "app.example.com", Path: "/api/*", Exposure: "public",
		TargetServiceID: target.Record.Desired.ID, TargetPort: 8080,
	})
	if err != nil {
		cleanup(store)
		t.Fatalf("construct Route: %v", err)
	}
	routeMarker := acceptanceMarker(t,
		environment.Record.ID, http.MethodPost, "/routes", "c07-route-create-001", routeRecord.Desired.ID,
	)
	concurrentExactReplay(t, func() error {
		result, callErr := repository.CreateRouteIdempotent(
			ctx, environment, project, target, routeRecord, routeMarker,
		)
		return acceptanceIdempotencyOutcome(result, callErr)
	})

	zones, err := repository.ListZones(ctx, environment.Record.ID, etcd.PageRequest{Limit: 20})
	if err != nil || len(zones.Items) != 2 {
		cleanup(store)
		t.Fatalf("ListZones() items/error = %d/%v, want primary plus one overlap winner", len(zones.Items), err)
	}
	routes, err := repository.ListRoutes(ctx, environment.Record.ID, etcd.PageRequest{Limit: 20})
	if err != nil || len(routes.Items) != 1 || routes.Items[0].Record != routeRecord {
		cleanup(store)
		t.Fatalf("ListRoutes() = %#v, %v", routes, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store before restart proof: %v", err)
	}

	restarted, err := etcd.New(ctx, []string{endpoint}, prefix)
	if err != nil {
		t.Fatalf("reopen real etcd store: %v", err)
	}
	defer cleanup(restarted)
	restartedRepository, restartedHierarchy, _, _, _ := acceptanceRepositoryForExisting(t, restarted)
	storedZone, err := restartedRepository.GetZone(ctx, zoneRecord.Desired.ID)
	if err != nil || storedZone.Record != zoneRecord {
		t.Fatalf("GetZone() after restart = %#v, %v", storedZone, err)
	}
	storedRoute, err := restartedRepository.GetRoute(ctx, routeRecord.Desired.ID)
	if err != nil || storedRoute.Record != routeRecord {
		t.Fatalf("GetRoute() after restart = %#v, %v", storedRoute, err)
	}
	if _, err := restartedHierarchy.GetEnvironment(ctx, environment.Record.ID); err != nil {
		t.Fatalf("GetEnvironment() after restart: %v", err)
	}
}

type acceptanceFactResolver struct{}

func (acceptanceFactResolver) ResolveRemovalDatabase(
	context.Context,
	etcd.Versioned[etcd.AttachRecord],
	func(string) error,
) error {
	return errors.New("acceptance fact resolver is intentionally unused")
}

func acceptanceRepository(
	t *testing.T,
	store etcd.Store,
) (*Repository, *etcd.HierarchyRepository, *etcd.ServiceRepository, etcd.Versioned[etcd.EnvironmentRecord],
	etcd.Versioned[etcd.ProjectRecord]) {
	t.Helper()
	repository, hierarchy, services, _, _ := acceptanceRepositoryForExisting(t, store)
	ctx := context.Background()
	tenant := etcd.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "c07-tenant", Name: "C07 Tenant"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("create acceptance Tenant: %v", err)
	}
	projectRecord := etcd.ProjectRecord{
		ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "c07-project", Name: "C07 Project", Kind: etcd.ProjectKindTenant,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("create acceptance Project: %v", err)
	}
	environmentRecord, err := etcd.NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot, projectRecord, ids.New(ids.KindEnvironment), "c07-environment",
		"10.200.0.0/16", ids.New(ids.KindTask), time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("construct acceptance Environment: %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("create acceptance Environment: %v", err)
	}
	return repository, hierarchy, services, environment, project
}

func acceptanceRepositoryForExisting(
	t *testing.T,
	store etcd.Store,
) (*Repository, *etcd.HierarchyRepository, *etcd.ServiceRepository, *etcd.ZoneRepository, *etcd.RouteRepository) {
	t.Helper()
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("construct Hierarchy repository: %v", err)
	}
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatalf("construct Service repository: %v", err)
	}
	zones, err := etcd.NewZoneRepository(store)
	if err != nil {
		t.Fatalf("construct Zone repository: %v", err)
	}
	routes, err := etcd.NewRouteRepository(store)
	if err != nil {
		t.Fatalf("construct Route repository: %v", err)
	}
	attaches, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatalf("construct Attach repository: %v", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatalf("construct Task repository: %v", err)
	}
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("construct Idempotency repository: %v", err)
	}
	repository, err := NewRepository(
		hierarchy, services, zones, routes, attaches, tasks, idempotency, acceptanceFactResolver{},
	)
	if err != nil {
		t.Fatalf("construct Network adapter: %v", err)
	}
	return repository, hierarchy, services, zones, routes
}

func acceptanceZoneRecord(t *testing.T, environmentID string, name string, subnet string) etcd.ZoneRecord {
	t.Helper()
	record, err := etcd.NewZoneRecord(environmentID, core.Zone{
		ID: ids.New(ids.KindNetwork), Name: name, Subnet: subnet, Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	})
	if err != nil {
		t.Fatalf("construct acceptance Zone: %v", err)
	}
	return record
}

func acceptanceMarker(
	t *testing.T,
	environmentID string,
	method string,
	route string,
	key string,
	resourceID string,
) etcd.IdempotencyMarker {
	t.Helper()
	ciphertext := []byte("c07-acceptance-protected-intent:" + key)
	digest := sha256.Sum256(ciphertext)
	now := time.Now().UTC()
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(
		etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: method, Route: route, Key: key,
		},
		etcd.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		etcd.IdempotencyResponse{
			Status: http.StatusCreated, ContentKind: "application/json",
			Body: []byte(fmt.Sprintf(`{"id":%q}`, resourceID)),
		},
		now,
	)
	if err != nil {
		t.Fatalf("construct acceptance idempotency marker: %v", err)
	}
	return marker
}

func concurrentExactReplay(t *testing.T, call func() error) {
	t.Helper()
	for index, err := range concurrentCalls(call, call) {
		if err != nil {
			t.Fatalf("concurrent exact replay call %d error = %v", index, err)
		}
	}
}

func acceptanceIdempotencyOutcome(result etcd.IdempotencyTransactionResult, err error) error {
	if err != nil {
		return err
	}
	_, marker, conflict, classifyErr := result.Classify()
	clear(marker.Intent.Ciphertext)
	clear(marker.Response.Body)
	if classifyErr != nil {
		return classifyErr
	}
	return conflict
}

func concurrentCalls(calls ...func() error) []error {
	start := make(chan struct{})
	errorsByCall := make([]error, len(calls))
	var wait sync.WaitGroup
	wait.Add(len(calls))
	for index, call := range calls {
		go func(callIndex int, currentCall func() error) {
			defer wait.Done()
			<-start
			errorsByCall[callIndex] = currentCall()
		}(index, call)
	}
	close(start)
	wait.Wait()
	return errorsByCall
}

func assertOneStateConflict(t *testing.T, results []error) {
	t.Helper()
	successes := 0
	conflicts := 0
	for _, err := range results {
		if err == nil {
			successes++
			continue
		}
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
			conflicts++
			continue
		}
		t.Fatalf("overlap race returned unexpected error: %v", err)
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("overlap race successes/conflicts = %d/%d, want 1/1", successes, conflicts)
	}
}
