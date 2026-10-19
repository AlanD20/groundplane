package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	networkmodel "github.com/AlanD20/groundplane/internal/core/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// TASK-85 / HTTP-06: an enabled router requires captured Agent work, not a
// metadata-only deletion. Initial router authoring is supplied; acceptance,
// staging, rendering and the stored input are real. No running predecessor or
// container effects are supplied, so this does not qualify traffic withdrawal.
func TestNativeRouteRemovalPublishesOnlyItsEnabledRouter(t *testing.T) {
	f := newNativeRouteFixture(t)
	response, err := f.network.CreateRoute(t.Context(), networkmodel.CreateRouteRequest{
		EnvironmentID: f.base.environmentID, Host: "remove.example.test", Path: "/",
		Exposure: "public", TargetServiceID: f.target.ID, TargetPort: 8080,
	}, "native-router-removal-source")
	if err != nil {
		t.Fatal(err)
	}
	var created api.RouteTaskAccepted
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatal(err)
	}
	f.checkDefinition(t, created.TaskID, created.Route.ID, true)
	componentID, routerID := seedNativeRouteRouter(t, f)
	removed, err := f.network.RemoveRoute(t.Context(), created.Route.ID, "native-router-removal")
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &accepted); err != nil || removed.Status != http.StatusAccepted {
		t.Fatalf("router removal response: %v", err)
	}
	page, err := f.base.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("router removal work: %v", err)
	}
	item := page.Items[0].Item
	if item.Placement.Executor != workexecution.Agent || item.Operation != workexecution.Component ||
		item.TargetID != componentID || item.Placement.HostID != f.base.authorization.Identity().HostID ||
		item.Input == nil || item.AttemptNumber != 0 {
		t.Fatal("enabled-router removal was not captured for its exact host and Component")
	}
	view, err := f.base.queue.ReadWork(t.Context(), accepted.TaskID, item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := componentwork.NewInputRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	input, capture, _, err := inputs.Load(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Candidate.Services) != 1 || input.Candidate.Services[0].ServiceId != routerID ||
		input.ConfigurationAction == nil || input.Predecessor == nil || !input.Predecessor.Absent ||
		capture.Route == nil || !capture.Route.Remove || capture.Route.Record.Desired.ID != created.Route.ID ||
		len(input.Files) != 1 {
		t.Fatal("Route removal captured application work, lost its file activation, or invented a running predecessor")
	}
	projection, found, err := f.base.blueprint.GetEnvironmentComposeProjection(t.Context(), f.base.environmentID)
	if err != nil || !found || projection.Record.RevisionID != accepted.TaskID ||
		len(projection.Record.DesiredRoutes) != 0 || len(projection.Record.DesiredServices) != 1 ||
		projection.Record.DesiredServices[0].Desired.ID != f.target.ID || len(projection.Record.Volumes) != 1 {
		t.Fatalf("Route removal lost its atomic desired publication or unrelated resources: %v", err)
	}
	if _, err := f.network.GetRoute(t.Context(), created.Route.ID); !errors.Is(
		err,
		errs.New(errs.KindRouteNotFound, ""),
	) {
		t.Fatalf("removed Route remained in authored resources: %v", err)
	}
	retained, err := f.base.store.Get(t.Context(), routes.RecordKey(created.Route.ID))
	if err != nil || retained.Entry == nil {
		t.Fatalf("Route execution metadata retired before its Agent withdrawal: %v", err)
	}
	revision := f.base.store.revision
	_, err = f.network.RemoveRoute(t.Context(), created.Route.ID, "native-router-removal")
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || f.base.store.revision != revision {
		t.Fatalf("Agent Route replay lost removal authority or published more work: %v", err)
	}
}

// Supply a new initial router snapshot, without rewriting any earlier revision
// or pretending that a Component Task ran. Use the actual catalog and renderer;
// desired input is sealed by the real staging repository. Only this baseline's
// publication is supplied by the fixture, not the Route mutation under test.
func seedNativeRouteRouter(t *testing.T, f *nativeRouteFixture) (string, string) {
	t.Helper()
	ctx := t.Context()
	response, err := f.network.CreateZone(ctx, networkmodel.CreateZoneRequest{
		EnvironmentID: f.base.environmentID, Name: "frontend", Subnet: "10.40.0.0/28",
	}, "native-router-frontend")
	if err != nil {
		t.Fatal(err)
	}
	var zone api.Zone
	if err := json.Unmarshal(response.Body, &zone); err != nil {
		t.Fatal(err)
	}
	current, found, err := f.base.blueprint.GetEnvironmentComposeProjection(ctx, f.base.environmentID)
	if err != nil || !found {
		t.Fatalf("router baseline: %v", err)
	}
	desired, found, err := f.base.blueprint.GetEnvironmentDesiredInput(ctx, f.base.environmentID)
	if err != nil || !found {
		t.Fatalf("router authoring: %v", err)
	}
	environment, err := f.base.blueprint.GetEnvironment(ctx, f.base.environmentID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := f.base.blueprint.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	componentID, routerID := ids.New(ids.KindComponent), ids.New(ids.KindService)
	instance := core.Component{ID: componentID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: f.base.environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		GeneratedServices: []string{routerID}, PinnedIPv4: "10.40.0.2",
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{zone.ID}}}}
	component, err := components.NewRecord(instance)
	if err != nil {
		t.Fatal(err)
	}
	tunnel := core.Component{ID: ids.New(ids.KindComponent), Owner: core.ComponentOwnerEnvironment,
		OwnerID: f.base.environmentID, Kind: core.ComponentKindEdgeCloudflare}
	disabled, err := components.NewRecord(tunnel)
	if err != nil {
		t.Fatal(err)
	}
	projection := environmentprojection.CloneEnvironmentComposeProjection(current.Record)
	projection.RevisionID, projection.RenderGeneration = ids.New(ids.KindTask), projection.RenderGeneration+1
	projection.Components = []components.Record{disabled, component}
	slices.SortFunc(projection.Components, func(left, right components.Record) int {
		return strings.Compare(string(left.Desired.Kind), string(right.Desired.Kind))
	})
	composeProject, err := composerender.LoadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		t.Fatal(err)
	}
	for index := range projection.DesiredServices {
		service := &projection.DesiredServices[index].Desired
		if service.ID != f.target.ID {
			continue
		}
		service.Zones = []string{"frontend"}
		workload := composeProject.Services[service.Name]
		workload.Networks = map[string]*compose.ServiceNetworkConfig{"frontend": {}}
		composeProject.Services[service.Name] = workload
	}
	projection.NormalizedCompose, err = composeProject.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := componentregistration.EnvironmentCatalog()
	if err != nil {
		t.Fatal(err)
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(current.Record)
	if err != nil {
		t.Fatal(err)
	}
	model := core.Environment{ID: environment.Record.ID, VolumeDir: environment.Record.VolumeDir,
		Components: []core.Component{
			tunnel,
			instance,
		}, Zones: map[string]core.Zone{}, Services: map[string]core.Service{}}
	for _, projected := range projection.DesiredZones {
		model.Zones[projected.Desired.Name] = projected.Desired
	}
	for _, projected := range projection.DesiredServices {
		model.Services[projected.Desired.Name] = projected.Desired
	}
	for _, projected := range projection.DesiredRoutes {
		model.Routes = append(model.Routes, projected.Desired)
	}
	managed, err := composerender.ProjectEnvironmentComponents(composeProject, model, catalog)
	if err != nil {
		t.Fatal(err)
	}
	identities.Services = append(identities.Services, managed.Services...)
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: managed.Project, ArtifactID: ids.New(ids.KindConfig), ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: projection.RenderGeneration,
		AuthorizedVolumeDir: environment.Record.VolumeDir, Identities: identities,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	desired.Record.RevisionID, desired.Record.RenderGeneration = projection.RevisionID, projection.RenderGeneration
	desired.Record.Input.NormalizedCompose = append([]byte(nil), projection.NormalizedCompose...)
	desired.Record.Input.Components = map[string]core.ComponentSpec{"http-router": {
		Implementation: core.ComponentKindIngressCaddy, Enabled: true,
		Settings: core.ComponentCapabilitySettings{ZoneIDs: []string{zone.ID}},
	}}
	document, err := blueprintparser.MarshalAuthoringDocument(blueprintparser.AuthoringDocument{
		Envelope: core.Envelope{Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "volume-check", Project: "volume-check", Environment: "qa"}},
		NetworkPool: environment.Record.NetworkPool, Compose: desired.Record.Input.NormalizedCompose,
		Components: desired.Record.Input.Components, Routes: desired.Record.Input.Routes,
	})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	sealed := []byte("supplied-initial-router-authoring")
	digest := sha256.Sum256(sealed)
	staging, err := desiredrevision.NewRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := staging.ClaimEnvironmentBlueprintStage(ctx, blueprints.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: environment.Record.ID, CandidateRevisionID: projection.RevisionID, CandidateTaskID: projection.RevisionID,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   environment.Record.ID,
			Method:    http.MethodPost,
			Route:     "/environments/{id}/blueprint",
			Key:       "supplied-router-baseline",
		},
		Intent: idempotency.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: sealed},
		BaselineHeadRevision: current.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceApply,
		RenderGeneration: projection.RenderGeneration, ProjectionSchema: blueprints.EnvironmentDesiredInputSchema, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	dependency, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	_, err = staging.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, DesiredInput: desired.Record, DependencyDigest: dependency,
		Blueprint: &blueprints.EnvironmentBlueprintRevision{EnvironmentID: environment.Record.ID,
			RevisionID: projection.RevisionID, RootPath: "groundplane.yml", ComposeSources: []string{"groundplane.yml"},
			Files: []blueprints.EnvironmentBlueprintFile{{Path: "groundplane.yml", Content: document}}, CreatedAt: at},
	})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := environmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	nativeVolumePut(t, f.base.store, components.RecordKey(componentID), component, components.EncodeRecord)
	nativeVolumePut(t, f.base.store, components.RecordKey(tunnel.ID), disabled, components.EncodeRecord)
	nativeVolumePut(
		t,
		f.base.store,
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(environment.Record.ID, projection.RevisionID),
		projection,
		environmentprojection.EncodeEnvironmentComposeProjectionStorage,
	)
	nativeVolumePut(
		t,
		f.base.store,
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environment.Record.ID, projection.RevisionID),
		owned,
		environmentprojection.EncodeEnvironmentOwnedIdentities,
	)
	nativeVolumePut(
		t,
		f.base.store,
		blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
		projection.RevisionID,
		idempotency.EncodeTaskReference,
	)
	return componentID, routerID
}
