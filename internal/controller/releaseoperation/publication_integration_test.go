package releaseoperation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// TASK-100: execute native direct publication through image preflight, frozen
// render/configuration capture, staging and atomic Task/work acceptance. The CAS
// store supplies persistence only, not the publisher's result. This does not run
// Agent effects, prepare the later complete graph or settle applied runtime.
func TestPublishFirstBlueGreenCommitsTaskAndCandidateAuthority(t *testing.T) {
	testPublishFirstBlueGreen(t, false, false)
}

// Rationale: an Entry edit can seal an Attach network into its projection before
// a later Detach removes that membership. A new Release must capture the current
// Attach set, not revive membership from the earlier desired artifact.
func TestPublishReleaseDropsDetachedNetworkFromEarlierProjection(t *testing.T) {
	testPublishFirstBlueGreen(t, true, false)
}

// Rationale: SVC-06/H31 requires explicit first Deploy to retain profile-disabled
// definitions through Attach projection, with either no joins or replacement joins.
func TestPublishFirstBlueGreenPreservesProfileDisabledService(t *testing.T) {
	for _, attached := range []bool{false, true} {
		t.Run(fmt.Sprintf("attached=%t", attached), func(t *testing.T) {
			testPublishFirstBlueGreen(t, attached, true)
		})
	}
}

func testPublishFirstBlueGreen(t *testing.T, detachedNetwork, profileDisabled bool) {
	t.Helper()
	ctx := context.Background()
	store := &directPublicationStore{values: make(map[string]*testkeyvalue.KeyValue)}
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := hierarchy.CreateTenant(
		ctx, testhierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "tenant", Name: "Tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	project, err := hierarchy.CreateProject(
		ctx, testhierarchy.ProjectRecord{
			ID:       ids.New(ids.KindProject),
			TenantID: tenant.Record.ID,
			Slug:     "project",
			Name:     "Project",
			Kind:     testhierarchy.ProjectKindTenant,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentID := ids.New(ids.KindEnvironment)
	volumeDir := "/var/lib/groundplane/vol/" + tenant.Record.ID + "/" + project.Record.ID + "/" + environmentID
	environment, err := hierarchy.CreateEnvironment(
		ctx, testhierarchy.EnvironmentRecord{
			ID:                environmentID,
			ProjectID:         project.Record.ID,
			Name:              "proof",
			NetworkPool:       "10.96.0.0/16",
			VolumeDir:         volumeDir,
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			CreateTaskID:      ids.New(ids.KindTask),
			CreatedAt:         time.Now().UTC(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	serviceID, networkID := ids.New(ids.KindService), ids.New(ids.KindNetwork)
	projectInput := &composetypes.Project{Services: composetypes.Services{"api": {
		Name:        "api",
		Image:       "api:first",
		Expose:      []string{"8080"},
		Networks:    map[string]*composetypes.ServiceNetworkConfig{"backend": {}},
		HealthCheck: &composetypes.HealthCheckConfig{Test: composetypes.HealthCheckTest{"CMD", "true"}},
	}}, Networks: composetypes.Networks{"backend": {Driver: "bridge", Ipam: composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: "10.96.10.0/24"}}}}}}
	if profileDisabled {
		service := projectInput.Services["api"]
		service.Profiles = []string{"configured"}
		projectInput.Services["api"] = service
	}
	normalized, err := testcomposerender.MarshalNormalizedEnvironmentProject(projectInput)
	if err != nil {
		t.Fatal(err)
	}
	var externalNetworks []testcomposeidentity.Resource
	if detachedNetwork {
		const oldNetwork = "gp_attach_net_01arz3ndektsv4rrffq69g5fav"
		api := projectInput.Services["api"]
		api.Networks[oldNetwork] = &composetypes.ServiceNetworkConfig{}
		projectInput.Services["api"] = api
		projectInput.Networks[oldNetwork] = composetypes.NetworkConfig{External: true}
		externalNetworks = []testcomposeidentity.Resource{{
			ID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: oldNetwork,
		}}
	}
	artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
		Project:             projectInput,
		ArtifactID:          ids.New(ids.KindConfig),
		ProjectOwnerKind:    testcomposerender.ComposeProjectOwnerTenant,
		TenantID:            tenant.Record.ID,
		ProjectID:           project.Record.ID,
		EnvironmentID:       environmentID,
		PlanID:              ids.New(ids.KindPlan),
		RenderGeneration:    1,
		AuthorizedVolumeDir: volumeDir,
		Identities: testcomposeidentity.Snapshot{
			Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}},
			Networks: []testcomposeidentity.Resource{{ID: networkID, Name: "backend"}},
		},
		ExternalNetworks: externalNetworks,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifactBytes, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	desired := core.Service{
		ID:          serviceID,
		Name:        "api",
		Image:       "api:first",
		Strategy:    core.StrategyBlueGreen,
		Healthcheck: core.Healthcheck{Test: []string{"CMD", "true"}},
		Replicas:    1,
		Expose:      []string{"8080"},
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:     environmentID,
		RevisionID:        ids.New(ids.KindTask),
		RenderGeneration:  1,
		ComposeArtifact:   artifactBytes,
		NormalizedCompose: normalized,
		Components:        releaseOperationProxyProjection(environmentID).Components,
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: environmentID, Desired: desired},
		},
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{
				EnvironmentID: environmentID,
				Desired: core.Zone{
					ID:        networkID,
					Name:      "backend",
					Subnet:    "10.96.10.0/24",
					OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID:   environmentID,
				},
			},
		},
	}
	if detachedNetwork {
		seedPublicationReadyAttach(t, store, environmentID, serviceID)
	}
	head, err := json.Marshal(struct {
		Schema   int    `json:"schema"`
		RecordID string `json:"record_id"`
	}{1, projection.RevisionID})
	if err != nil {
		t.Fatal(err)
	}
	headRevision, err := store.Put(ctx, "/v1/records/environment-blueprints/"+environmentID+"/current", head)
	if err != nil {
		t.Fatal(err)
	}
	selectedHead, found, err := hierarchy.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil || !found || selectedHead.Record.RevisionID != projection.RevisionID {
		t.Fatalf("configured-only desired head is invalid: %v", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := testtaskplanning.NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyImage := componentsdk.OCIImage{
		Repository:  "docker.io/library/caddy",
		IndexDigest: strings.Repeat("e", 64),
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat("b", 64),
				ConfigDigest: strings.Repeat("c", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat("d", 64),
				ConfigDigest: strings.Repeat("f", 64),
			},
		},
	}
	envoyImage := proxyImage
	envoyImage.Repository = "docker.io/envoyproxy/envoy"
	if err := plans.EnableServiceProxyImages(proxyImage, envoyImage); err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(releaseOperationTestCipher{}, releaseOperationTestCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	version, digest, err := idempotentintent.Canonicalize(
		ctx,
		idempotentintent.CanonicalIntentV1{
			Method: http.MethodPost,
			Route:  "/services/{id}/deploy",
			Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
			Path:   []idempotentintent.PathBinding{{Name: "id", Value: serviceID}},
			Query:  idempotentintent.Object(),
			Body:   idempotentintent.JSONBody(idempotentintent.Object()),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		t.Fatal(err)
	}
	bound, err := hostbindings.PrepareLocalEnrollment(ctx, store, ids.New(ids.KindAgent))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := store.Transact(ctx, bound.Conditions, bound.Mutations); err != nil || !result.Succeeded {
		t.Fatalf("native host enrollment: %v", err)
	}
	epoch := store.values["/v1/runtime/environment-mutation-epochs/"+environmentID]
	if epoch == nil {
		t.Fatal("fixture missing real environment epoch")
	}
	scope := testreleasequeries.ReleasePlanningScope{
		ReadRevision: store.revision,
		Tenant:       tenant,
		Project:      project,
		Environment:  environment,
		Compose: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record:       projection,
			Revision:     headRevision,
			ReadRevision: store.revision,
		},
		EnvironmentEpochRevision: epoch.ModRevision,
		EnvironmentEpochValue:    epoch.Value,
	}
	scripts, err := etcd.NewScriptRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	producer := &Service{
		ledger:      ledger,
		plans:       plans,
		scripts:     scripts,
		preparation: &testtaskplanning.ScriptRunnerPreparationService{},
		agents:      publicationAgent{},
		images:      directPublicationImages{},
		coordinator: coordinator,
		now:         time.Now,
		timeout:     time.Hour,
	}
	planning := testreleasequeries.ReleasePlanningService{
		Service: testkeyvalue.Versioned[testservices.ServiceRecord]{
			Record:   testservices.ServiceRecord{EnvironmentID: environmentID, Desired: desired},
			Revision: headRevision,
		},
	}
	candidate, err := producer.deployCandidate(
		ctx,
		scope,
		planning,
		"first", string(domain.StrategyBlueGreen), domain.OnFailureSwitchBack, "",
	)
	if err != nil {
		t.Fatalf("actual first blue-green candidate: %v", err)
	}
	response, err := producer.publishServiceWork(ctx, scope, candidate, testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: serviceDeployRoute, Key: "first-blue-green",
	}, durable, protected)
	if err != nil {
		t.Fatalf("native first blue-green publisher: %v", err)
	}
	if response.Status != http.StatusAccepted {
		t.Fatalf("publication status=%d", response.Status)
	}
	var accepted struct {
		TaskID string `json:"task_id"`
	}
	if json.Unmarshal(response.Body, &accepted) != nil || accepted.TaskID == "" {
		t.Fatal("missing accepted Task")
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	stored, revision, err := queue.ReadTask(ctx, accepted.TaskID, 0)
	if err != nil || stored.Task.Action != workexecution.ActionDeploy || stored.Task.TargetID != serviceID ||
		stored.Task.Scope.EnvironmentID != environmentID || stored.Task.Progress.Waiting != 1 {
		t.Fatalf("accepted action lost native identity or preparation barrier: %v", err)
	}
	page, err := queue.ListWork(ctx, accepted.TaskID, "", 100, revision)
	if err != nil || len(page.Items) != 1 || page.Items[0].Item.Operation != workexecution.Release ||
		page.Items[0].Item.Input != nil || page.Items[0].Item.Phase != workexecution.Waiting {
		t.Fatalf("acceptance made an incompletely prepared Release executable: %v", err)
	}
	work, err := queue.ReadWork(ctx, accepted.TaskID, page.Items[0].Item.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	captured, found, err := ledger.ReadServiceRollout(ctx, work)
	if err != nil || !found {
		t.Fatalf("accepted Release lost its captured rollout: %v", err)
	}
	preparation, _, err := ledger.ReadDirectReleasePreparation(ctx, work)
	if err != nil || preparation.Predecessor.Serving != nil ||
		preparation.Intent.PriorServingReleaseID != "" || preparation.Intent.CandidateWorkload.LocalImageID != "sha256:"+strings.Repeat("a", 64) ||
		preparation.Intent.CandidateWorkload.RequestedReference != "api:first" {
		t.Fatalf("first native publication lost image or initial-absence authority: %v", err)
	}
	render := captured.Record.Render
	selected, _, found := proxyImage.Select(runtime.GOOS, runtime.GOARCH)
	if !found || render.ProxyImage == nil || render.ProxyImage.Repository != proxyImage.Repository ||
		render.ProxyImage.IndexDigest != proxyImage.IndexDigest || render.ProxyImage.Platform != selected {
		t.Fatal("publication failed to persist exact compiled proxy identity")
	}
	provePreparedReleaseRuntime(t, store, captured.Record, serviceID, detachedNetwork)
	if detachedNetwork {
		capturedSource := mustPublicationArtifact(t, render.Projection.ComposeArtifact)
		const oldNetwork = "gp_attach_net_01arz3ndektsv4rrffq69g5fav"
		if strings.Contains(string(capturedSource.CanonicalYaml), oldNetwork) ||
			!strings.Contains(string(capturedSource.CanonicalYaml), publicationCurrentAttachNetwork) {
			t.Fatal("Release capture did not replace detached memberships with current Attaches")
		}
	}
	if !proto.Equal(artifact, mustPublicationArtifact(t, projection.ComposeArtifact)) {
		t.Fatal("Release capture changed its immutable source")
	}
}

func mustPublicationArtifact(t *testing.T, value []byte) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(value, artifact); err != nil {
		t.Fatal(err)
	}
	return artifact
}

type directPublicationImages struct{}

func (directPublicationImages) ResolveWorkloadImages(
	_ context.Context,
	_ string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	values := make([]*agentpb.WorkloadImageResolution, len(selectors))
	for i, selector := range selectors {
		values[i] = &agentpb.WorkloadImageResolution{
			Selector:     proto.CloneOf(selector),
			LocalImageId: "sha256:" + strings.Repeat("a", 64),
		}
	}
	return &agentpb.WorkloadImageResolutionResult{
		RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{
			Success: &agentpb.WorkloadImageResolutions{Resolutions: values},
		},
	}, nil
}

// Minimal revisioned CAS store for this sequential publication journey.
type directPublicationStore struct {
	revision int64
	values   map[string]*testkeyvalue.KeyValue
}

func (s *directPublicationStore) Health(context.Context) error { return nil }
func (s *directPublicationStore) Close() error                 { return nil }
func (s *directPublicationStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, fmt.Errorf("unexpected watch")
}
func (s *directPublicationStore) Snapshot(context.Context, io.Writer) error {
	return fmt.Errorf("unexpected snapshot")
}
func (s *directPublicationStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}
func (s *directPublicationStore) Get(_ context.Context, key string) (*testkeyvalue.GetResult, error) {
	return &testkeyvalue.GetResult{Entry: s.copyAt(key, s.revision), ReadRevision: s.revision}, nil
}
func (s *directPublicationStore) copyAt(key string, revision int64) *testkeyvalue.KeyValue {
	v := s.values[key]
	if v == nil {
		return nil
	}
	if v.ModRevision > revision {
		panic("fixture attempted unsupported overwritten historical read")
	}
	c := *v
	c.Value = append([]byte(nil), v.Value...)
	return &c
}

func (s *directPublicationStore) GetMany(
	_ context.Context,
	r testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	revision := r.Revision
	if revision == 0 {
		revision = s.revision
	}
	v := make([]*testkeyvalue.KeyValue, len(r.Keys))
	for i, k := range r.Keys {
		v[i] = s.copyAt(k, revision)
	}
	return &testkeyvalue.GetManyResult{Values: v, ReadRevision: revision, ResponseRevision: s.revision}, nil
}

func (s *directPublicationStore) Range(
	_ context.Context,
	r testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	revision := r.Revision
	if revision == 0 {
		revision = s.revision
	}
	keys := []string{}
	for k := range s.values {
		if strings.HasPrefix(k, r.Prefix) && k > r.StartExclusive {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := &testkeyvalue.RangeResult{ReadRevision: revision, ResponseRevision: s.revision}
	for _, k := range keys {
		if r.Limit > 0 && int64(len(out.Values)) == r.Limit {
			out.More = true
			break
		}
		out.Values = append(out.Values, *s.copyAt(k, revision))
	}
	return out, nil
}
func (s *directPublicationStore) Put(ctx context.Context, k string, v []byte) (int64, error) {
	r, e := s.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: k, Value: v}})
	return r.Revision, e
}
func (s *directPublicationStore) Delete(ctx context.Context, k string) (int64, error) {
	r, e := s.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: k}})
	return r.Revision, e
}

func (s *directPublicationStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, c := range conditions {
		actual := int64(0)
		if v := s.values[c.Key]; v != nil {
			actual = v.ModRevision
		}
		if c.Prefix {
			for k := range s.values {
				if strings.HasPrefix(k, c.Key) {
					actual = s.revision
					break
				}
			}
		}
		if actual != c.ModRevision {
			values := make([]*testkeyvalue.KeyValue, len(conditions))
			for i, c := range conditions {
				values[i] = s.copyAt(c.Key, s.revision)
			}
			return testkeyvalue.TransactionResult{Revision: s.revision, FailureReads: values}, nil
		}
	}
	s.revision++
	for _, m := range mutations {
		if m.Prefix {
			panic("unexpected prefix mutation")
		}
		if m.Type == testkeyvalue.MutationDelete {
			delete(s.values, m.Key)
			continue
		}
		version := int64(1)
		if old := s.values[m.Key]; old != nil {
			version = old.Version + 1
		}
		s.values[m.Key] = &testkeyvalue.KeyValue{
			Key:         m.Key,
			Value:       append([]byte(nil), m.Value...),
			ModRevision: s.revision,
			Version:     version,
		}
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: s.revision}, nil
}
