package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BACK-11 / TASK-78: Custom creation publishes only its selected shared runtime,
// not managed-adapter defaults. Request replay must retain the original native
// work and image capture after a tag moves. Repositories/CAS/encryption are real;
// registry/Agent image observations are supplied, and no container is started.
func TestNativeCustomBackingCreationPreservesTagAndCapturedRuntime(t *testing.T) {
	for _, race := range []bool{false, true} {
		name := "ordinary acceptance"
		if race {
			name = "acceptance wins after replay lookup"
		}
		t.Run(name, func(t *testing.T) { nativeCustomBackingCreationJourney(t, race) })
	}
}

func nativeCustomBackingCreationJourney(t *testing.T, race bool) {
	t.Helper()
	// Reuse an enrolled host and an unrelated Tenant Environment as an isolation
	// sentinel. A Backing Project must not acquire that Tenant or alter its input.
	f := newNativeVolumeFixture(t)
	before, found, err := f.blueprint.GetEnvironmentDesiredInput(t.Context(), f.environmentID)
	if err != nil || !found {
		t.Fatal("sentinel desired input is missing", err)
	}
	service, images, inputs := newNativeBackingCreationService(t, f)
	request := api.BackingServiceCreate{
		Slug:        "search-api",
		Name:        "Search API",
		Adapter:     "custom",
		Image:       images.plan.Requested,
		NetworkPool: "10.200.0.0/24",
		Zone:        api.BackingServiceZoneCreate{Name: "data", Subnet: "10.200.0.0/28", Internal: true},
	}
	var winner idempotencyrecord.IdempotencyResponse
	if race {
		f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
			for _, mutation := range mutations {
				if !strings.HasPrefix(mutation.Key, "/v1/staging/backing-services/") {
					continue
				}
				f.store.beforeCommit = nil
				var err error
				winner, err = service.CreateBackingService(t.Context(), request, "native-custom-create-network-only")
				if err != nil || winner.Status != 201 {
					t.Fatal("competing acceptance did not commit", err)
				}
				return
			}
		}
	}
	response, err := service.CreateBackingService(t.Context(), request, "native-custom-create-network-only")
	if race {
		if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || f.store.beforeCommit != nil ||
			winner.Status != 201 {
			t.Fatal("losing stage admission did not resolve the winning acceptance", err)
		}
		response = winner
	} else if err != nil || response.Status != 201 {
		t.Fatal("Custom creation was not accepted", err)
	}
	var created api.BackingServiceCreated
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatal(err)
	}
	backing := created.BackingService
	project, err := f.blueprint.GetProject(t.Context(), backing.ProjectID)
	if err != nil || project.Record.Kind != hierarchy.ProjectKindBacking || project.Record.TenantID != "" {
		t.Fatal("shared Backing acquired a Tenant owner", err)
	}
	serviceRecords, err := etcd.NewServiceRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := serviceRecords.GetService(t.Context(), backing.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Record.Desired.Image != request.Image || stored.Record.Desired.Adapter != "custom" ||
		len(stored.Record.Desired.Mounts) != 0 || !stored.Record.Desired.Healthcheck.Empty() {
		t.Fatal("Custom creation acquired managed-adapter defaults or lost the authored image")
	}
	page, err := f.queue.ListWork(t.Context(), created.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatal("Custom creation did not publish one native item", err)
	}
	view, err := f.queue.ReadWork(t.Context(), created.TaskID, page.Items[0].Item.ID, 0)
	if err != nil || view.Item.Item.Operation != workexecution.BackingProvision ||
		view.Item.Item.Placement.HostID != f.authorization.Identity().HostID {
		t.Fatal("Custom creation work lost native operation/host authority", err)
	}
	capture, _, err := inputs.Load(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(capture.Wire)
	// Explicit Retry extends the mutable Task deadline, not the sealed procedure.
	extended := view.Task.Task
	extended.Deadline = extended.Deadline.Add(time.Minute)
	if err := capture.Matches(extended, view.Item.Item); err != nil {
		t.Fatal("Retry deadline invalidated the unchanged captured input", err)
	}
	extended.Deadline = view.Task.Task.Deadline.Add(-time.Second)
	if capture.Matches(extended, view.Item.Item) == nil {
		t.Fatal("capture accepted a Task deadline preceding its initial bound")
	}
	procedure, err := capture.Procedure()
	if err != nil {
		t.Fatal(err)
	}
	if procedure.ServiceId != backing.ServiceID || procedure.TaskId != created.TaskID ||
		len(procedure.Files) != 0 || len(procedure.ManagedVolumeIds) != 0 || procedure.AfterStart != nil ||
		len(procedure.Runtime.Services) != 1 || len(procedure.Runtime.Volumes) != 0 ||
		procedure.Runtime.Services[0].ImageReference != images.plan.ConfigDigest {
		t.Fatal("Custom creation capture did not bind its exact image and network-only runtime")
	}
	projection, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), backing.EnvironmentID)
	if err != nil || !found {
		t.Fatal("published Backing projection is missing", err)
	}
	resolved := projection.Record
	resolved.NormalizedCompose = procedure.Runtime.CanonicalYaml
	runtime, err := composerender.LoadNormalizedEnvironmentProject(t.Context(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	workload := runtime.Services["search-api"]
	if workload.Image != images.plan.ConfigDigest || len(workload.Networks) != 1 || workload.Networks["data"] == nil ||
		len(workload.Volumes) != 0 || len(workload.Environment) != 0 || len(workload.EnvFiles) != 0 ||
		len(
			workload.Command,
		) != 0 || len(workload.Expose) != 0 || workload.HealthCheck != nil || len(runtime.Volumes) != 0 ||
		len(projection.Record.Entries) != 0 || len(projection.Record.Volumes) != 0 {
		t.Fatal("Custom runtime acquired a managed volume, credentials, command or healthcheck")
	}
	images.plan.ManifestDigest, images.plan.ConfigDigest = "sha256:"+strings.Repeat(
		"c",
		64,
	), "sha256:"+strings.Repeat(
		"d",
		64,
	)
	beforeReplay := f.store.revision
	_, err = service.CreateBackingService(t.Context(), request, "native-custom-create-network-only")
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || images.registryCalls != 1 ||
		images.agentCalls != 1 ||
		f.store.revision != beforeReplay {
		t.Fatal("pending replay selected a moved image tag or published more work", err)
	}
	view, err = f.queue.ReadWork(t.Context(), created.TaskID, view.Item.Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	reopened, _, err := inputs.Load(t.Context(), view)
	if err != nil || !bytes.Equal(reopened.Wire, capture.Wire) {
		t.Fatal("replay changed captured runtime", err)
	}
	clear(reopened.Wire)
	after, found, err := f.blueprint.GetEnvironmentDesiredInput(t.Context(), f.environmentID)
	if err != nil || !found || after.Revision != before.Revision {
		t.Fatal("Backing creation changed unrelated desired state", err)
	}
}

type nativeBackingCreationImages struct {
	t                         *testing.T
	agentID                   string
	plan                      imagefetch.Plan
	registryCalls, agentCalls int
}

func (store *nativeVolumeStore) TransactBackingService(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return store.Transact(ctx, conditions, mutations)
}

func (images *nativeBackingCreationImages) Resolve(_ context.Context, reference string) (imagefetch.Plan, error) {
	if reference != images.plan.Requested {
		images.t.Fatal("creation selected an unexpected registry image")
	}
	images.registryCalls++
	return images.plan, nil
}

func (images *nativeBackingCreationImages) ResolveWorkloadImages(
	_ context.Context,
	agentID string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	if agentID != images.agentID || len(selectors) != 1 ||
		selectors[0].GetRequestedReference() != images.plan.Reference() {
		images.t.Fatal("creation did not inspect the selected manifest on its enrolled Agent")
	}
	images.agentCalls++
	return &agentpb.WorkloadImageResolutionResult{RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{
				{Selector: proto.CloneOf(selectors[0]), LocalImageId: images.plan.ConfigDigest},
			},
		}}}, nil
}
