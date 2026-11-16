package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	componentexecution "github.com/AlanD20/groundplane/internal/controller/componentwork"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	servicecontroller "github.com/AlanD20/groundplane/internal/controller/services"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	networkmodel "github.com/AlanD20/groundplane/internal/core/network"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"
	networkstore "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-81: exercise native Route definition acceptance and replay through the
// public capability, actual desired publication and queue repositories. With
// no router enabled, the Controller settles without inventing Agent effects.
func TestNativeRouteDefinitionMutationAndRemovalReplay(t *testing.T) {
	f := newNativeRouteFixture(t)
	request := networkmodel.CreateRouteRequest{
		EnvironmentID:   f.base.environmentID,
		Host:            "app.example.test",
		Path:            "/api/*",
		Exposure:        networkmodel.RouteExposure("public"),
		TargetServiceID: f.target.ID,
		TargetPort:      8080,
	}
	created, err := f.network.CreateRoute(t.Context(), request, "native-route-create-identity")
	if err != nil {
		t.Fatal(err)
	}
	var accepted nativeRouteAccepted
	if err := f.prepareRouteResponse(t, created.Body, &accepted); err != nil || accepted.TaskID == "" ||
		accepted.Route.ID == "" {
		t.Fatalf("create Route: %v", err)
	}
	if created.Status != http.StatusAccepted || accepted.Route.EnvironmentID != request.EnvironmentID ||
		accepted.Route.TargetServiceID != request.TargetServiceID ||
		accepted.Route.Host != request.Host ||
		accepted.Route.Path != request.Path ||
		accepted.Route.Status != "unserved" {
		t.Fatal("Route response changed identity or fabricated serving state")
	}
	f.checkDefinition(t, accepted.TaskID, accepted.Route.ID, true)
	revision := f.base.store.revision
	replayed, err := f.network.CreateRoute(t.Context(), request, "native-route-create-identity")
	if err != nil || !reflect.DeepEqual(replayed, created) || f.base.store.revision != revision {
		t.Fatalf("create Route replay changed state: %v", err)
	}
	edited, err := f.network.EditRoute(
		t.Context(),
		accepted.Route.ID,
		networkmodel.EditRouteRequest{Exposure: networkmodel.RouteExposure("internal")},
		"native-route-edit-exposure",
	)
	if err != nil {
		t.Fatal(err)
	}
	var edit nativeRouteAccepted
	if err := f.prepareRouteResponse(t, edited.Body, &edit); err != nil || edit.Route.ID != accepted.Route.ID ||
		edit.Route.Exposure != "internal" ||
		edit.Route.Host != request.Host ||
		edit.Route.Path != request.Path ||
		edit.Route.TargetServiceID != request.TargetServiceID {
		t.Fatalf("Route edit lost stable match: %v", err)
	}
	f.checkDefinition(t, edit.TaskID, accepted.Route.ID, true)
	removed, err := f.network.RemoveRoute(
		t.Context(),
		accepted.Route.ID,
		api.TaskRunAfterRequest{},
		"native-route-remove-identity",
	)
	if err != nil {
		t.Fatal(err)
	}
	var removal api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &removal); err != nil || removal.TaskID == "" {
		t.Fatalf("remove Route: %v", err)
	}
	f.prepare(t, removal.TaskID)
	f.checkDefinition(t, removal.TaskID, accepted.Route.ID, false)
	if _, err := f.network.GetRoute(t.Context(), accepted.Route.ID); !errors.Is(
		err,
		errs.New(errs.KindRouteNotFound, ""),
	) {
		t.Fatalf("removed Route still readable: %v", err)
	}
	revision = f.base.store.revision
	replayed, err = f.network.RemoveRoute(
		t.Context(),
		accepted.Route.ID,
		api.TaskRunAfterRequest{},
		"native-route-remove-identity",
	)
	if err != nil || !reflect.DeepEqual(replayed, removed) || f.base.store.revision != revision {
		t.Fatalf("removed Route replay lost target authority: %v", err)
	}
}

// Invalid syntax is rejected at acceptance; current Service port eligibility is
// evaluated after the dependency gate and fails the original Task visibly.
func TestNativeRouteRejectsInvalidTargetBeforePublication(t *testing.T) {
	for _, scenario := range []string{"unsafe path", "unexposed port", "UDP-only port"} {
		t.Run(scenario, func(t *testing.T) {
			f := newNativeRouteFixture(t)
			request := networkmodel.CreateRouteRequest{
				EnvironmentID:   f.base.environmentID,
				Host:            "app.example.test",
				Path:            "/api/*",
				Exposure:        "public",
				TargetServiceID: f.target.ID,
				TargetPort:      8080,
			}
			switch scenario {
			case "unsafe path":
				request.Path = "/ok\n}\nrespond 200\n"
			case "unexposed port":
				request.TargetPort = 8081
			case "UDP-only port":
				request.TargetPort = 5353
			}
			revision := f.base.store.revision
			response, err := f.network.CreateRoute(t.Context(), request, "native-route-reject-input")
			if scenario == "unsafe path" {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || f.base.store.revision != revision {
					t.Fatalf("unsafe syntax changed state: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var accepted api.TaskAccepted
			if err := json.Unmarshal(response.Body, &accepted); err != nil {
				t.Fatal(err)
			}
			f.prepare(t, accepted.TaskID)
			task, _, err := f.base.queue.ReadTask(t.Context(), accepted.TaskID, 0)
			if err != nil || task.Task.Outcome != workexecution.Failed || task.Task.FirstFailure == nil {
				t.Fatalf("invalid target did not fail visibly: %v", err)
			}
			page, err := f.base.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
			if err != nil || len(page.Items) != 0 {
				t.Fatalf("invalid target published execution: %v", err)
			}
			if _, err := f.network.GetRoute(t.Context(), task.Task.TargetID); !errors.Is(
				err,
				errs.New(errs.KindRouteNotFound, ""),
			) {
				t.Fatalf("invalid target created Route: %v", err)
			}
		})
	}
}

// TASK-81: settled counts alone cannot close replay. Completion validates the
// exact immutable result and fences its revision with the terminal marker.
func TestNativeRouteCompletionFencesItsExactDecision(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "corrupt result", true: "result replaced before commit"}[race],
			func(t *testing.T) {
				f := newNativeRouteFixture(t)
				response, err := f.network.CreateRoute(
					t.Context(),
					networkmodel.CreateRouteRequest{EnvironmentID: f.base.environmentID,
						Host: "fence.example.test", Path: "/", Exposure: networkmodel.RouteExposure("public"), TargetServiceID: f.target.ID, TargetPort: 8080},
					"native-route-result-fence",
				)
				if err != nil {
					t.Fatal(err)
				}
				var accepted nativeRouteAccepted
				if err := f.prepareRouteResponse(t, response.Body, &accepted); err != nil {
					t.Fatal(err)
				}
				finished, err := f.base.queue.ReadFinishedWork(t.Context(), accepted.TaskID)
				if err != nil || len(finished.Items) != 1 {
					t.Fatalf("Route result: %v", err)
				}
				resultKey := "/v1/route-definition-result/" + finished.Items[0].Item.ID
				result, err := f.base.store.Get(t.Context(), resultKey)
				if err != nil || result.Entry == nil {
					t.Fatalf("Route decision result: %v", err)
				}
				if race {
					f.base.store.beforeCommit = func(mutations []keyvalue.Mutation) {
						for _, mutation := range mutations {
							if mutation.Key == workqueue.TaskKey(accepted.TaskID) {
								f.base.store.beforeCommit = nil
								if _, err := f.base.store.Put(t.Context(), resultKey, result.Entry.Value); err != nil {
									t.Fatal(err)
								}
								return
							}
						}
					}
				} else if _, err := f.base.store.Put(t.Context(), resultKey, []byte("invalid decision bytes")); err != nil {
					t.Fatal(err)
				}
				if err := f.completions.Finish(t.Context(), accepted.TaskID, time.Now().UTC(), time.Hour); err == nil {
					t.Fatal("invalid or replaced result closed the Route Task")
				}
				task, _, err := f.base.queue.ReadTask(t.Context(), accepted.TaskID, 0)
				if err != nil || task.Task.FinishedAt != nil || race && f.base.store.beforeCommit != nil {
					t.Fatalf("unsafe Route completion changed task: %v", err)
				}
				if race {
					if err := f.completions.Finish(t.Context(), accepted.TaskID, time.Now().UTC(), time.Hour); err != nil {
						t.Fatalf("fresh exact result could not finish: %v", err)
					}
				}
			},
		)
	}
}

// A competing desired-state write loses only the captured promotion. The pure
// accepted action remains the same Task and can compile again on a later tick.
func TestNativeRouteRemovalRetriesItsOriginalAcceptedTask(t *testing.T) {
	f := newNativeRouteFixture(t)
	response, err := f.network.CreateRoute(
		t.Context(),
		networkmodel.CreateRouteRequest{
			EnvironmentID:   f.base.environmentID,
			Host:            "retry.example.test",
			Path:            "/",
			Exposure:        "public",
			TargetServiceID: f.target.ID,
			TargetPort:      8080,
		},
		"native-route-retry-source",
	)
	if err != nil {
		t.Fatal(err)
	}
	var created nativeRouteAccepted
	if err := f.prepareRouteResponse(t, response.Body, &created); err != nil {
		t.Fatal(err)
	}
	f.checkDefinition(t, created.TaskID, created.Route.ID, true)
	response, err = f.network.RemoveRoute(
		t.Context(),
		created.Route.ID,
		api.TaskRunAfterRequest{},
		"native-route-publication-retry",
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	conflicted := false
	f.base.store.beforeCommit = func(mutations []keyvalue.Mutation) {
		for _, mutation := range mutations {
			task, err := workqueue.DecodeTask(mutation.Value)
			if err != nil || task.ID != accepted.TaskID || task.ExpectedItems != 1 {
				continue
			}
			f.base.store.beforeCommit = nil
			conflicted = true
			key := hierarchy.EnvironmentMutationEpochKey(f.base.environmentID)
			current, err := f.base.store.Get(t.Context(), key)
			if err != nil || current.Entry == nil {
				t.Fatalf("race epoch: %v", err)
			}
			if _, err = f.base.store.Put(t.Context(), key, current.Entry.Value); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	changed, err := f.preparations.Advance(t.Context(), accepted.TaskID, time.Now().UTC())
	if err != nil || changed || !conflicted {
		t.Fatalf("capture race: changed=%t conflicted=%t err=%v", changed, conflicted, err)
	}
	task, _, err := f.base.queue.ReadTask(t.Context(), accepted.TaskID, 0)
	if err != nil || !task.Task.PendingPreparation() {
		t.Fatalf("capture conflict lost original pure Task: %v", err)
	}
	if _, err = f.network.GetRoute(t.Context(), created.Route.ID); err != nil {
		t.Fatal(err)
	}
	f.prepare(t, accepted.TaskID)
	f.checkDefinition(t, accepted.TaskID, created.Route.ID, false)
}

type nativeRouteFixture struct {
	base         *nativeVolumeFixture
	network      *networkcontroller.Service
	target       api.Service
	completions  *workdispatch.Completions
	preparations *taskdependencies.Preparations
}

func (f *nativeRouteFixture) checkDefinition(t *testing.T, taskID, routeID string, present bool) {
	t.Helper()
	page, err := f.base.queue.ListWork(t.Context(), taskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		task, _, _ := f.base.queue.ReadTask(t.Context(), taskID, 0)
		failure, _ := f.base.queue.ReadAcceptedPreparationFailure(t.Context(), taskID, 0)
		t.Fatalf("Route work: %v task=%+v failure=%+v", err, task.Task, failure)
	}
	item := page.Items[0].Item
	if item.Placement.Executor != workexecution.Controller || item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Succeeded ||
		item.AttemptNumber != 0 ||
		item.TargetID != routeID {
		t.Fatal("definition-only Route fabricated Agent work or remained unsettled")
	}
	projection, found, err := f.base.blueprint.GetEnvironmentComposeProjection(t.Context(), f.base.environmentID)
	if err != nil || !found || projection.Record.RevisionID != taskID {
		t.Fatalf("Route desired projection: %v", err)
	}
	seen := false
	for _, route := range projection.Record.DesiredRoutes {
		if route.Desired.ID == routeID {
			seen = true
		}
	}
	if seen != present {
		t.Fatal("Route execution and desired state were not published together")
	}
	if len(projection.Record.DesiredServices) != 1 || projection.Record.DesiredServices[0].Desired.ID != f.target.ID ||
		len(projection.Record.Volumes) != 1 || projection.Record.Volumes[0].ID != f.base.created.Volume.ID {
		t.Fatal("Route mutation changed unrelated Service or Volume declarations")
	}
	if err := f.completions.Finish(t.Context(), taskID, time.Now().UTC(), time.Hour); err != nil {
		t.Fatalf("finish Route action: %v", err)
	}
}

func newNativeRouteFixture(t *testing.T) *nativeRouteFixture {
	t.Helper()
	f := &nativeRouteFixture{base: newNativeVolumeFixture(t)}
	store := f.base.store
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := desiredrevision.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := etcd.NewRouteRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	attaches, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	serviceRepository, err := servicecontroller.NewMutationRepository(
		f.base.blueprint.HierarchyRepository,
		services,
		zones,
		desired,
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	serviceIdempotency, err := servicecontroller.NewMutationIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	submitter, err := newAcceptedActions(store, protector, f.base.queue, replay, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := servicecontroller.NewMutationService(serviceRepository, serviceIdempotency, nil, submitter)
	if err != nil {
		t.Fatal(err)
	}
	created, err := mutations.CreateService(
		t.Context(),
		api.ServiceCreate{
			EnvironmentID: f.base.environmentID,
			Name:          "api",
			Image:         "nginx:latest",
			Strategy:      "recreate",
			Replicas:      1,
			Expose:        []string{"8080/tcp", "5353/udp"},
		},
		"native-route-target-create",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(created.Body, &f.target); err != nil || f.target.ID == "" {
		t.Fatalf("Route target Service: %v", err)
	}
	repository, err := networkstore.NewRepository(
		f.base.blueprint.HierarchyRepository,
		services,
		zones,
		routes,
		attaches,
		tasks,
		replay,
		f.base.blueprint,
		nativeRouteNoBackingFacts{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.EnableDesiredRevisions(desired); err != nil {
		t.Fatal(err)
	}
	// Use the actual router catalog so definition-only admission checks every
	// registered provider's absence, even though no router is enabled here.
	catalog, err := componentregistration.EnvironmentCatalog()
	if err != nil {
		t.Fatal(err)
	}
	providers, err := taskplanning.NewRouteProviderCapture(repository, catalog)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := materializationcontent.New(store)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := taskplanning.NewComponentConfigurationCapture(catalog, contents)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := runtimeconfiguration.New(store)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := networkcontroller.NewRouteWorkPublisher(
		f.base.blueprint.HierarchyRepository,
		desired,
		providers,
		configuration,
		snapshots,
		contents,
	)
	if err != nil {
		t.Fatal(err)
	}
	f.network, err = networkcontroller.NewEtcdService(repository, coordinator, publisher, submitter)
	if err != nil {
		t.Fatal(err)
	}
	f.preparations, err = taskdependencies.NewPreparations(f.base.queue, f.network.AcceptedPreparations())
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := componentwork.NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	registrations, err := componentexecution.RouteCompletions(inputs)
	if err != nil {
		t.Fatal(err)
	}
	f.completions, err = workdispatch.NewCompletions(f.base.queue, registrations)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type nativeRouteNoBackingFacts struct{}

func (nativeRouteNoBackingFacts) ResolveRemovalDatabase(
	context.Context,
	keyvalue.Versioned[attachments.Record],
	func(string) error,
) error {
	return errs.New(errs.KindInternal, "Route fixture unexpectedly requested database removal")
}

func (nativeRouteNoBackingFacts) SealBackingHookInputsAtRevision(
	context.Context,
	string,
	string,
	backinghook.Configuration,
	int64,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	return nil, errs.New(errs.KindInternal, "Route fixture unexpectedly requested backing hooks")
}

type nativeRouteAccepted struct {
	api.TaskAccepted
	Route api.Route
}

func (f *nativeRouteFixture) prepareRouteResponse(t *testing.T, body []byte, result *nativeRouteAccepted) error {
	t.Helper()
	if err := json.Unmarshal(body, &result.TaskAccepted); err != nil {
		return err
	}
	f.prepare(t, result.TaskID)
	task, _, err := f.base.queue.ReadTask(t.Context(), result.TaskID, 0)
	if err != nil {
		return err
	}
	route, err := f.network.GetRoute(t.Context(), task.Task.TargetID)
	if err != nil {
		return err
	}
	result.Route = api.Route{
		ID:              route.ID,
		EnvironmentID:   route.EnvironmentID,
		Host:            route.Host,
		Path:            route.Path,
		Exposure:        string(route.Exposure),
		TargetServiceID: route.TargetServiceID,
		TargetPort:      route.TargetPort,
		Status:          string(route.Status),
	}
	return nil
}
func (f *nativeRouteFixture) prepare(t *testing.T, taskID string) {
	t.Helper()
	before, err := f.base.queue.ListWork(t.Context(), taskID, "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := f.base.queue.ReadTask(t.Context(), taskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if task.Task.PendingPreparation() && len(before.Items) != 0 {
		t.Fatal("pure Route acceptance captured work")
	}
	for range 4 {
		task, _, err = f.base.queue.ReadTask(t.Context(), taskID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !task.Task.PendingPreparation() {
			return
		}
		changed, err := f.preparations.Advance(t.Context(), taskID, time.Now().UTC())
		if err != nil || !changed {
			t.Fatalf("Route preparation: changed=%t error=%v", changed, err)
		}
	}
	t.Fatal("Route preparation did not finish")
}
