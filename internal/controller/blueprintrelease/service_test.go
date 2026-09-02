package blueprintrelease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: no candidate Release means the producer seals exactly its typed
// prefix. It must not fabricate a Compose reconcile that restart cannot derive
// from durable state.
func TestPrepareWithoutCandidatesPreservesMaterializationAndVolumePrefix(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 16, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 201)
	artifactID := ids.NewAt(ids.KindConfig, at, 202)
	volumeID := ids.NewAt(ids.KindVolume, at, 203)
	planID := ids.NewAt(ids.KindPlan, at, 209)
	materialization := etcd.TaskMaterializationRecord{
		StepID:            ids.NewAt(ids.KindStep, at, 204),
		MaterializationID: ids.NewAt(ids.KindConfig, at, 205),
		EnvironmentID:     environmentID, Destination: "blueprints/" + planID + "/blueprint.yaml",
		OutputKind: etcd.TaskMaterializationOutputPlainFile, Mode: 0o444,
		SHA256: hex.EncodeToString(make([]byte, sha256.Size)),
	}
	materializeStep, err := controller.BuildTaskMaterializationStep(materialization, artifactID, 120)
	if err != nil {
		t.Fatalf("BuildTaskMaterializationStep() error = %v", err)
	}
	volumeStep := &agentpb.ExecutionStep{
		StepId: ids.NewAt(ids.KindStep, at, 206), TimeoutSeconds: 120,
		Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
			ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
				ArtifactId: artifactID, VolumeIds: []string{volumeID}, IntentSha256: make([]byte, sha256.Size),
			},
		},
	}
	yaml := []byte("services: {}\nvolumes:\n  data:\n    name: gp_vol_" + volumeID + "\n")
	yamlDigest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID), CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID,
		Volumes: []*agentpb.ComposeVolume{{
			VolumeId: volumeID, ComposeName: "data", DockerName: "gp_vol_" + volumeID,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: environmentID},
				{Key: "com.groundplane.kind", Value: "volume"},
				{Key: "com.groundplane.managed", Value: "true"},
			},
		}},
	}
	resolver, err := controller.NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	service := &Service{ledger: &etcd.ReleaseLedger{}, plans: resolver}
	memberships, err := BuildNormalizedServiceMemberships(nil, &composetypes.Project{})
	if err != nil {
		t.Fatalf("BuildNormalizedServiceMemberships() error = %v", err)
	}
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 207), OperationID: ids.NewAt(ids.KindOperation, at, 208),
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: 1, Type: etcd.TaskUpdate, Target: environmentID,
		Params: map[string]string{
			taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
		},
		TimeoutSeconds: 120,
	}
	prepared, err := service.Prepare(context.Background(), PrepareInput{
		VolumeRoot: "/var/lib/groundplane/vol",
		Projection: etcd.EnvironmentComposeProjection{
			EnvironmentID:     environmentID,
			RevisionID:        task.ID,
			NormalizedCompose: []byte("services: {}\n"),
		},
		Memberships: memberships,
		Task:        task, PrefixSteps: []*agentpb.ExecutionStep{materializeStep, volumeStep}, Artifact: artifact,
		AllocateNamed: func(kind ids.Kind, name string) string { return ids.NewAt(kind, at, int64(len(name)+300)) },
		CreatedAt:     at,
	})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if len(prepared.Plan.GetSteps()) != 2 || len(prepared.Task.Steps) != 2 ||
		prepared.Plan.Steps[0].GetMaterializeFile() == nil ||
		prepared.Plan.Steps[1].GetManagedVolumeDirectoriesEnsure() == nil ||
		prepared.Task.Params[taskcontract.EnvironmentBlueprintProcedureParam] != string(taskcontract.BlueprintComposeProcedureNone) ||
		prepared.Task.Params[etcd.TaskReleasePublicationParam] != "" {
		t.Fatalf("no-candidate Blueprint preparation = %#v / %#v", prepared.Task, prepared.Plan)
	}
}

func TestSealedCandidateSelectsRunningChangedSingletonsInDependencyOrder(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	databaseID := ids.NewAt(ids.KindService, at, 2)
	apiID := ids.NewAt(ids.KindService, at, 3)
	workerID := ids.NewAt(ids.KindService, at, 4)
	stoppedID := ids.NewAt(ids.KindService, at, 5)
	current := func(id, name, image string, intent core.ServiceRuntimeIntent) *etcd.Versioned[etcd.ServiceRecord] {
		record, err := etcd.NewServiceRecord(environmentID, core.Service{
			ID: id, Name: name, Image: image, Strategy: core.StrategyRecreate, Replicas: 1,
		}, "")
		if err != nil {
			t.Fatal(err)
		}
		record.Runtime.RuntimeIntent = intent
		versioned := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}
		return &versioned
	}
	change := func(existing *etcd.Versioned[etcd.ServiceRecord], image string) etcd.EnvironmentBlueprintServiceChange {
		desired := existing.Record.Desired
		desired.Image = image
		record, err := etcd.ReplaceServiceDesired(existing.Record, desired)
		if err != nil {
			t.Fatal(err)
		}
		return etcd.EnvironmentBlueprintServiceChange{Current: existing, Record: record}
	}
	database, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: databaseID, Name: "database", Image: "registry.example/database:v1",
		Strategy: core.StrategyRecreate, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	api := current(apiID, "api", "registry.example/api:v1", core.ServiceRuntimeIntentRunning)
	worker := current(workerID, "worker", "registry.example/worker:v1", core.ServiceRuntimeIntentRunning)
	stopped := current(stoppedID, "stopped", "registry.example/stopped:v1", core.ServiceRuntimeIntentStopped)
	changes := []etcd.EnvironmentBlueprintServiceChange{
		change(api, "registry.example/api:v2"),
		{Record: database},
		change(worker, "registry.example/worker:v2"),
		change(stopped, "registry.example/stopped:v2"),
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		ServiceDependencyPlans: core.ServiceDependencyPlans{
			DeployDependencyPlan: core.ServiceDependencyPhasePlan{
				Phase:           core.ServiceLifecycleDeploy,
				OrderedServices: []string{"database", "api", "stopped", "worker"},
				Edges: []core.ServiceDependencyEdge{{
					Service: "api", Dependency: "database",
					Condition: core.ServiceDependencyStarted,
				}},
			},
		},
	}

	memberships, err := BuildNormalizedServiceMemberships(
		&composetypes.Project{Services: composetypes.Services{
			"api": {}, "worker": {}, "stopped": {},
		}},
		&composetypes.Project{Services: composetypes.Services{
			"database": {}, "api": {}, "worker": {}, "stopped": {},
		}},
	)
	if err != nil {
		t.Fatalf("BuildNormalizedServiceMemberships() error = %v", err)
	}
	selected, err := selectCandidates(
		projection,
		changes,
		map[string]struct{}{workerID: {}},
		memberships,
	)
	if err != nil {
		t.Fatalf("selectCandidates() error = %v", err)
	}
	if len(selected) != 2 || selected[0].Record.Desired.ID != databaseID || selected[1].Record.Desired.ID != apiID {
		t.Fatalf("selected candidates = %#v", selected)
	}
}

// Rationale: native Compose profiles are operator decisions, so implicit
// Blueprint Releases and post-deploy hooks must never activate profiled Services.
func TestSelectCandidatesExcludesNewAndChangedProfileDisabledServices(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 18, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 401)
	enabledNewID := ids.NewAt(ids.KindService, at, 402)
	enabledChangedID := ids.NewAt(ids.KindService, at, 403)
	disabledNewID := ids.NewAt(ids.KindService, at, 404)
	disabledChangedID := ids.NewAt(ids.KindService, at, 405)
	newService := func(id, name, image string) etcd.EnvironmentBlueprintServiceChange {
		record, err := etcd.NewServiceRecord(environmentID, core.Service{
			ID: id, Name: name, Image: image, Strategy: core.StrategyRecreate, Replicas: 1,
		}, "")
		if err != nil {
			t.Fatal(err)
		}
		record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
		return etcd.EnvironmentBlueprintServiceChange{Record: record}
	}
	changedService := func(id, name string) etcd.EnvironmentBlueprintServiceChange {
		current := newService(id, name, "registry.example/"+name+":v1").Record
		currentVersion := &etcd.Versioned[etcd.ServiceRecord]{Record: current, Revision: 7, ReadRevision: 9}
		desired := current.Desired
		desired.Image = "registry.example/" + name + ":v2"
		record, err := etcd.ReplaceServiceDesired(current, desired)
		if err != nil {
			t.Fatal(err)
		}
		return etcd.EnvironmentBlueprintServiceChange{Current: currentVersion, Record: record}
	}
	changes := []etcd.EnvironmentBlueprintServiceChange{
		newService(enabledNewID, "frontend", "registry.example/frontend:v1"),
		changedService(enabledChangedID, "api"),
		newService(disabledNewID, "migrate", "registry.example/migrate:v1"),
		changedService(disabledChangedID, "worker"),
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		NormalizedCompose: []byte(
			"services:\n" +
				"  frontend:\n    image: registry.example/frontend:v1\n" +
				"  api:\n    image: registry.example/api:v2\n" +
				"  migrate:\n    image: registry.example/migrate:v1\n    profiles: [jobs]\n" +
				"  worker:\n    image: registry.example/worker:v2\n    profiles: [jobs]\n",
		),
		ServiceDependencyPlans: core.ServiceDependencyPlans{
			DeployDependencyPlan: core.ServiceDependencyPhasePlan{
				Phase:           core.ServiceLifecycleDeploy,
				OrderedServices: []string{"migrate", "frontend", "worker", "api"},
			},
		},
	}
	memberships, err := BuildNormalizedServiceMemberships(
		&composetypes.Project{Services: composetypes.Services{
			"api": {}, "worker": {},
		}},
		&composetypes.Project{
			Services:         composetypes.Services{"frontend": {}, "api": {}},
			DisabledServices: composetypes.Services{"migrate": {}, "worker": {}},
		},
	)
	if err != nil {
		t.Fatalf("BuildNormalizedServiceMemberships() error = %v", err)
	}
	selected, err := selectCandidates(projection, changes, nil, memberships)
	if err != nil {
		t.Fatalf("selectCandidates() error = %v", err)
	}
	if len(selected) != 2 || selected[0].Record.Desired.ID != enabledNewID ||
		selected[1].Record.Desired.ID != enabledChangedID {
		t.Fatalf("selected Blueprint candidates = %#v", selected)
	}
}

// Rationale: enabling a previously profile-disabled singleton is an effective
// desired-state transition even when its flattened Service fields are equal.
func TestSelectCandidatesIncludesProfileDisabledToActiveTransition(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 18, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 451)
	serviceID := ids.NewAt(ids.KindService, at, 452)
	equalActiveID := ids.NewAt(ids.KindService, at, 453)
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "worker", Image: "registry.example/worker:v1",
		Strategy: core.StrategyRecreate, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	current := &etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}
	equalActive, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: equalActiveID, Name: "api", Image: "registry.example/api:v1",
		Strategy: core.StrategyRecreate, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	equalActive.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	equalActiveCurrent := &etcd.Versioned[etcd.ServiceRecord]{Record: equalActive, Revision: 8, ReadRevision: 9}
	memberships, err := BuildNormalizedServiceMemberships(
		&composetypes.Project{
			Services: composetypes.Services{"api": {Name: "api", Image: "registry.example/api:v1"}},
			DisabledServices: composetypes.Services{
				"worker": {Name: "worker", Image: "registry.example/worker:v1"},
			},
		},
		&composetypes.Project{Services: composetypes.Services{
			"worker": {Name: "worker", Image: "registry.example/worker:v1"},
			"api":    {Name: "api", Image: "registry.example/api:v1"},
		}},
	)
	if err != nil {
		t.Fatalf("BuildNormalizedServiceMemberships() error = %v", err)
	}
	selected, err := selectCandidates(
		etcd.EnvironmentComposeProjection{EnvironmentID: environmentID},
		[]etcd.EnvironmentBlueprintServiceChange{
			{Current: current, Record: record},
			{Current: equalActiveCurrent, Record: equalActive},
		},
		nil,
		memberships,
	)
	if err != nil {
		t.Fatalf("selectCandidates() error = %v", err)
	}
	if len(selected) != 1 || selected[0].Record.Desired.ID != serviceID {
		t.Fatalf("selected Blueprint candidates = %#v", selected)
	}
}

// Rationale: omission from the sealed normalized Compose project is corruption,
// not profile disablement, and must fail before the no-candidate path is built.
func TestPrepareRejectsChangedOrNewServiceMissingFromSealedProjection(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 19, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 501)
	serviceID := ids.NewAt(ids.KindService, at, 502)
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "worker", Image: "registry.example/worker:v2",
		Strategy: core.StrategyRecreate, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	current := record
	current.Desired.Image = "registry.example/worker:v1"
	currentVersion := &etcd.Versioned[etcd.ServiceRecord]{Record: current, Revision: 7, ReadRevision: 9}
	resolver, err := controller.NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	releaseService := &Service{ledger: &etcd.ReleaseLedger{}, plans: resolver}
	for name, change := range map[string]etcd.EnvironmentBlueprintServiceChange{
		"new":     {Record: record},
		"changed": {Current: currentVersion, Record: record},
	} {
		t.Run(name, func(t *testing.T) {
			var previousProject *composetypes.Project
			if change.Current != nil {
				previousProject = &composetypes.Project{Services: composetypes.Services{"worker": {}}}
			}
			memberships, membershipErr := BuildNormalizedServiceMemberships(
				previousProject,
				&composetypes.Project{},
			)
			if membershipErr != nil {
				t.Fatalf("BuildNormalizedServiceMemberships() error = %v", membershipErr)
			}
			task := etcd.TaskRecord{
				ID:               ids.NewAt(ids.KindTask, at, int64(510+len(name))),
				OperationID:      ids.NewAt(ids.KindOperation, at, int64(520+len(name))),
				Executor:         etcd.TaskExecutorAgent,
				PlanID:           ids.NewAt(ids.KindPlan, at, int64(530+len(name))),
				RenderGeneration: 1,
				Type:             etcd.TaskUpdate,
				Target:           environmentID,
				Params: map[string]string{
					taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
				},
				TimeoutSeconds: 120,
			}
			prepared, prepareErr := releaseService.Prepare(context.Background(), PrepareInput{
				VolumeRoot: "/var/lib/groundplane/vol",
				Projection: etcd.EnvironmentComposeProjection{
					EnvironmentID:     environmentID,
					RevisionID:        task.ID,
					NormalizedCompose: []byte("services: {}\n"),
				},
				ServiceChanges: []etcd.EnvironmentBlueprintServiceChange{change},
				Memberships:    memberships,
				Task:           task,
				Artifact:       &agentpb.ComposeArtifact{},
				AllocateNamed: func(kind ids.Kind, label string) string {
					return ids.NewAt(kind, at, int64(540+len(label)))
				},
				CreatedAt: at,
			})
			if !errors.Is(prepareErr, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Prepare() error = %v, want internal", prepareErr)
			}
			if prepared.Plan != nil || prepared.Task.ID != "" {
				t.Fatalf("missing Service produced prepared state = %#v", prepared)
			}
		})
	}
}

func TestPostDeployHookBoundsRejectBeforePublication(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		executions int
		bodyBytes  uint64
	}{
		{name: "seventeenth execution", executions: 17, bodyBytes: 17},
		{name: "aggregate body", executions: 16, bodyBytes: 1<<20 + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostDeployHookBounds(test.executions, test.bodyBytes)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("validatePostDeployHookBounds() error = %v, want validation.failed", err)
			}
		})
	}
}

func TestPostDeployScriptSelectionIncludesNewServiceAndExcludesManual(t *testing.T) {
	t.Parallel()
	serviceID := "svc_new"
	selected := postDeployScriptsByService([]etcd.ScriptRecord{
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_post", Slug: "migrate", When: core.ScriptPostDeploy},
		},
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_manual", Slug: "manual", When: core.ScriptManual},
		},
	})
	if len(selected) != 1 || len(selected[serviceID]) != 1 ||
		selected[serviceID][0].Desired.ID != "scr_post" {
		t.Fatalf("selected post-deploy Scripts = %#v", selected)
	}
}
