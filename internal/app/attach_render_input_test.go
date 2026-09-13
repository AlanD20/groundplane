package app

import (
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: capture follows Service-name order, which is unrelated to stable-id
// order. Durable Attach selection must sort its copy without changing the capture.
func TestBuildAttachTaskRenderInputSortsRunningIdentityCopy(t *testing.T) {
	fixture := newAttachRenderFixture(t)
	running := []string{fixture.workerID, fixture.apiID}
	slices.Sort(running)
	want := slices.Clone(running)
	slices.Reverse(running)
	input, err := buildAttachTaskRenderInput(fixture.scope,
		controllerpkg.EntryMutationRuntime{Projection: fixture.scope.ComposeProjection.Record,
			EpochRevision: 1, RunningServiceIDs: running},
		fixture.current, nil, fixture.createTask, fixture.artifactID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(input.RunningServiceIDs, want) || slices.IsSorted(running) {
		t.Fatal("Attach running identities were not sorted independently of the captured source")
	}
}

// Rationale: create publication must pin the complete union, including failed provision intent,
// while deduplicating Services that consume the same durable backing network.
func TestBuildAttachTaskRenderInputCreatesCompleteNetworkUnion(t *testing.T) {
	t.Parallel()
	fixture := newAttachRenderFixture(t)
	worker := fixture.attach(t, 30, fixture.networkA, fixture.workerID)
	worker.Status = core.AttachFailed
	duplicate := fixture.attach(t, 31, fixture.networkA, fixture.apiID)
	detaching := fixture.attach(t, 32, fixture.networkB, fixture.workerID)
	detaching, err := etcd.MarkAttachProvisioning(detaching, detaching.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	detaching, err = etcd.CompleteAttachProvisioning(detaching, detaching.TaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	detaching, err = etcd.BeginAttachDetaching(detaching, ids.NewAt(ids.KindTask, fixture.now, 132))
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}

	input, err := buildAttachTaskRenderInput(
		fixture.scope,
		controllerpkg.EntryMutationRuntime{Projection: fixture.scope.ComposeProjection.Record, EpochRevision: 1},
		fixture.current,
		[]etcd.Versioned[etcd.AttachRecord]{
			{Record: worker, Revision: 1},
			{Record: duplicate, Revision: 2},
			{Record: detaching, Revision: 3},
		},
		fixture.createTask,
		fixture.artifactID,
	)
	if err != nil {
		t.Fatalf("buildAttachTaskRenderInput() error = %v", err)
	}
	if len(input.NetworkJoins) != 1 || input.NetworkJoins[0].NetworkID != fixture.networkA ||
		len(input.NetworkJoins[0].ServiceIDs) != 2 || input.NetworkJoins[0].ServiceIDs[0] != fixture.apiID ||
		input.NetworkJoins[0].ServiceIDs[1] != fixture.workerID {
		t.Fatalf("buildAttachTaskRenderInput() joins = %#v", input.NetworkJoins)
	}
	if input.PlanID != fixture.createTask.PlanID || input.DesiredRevisionID != fixture.revisionID ||
		input.ArtifactID != fixture.artifactID || input.BackingServiceID != fixture.backingServiceID ||
		input.Authentication != core.BackingAuthenticationPassword {
		t.Fatalf("buildAttachTaskRenderInput() identity = %#v", input)
	}
	if len(input.Volumes) != 1 || len(input.VolumeMounts) != 1 ||
		input.VolumeMounts[0].VolumeID != input.Volumes[0].ID {
		t.Fatalf("buildAttachTaskRenderInput() volume projection = %#v/%#v", input.Volumes, input.VolumeMounts)
	}
}

// Rationale: detach publication must remove only the target membership and preserve every union edge
// still required by another provision-intent Attach.
func TestBuildAttachTaskRenderInputDetachesOnlyTargetMembership(t *testing.T) {
	t.Parallel()
	fixture := newAttachRenderFixture(t)
	ready, err := etcd.MarkAttachProvisioning(fixture.current, fixture.current.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	ready, err = etcd.CompleteAttachProvisioning(ready, ready.TaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	detachTask := fixture.createTask
	detachTask.ID = ids.NewAt(ids.KindTask, fixture.now, 50)
	detachTask.Type = etcd.TaskDetach
	detachTask.PlanID = ids.NewAt(ids.KindPlan, fixture.now, 51)
	detaching, err := etcd.BeginAttachDetaching(ready, detachTask.ID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	shared := fixture.attach(t, 33, fixture.networkA, fixture.workerID)
	other := fixture.attach(t, 34, fixture.networkB, fixture.apiID)

	input, err := buildAttachTaskRenderInput(
		fixture.scope,
		controllerpkg.EntryMutationRuntime{Projection: fixture.scope.ComposeProjection.Record, EpochRevision: 1},
		detaching,
		[]etcd.Versioned[etcd.AttachRecord]{
			{Record: detaching, Revision: 1},
			{Record: shared, Revision: 2},
			{Record: other, Revision: 3},
		},
		detachTask,
		fixture.artifactID,
	)
	if err != nil {
		t.Fatalf("buildAttachTaskRenderInput() error = %v", err)
	}
	if len(input.NetworkJoins) != 2 || input.NetworkJoins[0].NetworkID != fixture.networkA ||
		len(input.NetworkJoins[0].ServiceIDs) != 1 || input.NetworkJoins[0].ServiceIDs[0] != fixture.workerID ||
		input.NetworkJoins[1].NetworkID != fixture.networkB || len(input.NetworkJoins[1].ServiceIDs) != 1 ||
		input.NetworkJoins[1].ServiceIDs[0] != fixture.apiID {
		t.Fatalf("buildAttachTaskRenderInput() detach joins = %#v", input.NetworkJoins)
	}
}

type attachRenderFixture struct {
	now              time.Time
	tenantID         string
	projectID        string
	environmentID    string
	backingProjectID string
	backingEnvID     string
	backingServiceID string
	apiID            string
	workerID         string
	volumeID         string
	networkA         string
	networkB         string
	revisionID       string
	artifactID       string
	scope            etcd.AttachCreateScope
	current          etcd.AttachRecord
	createTask       etcd.TaskRecord
}

func newAttachRenderFixture(t *testing.T) attachRenderFixture {
	t.Helper()
	now := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	fixture := attachRenderFixture{
		now:      now,
		tenantID: ids.NewAt(ids.KindTenant, now, 1), projectID: ids.NewAt(ids.KindProject, now, 2),
		environmentID:    ids.NewAt(ids.KindEnvironment, now, 3),
		backingProjectID: ids.NewAt(ids.KindProject, now, 4),
		backingEnvID:     ids.NewAt(ids.KindEnvironment, now, 5),
		backingServiceID: ids.NewAt(ids.KindService, now, 6),
		apiID:            ids.NewAt(ids.KindService, now, 7), workerID: ids.NewAt(ids.KindService, now, 8),
		networkA: ids.NewAt(ids.KindNetwork, now, 9), networkB: ids.NewAt(ids.KindNetwork, now, 10),
		revisionID: ids.NewAt(ids.KindTask, now, 11), artifactID: ids.NewAt(ids.KindConfig, now, 12),
		volumeID: ids.NewAt(ids.KindVolume, now, 16),
	}
	fixture.scope = etcd.AttachCreateScope{
		Tenant: etcd.Versioned[etcd.TenantRecord]{
			Record: etcd.TenantRecord{ID: fixture.tenantID, Slug: "acme"}, Revision: 1,
		},
		Project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{
				ID: fixture.projectID, TenantID: fixture.tenantID, Slug: "shop", Kind: etcd.ProjectKindTenant,
			},
			Revision: 2,
		},
		Environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
				ID: fixture.environmentID, ProjectID: fixture.projectID, Name: "prod",
				VolumeDir: "/var/lib/groundplane/vol/acme/shop/" + fixture.environmentID,
			},
			Revision: 3,
		},
		DesiredHead: etcd.Versioned[etcd.EnvironmentBlueprintHead]{
			Record: etcd.EnvironmentBlueprintHead{
				EnvironmentID: fixture.environmentID, RevisionID: fixture.revisionID,
			},
			Revision: 4,
		},
		ComposeProjection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: etcd.EnvironmentComposeProjection{
				EnvironmentID: fixture.environmentID, RevisionID: fixture.revisionID,
				RenderGeneration: 4,
				DesiredServices: []etcd.EnvironmentServiceProjection{
					{EnvironmentID: fixture.environmentID, Desired: core.Service{ID: fixture.apiID, Name: "api"}},
					{EnvironmentID: fixture.environmentID, Desired: core.Service{ID: fixture.workerID, Name: "worker"}},
				},
				DesiredZones: []etcd.EnvironmentZoneProjection{{
					EnvironmentID: fixture.environmentID,
					Desired: core.Zone{
						ID: ids.NewAt(ids.KindNetwork, now, 20), Name: "app",
						OwnerKind: core.ZoneOwnerEnvironment, OwnerID: fixture.environmentID,
					},
				}},
				Volumes: []etcd.EnvironmentVolumeIdentity{{ID: fixture.volumeID, Slug: "data", Key: "data"}},
				VolumeMounts: []etcd.EnvironmentServiceVolumeMount{{
					ServiceID: fixture.apiID, VolumeID: fixture.volumeID, Target: "/data",
				}},
			},
			Revision: 5,
		},
		Services: []etcd.Versioned[etcd.ServiceRecord]{
			{
				Record: etcd.ServiceRecord{
					EnvironmentID: fixture.environmentID,
					Desired:       core.Service{ID: fixture.apiID},
				},
			},
		},
		BackingProject: etcd.Versioned[etcd.ProjectRecord]{
			Record:   etcd.ProjectRecord{ID: fixture.backingProjectID, Slug: "postgres", Kind: etcd.ProjectKindBacking},
			Revision: 6,
		},
		BackingEnvironment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
				ID: fixture.backingEnvID, ProjectID: fixture.backingProjectID, Name: "main",
				VolumeDir: "/var/lib/groundplane/vol/backing/postgres/" + fixture.backingEnvID,
			},
			Revision: 7,
		},
		BackingService: etcd.Versioned[etcd.ServiceRecord]{
			Record: etcd.ServiceRecord{
				EnvironmentID: fixture.backingEnvID, BackingNetworkID: fixture.networkA,
				Desired: core.Service{
					ID: fixture.backingServiceID, Name: "valkey", Adapter: "valkey:9",
					Authentication: core.BackingAuthenticationPassword,
				},
			},
			Revision: 8,
		},
	}
	attachID := ids.NewAt(ids.KindAttach, now, 13)
	taskID := ids.NewAt(ids.KindTask, now, 14)
	current, err := etcd.NewPendingAttachRecord(
		attachID, fixture.environmentID, "api-db", fixture.backingProjectID, fixture.backingEnvID,
		fixture.backingServiceID, fixture.networkA, fixture.apiID, attachID, nil, nil, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	fixture.current = current
	fixture.createTask = etcd.TaskRecord{
		ID: taskID, PlanID: ids.NewAt(ids.KindPlan, now, 15), Type: etcd.TaskAttach, Target: attachID,
		RenderGeneration: 4,
	}
	return fixture
}

func (fixture attachRenderFixture) attach(
	t *testing.T,
	seed int64,
	networkID string,
	serviceID string,
) etcd.AttachRecord {
	t.Helper()
	record, err := etcd.NewPendingAttachRecord(
		ids.NewAt(ids.KindAttach, fixture.now, seed), fixture.environmentID, "attach", fixture.backingProjectID,
		fixture.backingEnvID, fixture.backingServiceID, networkID, serviceID,
		ids.NewAt(ids.KindAttach, fixture.now, seed), nil, nil,
		ids.NewAt(ids.KindTask, fixture.now, seed+100), fixture.now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	return record
}
