package volume

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestCloneVolumeMutationProjectionPreservesDesiredTopology(t *testing.T) {
	t.Parallel()
	current := etcd.EnvironmentComposeProjection{
		DesiredZones:    []etcd.EnvironmentZoneProjection{{Desired: core.Zone{ID: "net_desired", Name: "private"}}},
		DesiredServices: []etcd.EnvironmentServiceProjection{{Desired: core.Service{ID: "svc_desired", Name: "api"}}},
		DesiredRoutes:   []etcd.EnvironmentRouteProjection{{Desired: core.Route{ID: "route_desired", Path: "/"}}},
		Volumes:         []etcd.EnvironmentVolumeIdentity{{ID: "vol_desired", Slug: "data", Key: "data"}},
	}
	result := cloneVolumeMutationProjection(current)
	if len(result.DesiredZones) != 1 || len(result.DesiredServices) != 1 || len(result.DesiredRoutes) != 1 ||
		len(result.Volumes) != 1 {
		t.Fatalf("cloned Volume projection = %#v", result)
	}
}

// Rationale: a Volume mutation must not erase retained Backup decisions while
// deriving its next desired revision. Unselected removal is not a policy edit.
func TestVolumeMutationPreservesBackupPolicyDecisions(t *testing.T) {
	tenantID, projectID, environment, baseline := volumePolicyProjectionFixture(t)
	for _, test := range []struct{ action, volumeID, key, slug string }{
		{volumeMutationActionAdd, ids.NewAt(ids.KindVolume, environment.CreatedAt, 40), "cache", "cache"},
		{volumeMutationActionEdit, baseline.Volumes[0].ID, baseline.Volumes[0].Key, "renamed"},
		{volumeMutationActionRemove, baseline.Volumes[1].ID, baseline.Volumes[1].Key, baseline.Volumes[1].Slug},
	} {
		t.Run(test.action, func(t *testing.T) {
			_, candidate, _, _, err := buildVolumeMutationCandidate(tenantID, projectID, environment, baseline, true,
				volumeMutationRequest{action: test.action, environmentID: environment.ID,
					volumeID: test.volumeID, key: test.key, slug: test.slug},
				ids.NewAt(ids.KindTask, environment.CreatedAt, 41), 3)
			if err != nil {
				t.Fatal(err)
			}
			want, got := baseline.Backup, candidate.Backup
			if got == nil || got.Enabled != want.Enabled || got.Frequency != want.Frequency || got.Keep != want.Keep ||
				got.Encryption != want.Encryption || got.ConnectorID != want.ConnectorID ||
				len(got.Sources) != 1 || got.Sources[0] != want.Sources[0] {
				t.Fatal("Volume mutation dropped or changed the retained Backup policy")
			}
			if _, err := etcd.EnvironmentBlueprintDependencyDigest(candidate); err != nil {
				t.Fatalf("preserved policy is not stageable: %v", err)
			}
			got.Enabled = false
			got.Sources[0].TargetID = "changed"
			if !baseline.Backup.Enabled || baseline.Backup.Sources[0].TargetID != baseline.Volumes[0].ID {
				t.Fatal("candidate policy aliases the prior desired revision")
			}
		})
	}
}

// Rationale: copying the prior projection is not policy-replacement authority.
// Even selected-source removal retains the original decisions until the caller
// supplies its explicitly prepared replacement for staging and publication.
func TestVolumeMutationRetainsPolicyUntilExplicitSourceReplacement(t *testing.T) {
	tenantID, projectID, environment, baseline := volumePolicyProjectionFixture(t)
	volume := baseline.Volumes[0]
	_, candidate, _, _, err := buildVolumeMutationCandidate(tenantID, projectID, environment, baseline, true,
		volumeMutationRequest{action: volumeMutationActionRemove, environmentID: environment.ID,
			volumeID: volume.ID, key: volume.Key, slug: volume.Slug},
		ids.NewAt(ids.KindTask, environment.CreatedAt, 42), 3)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Backup == nil || candidate.Backup.ConnectorID != baseline.Backup.ConnectorID ||
		!candidate.Backup.Enabled || len(candidate.Backup.Sources) != 1 ||
		candidate.Backup.Sources[0] != baseline.Backup.Sources[0] {
		t.Fatal("selected Volume removal silently discarded the policy before its explicit replacement")
	}
}

func volumePolicyProjectionFixture(
	t *testing.T,
) (string, string, etcd.EnvironmentRecord, etcd.EnvironmentComposeProjection) {
	t.Helper()
	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	tenantID, projectID := ids.NewAt(ids.KindTenant, at, 1), ids.NewAt(ids.KindProject, at, 2)
	environment := etcd.EnvironmentRecord{ID: ids.NewAt(ids.KindEnvironment, at, 3), CreatedAt: at}
	environment.VolumeDir = "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environment.ID
	baseline := etcd.EnvironmentComposeProjection{}
	for index, key := range []string{"data", "scratch"} {
		_, candidate, _, _, err := buildVolumeMutationCandidate(tenantID, projectID, environment, baseline, index != 0,
			volumeMutationRequest{action: volumeMutationActionAdd, environmentID: environment.ID, key: key, slug: key},
			ids.NewAt(ids.KindTask, at, int64(index+10)), uint64(index+1))
		if err != nil {
			t.Fatal(err)
		}
		baseline = candidate
	}
	baseline.Backup = &etcd.EnvironmentBlueprintBackupPolicy{
		Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "age",
		ConnectorID: ids.NewAt(ids.KindConnector, at, 30),
		Sources: []etcd.EnvironmentBlueprintBackupPolicySource{{
			ID: ids.NewAt(
				ids.KindBackupSource,
				at,
				31,
			), Kind: core.BackupSourceVolume, TargetID: baseline.Volumes[0].ID,
		}},
	}
	if _, err := etcd.EnvironmentBlueprintDependencyDigest(baseline); err != nil {
		t.Fatalf("baseline policy fixture: %v", err)
	}
	return tenantID, projectID, environment, baseline
}
