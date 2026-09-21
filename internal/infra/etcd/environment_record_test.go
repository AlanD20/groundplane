package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
)

func TestNewProvisioningEnvironmentDerivesStableTenantAndBackingPaths(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, createdAt, 1)
	projectID := ids.NewAt(ids.KindProject, createdAt, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, createdAt, 3)
	taskID := ids.NewAt(ids.KindTask, createdAt, 4)

	for _, test := range []struct {
		name    string
		project testhierarchy.ProjectRecord
		want    string
	}{
		{
			name: "tenant",
			project: testhierarchy.ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "renamable", Name: "Tenant project",
				Kind: testhierarchy.ProjectKindTenant,
			},
			want: environmentpath.DefaultVolumeRoot + "/" + tenantID + "/" + projectID + "/" + environmentID,
		},
		{
			name: "backing",
			project: testhierarchy.ProjectRecord{
				ID: projectID, Slug: "renamable", Name: "Backing project", Kind: testhierarchy.ProjectKindBacking,
			},
			want: environmentpath.DefaultVolumeRoot + "/platform/" + projectID + "/" + environmentID,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			record, err := testhierarchy.NewProvisioningEnvironment(
				environmentpath.DefaultVolumeRoot,
				test.project,
				environmentID,
				"production",
				"10.35.0.0/16",
				taskID,
				createdAt,
			)
			if err != nil {
				t.Fatalf("NewProvisioningEnvironment() error = %v", err)
			}
			if record.VolumeDir != test.want ||
				record.ProvisioningState != testhierarchy.EnvironmentProvisioningProvisioning {
				t.Fatalf("record = %#v, want volume %q in provisioning", record, test.want)
			}
			if err := testhierarchy.ValidateEnvironmentVolumeDir(
				environmentpath.DefaultVolumeRoot,
				test.project,
				record,
			); err != nil {
				t.Fatalf("ValidateEnvironmentVolumeDir() error = %v", err)
			}
		})
	}
}

func TestEnvironmentRecordRejectsSlugOrMismatchedVolumePath(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	record := testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ID:                ids.NewAt(ids.KindEnvironment, createdAt, 5),
		ProjectID:         ids.NewAt(ids.KindProject, createdAt, 6),
		Name:              "Production",
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, createdAt, 7),
		CreatedAt:         createdAt,
	}
	for _, volumeDir := range []string{
		"/var/lib/groundplane/vol/tenant-slug/project-slug/" + record.ID,
		"/var/lib/groundplane/vol/platform/" + record.ProjectID + "/wrong-environment",
		"relative/platform/" + record.ProjectID + "/" + record.ID,
	} {
		record.VolumeDir = volumeDir
		if err := testhierarchy.ValidateEnvironment(record); err == nil {
			t.Fatalf("validateEnvironment() accepted %q", volumeDir)
		}
	}
}

func TestEnvironmentProvisioningTransitionsPreserveIdentity(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	originalTask := ids.NewAt(ids.KindTask, createdAt, 8)
	retryTask := ids.NewAt(ids.KindTask, createdAt, 9)
	record := testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ID: ids.NewAt(ids.KindEnvironment, createdAt, 10), VolumeDir: "/stable/path",
		ProvisioningState: testhierarchy.EnvironmentProvisioningProvisioning, CreateTaskID: originalTask,
	}

	failed, err := testhierarchy.CompleteEnvironmentProvisioning(record, originalTask, false)
	if err != nil || failed.ProvisioningState != testhierarchy.EnvironmentProvisioningFailed {
		t.Fatalf("CompleteEnvironmentProvisioning(failed) = %#v, %v", failed, err)
	}
	retrying, err := testhierarchy.RetryEnvironmentProvisioning(failed, retryTask)
	if err != nil {
		t.Fatalf("RetryEnvironmentProvisioning() error = %v", err)
	}
	ready, err := testhierarchy.CompleteEnvironmentProvisioning(retrying, retryTask, true)
	if err != nil || ready.ProvisioningState != testhierarchy.EnvironmentProvisioningReady {
		t.Fatalf("CompleteEnvironmentProvisioning(ready) = %#v, %v", ready, err)
	}
	if ready.ID != record.ID || ready.VolumeDir != record.VolumeDir {
		t.Fatal("provisioning transitions changed stable identity")
	}
	if _, err := testhierarchy.CompleteEnvironmentProvisioning(retrying, originalTask, true); err == nil {
		t.Fatal("stale create Task completed a retried Environment")
	}
}
