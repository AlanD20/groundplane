package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
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
		project ProjectRecord
		want    string
	}{
		{
			name: "tenant",
			project: ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "renamable", Name: "Tenant project",
				Kind: ProjectKindTenant,
			},
			want: environmentpath.DefaultVolumeRoot + "/" + tenantID + "/" + projectID + "/" + environmentID,
		},
		{
			name: "backing",
			project: ProjectRecord{
				ID: projectID, Slug: "renamable", Name: "Backing project", Kind: ProjectKindBacking,
			},
			want: environmentpath.DefaultVolumeRoot + "/platform/" + projectID + "/" + environmentID,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			record, err := NewProvisioningEnvironment(
				environmentpath.DefaultVolumeRoot,
				test.project,
				environmentID,
				"production",
				taskID,
				createdAt,
			)
			if err != nil {
				t.Fatalf("NewProvisioningEnvironment() error = %v", err)
			}
			if record.VolumeDir != test.want || record.ProvisioningState != EnvironmentProvisioningProvisioning {
				t.Fatalf("record = %#v, want volume %q in provisioning", record, test.want)
			}
			if err := ValidateEnvironmentVolumeDir(environmentpath.DefaultVolumeRoot, test.project, record); err != nil {
				t.Fatalf("ValidateEnvironmentVolumeDir() error = %v", err)
			}
		})
	}
}

func TestEnvironmentRecordRejectsSlugOrMismatchedVolumePath(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	record := EnvironmentRecord{
		ID:                ids.NewAt(ids.KindEnvironment, createdAt, 5),
		ProjectID:         ids.NewAt(ids.KindProject, createdAt, 6),
		Name:              "Production",
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, createdAt, 7),
		CreatedAt:         createdAt,
	}
	for _, volumeDir := range []string{
		"/var/lib/groundplane/vol/tenant-slug/project-slug/" + record.ID,
		"/var/lib/groundplane/vol/platform/" + record.ProjectID + "/wrong-environment",
		"relative/platform/" + record.ProjectID + "/" + record.ID,
	} {
		record.VolumeDir = volumeDir
		if err := validateEnvironment(record); err == nil {
			t.Fatalf("validateEnvironment() accepted %q", volumeDir)
		}
	}
}

func TestEnvironmentProvisioningTransitionsPreserveIdentity(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	originalTask := ids.NewAt(ids.KindTask, createdAt, 8)
	retryTask := ids.NewAt(ids.KindTask, createdAt, 9)
	record := EnvironmentRecord{
		ID: ids.NewAt(ids.KindEnvironment, createdAt, 10), VolumeDir: "/stable/path",
		ProvisioningState: EnvironmentProvisioningProvisioning, CreateTaskID: originalTask,
	}

	failed, err := CompleteEnvironmentProvisioning(record, originalTask, false)
	if err != nil || failed.ProvisioningState != EnvironmentProvisioningFailed {
		t.Fatalf("CompleteEnvironmentProvisioning(failed) = %#v, %v", failed, err)
	}
	retrying, err := RetryEnvironmentProvisioning(failed, retryTask)
	if err != nil {
		t.Fatalf("RetryEnvironmentProvisioning() error = %v", err)
	}
	ready, err := CompleteEnvironmentProvisioning(retrying, retryTask, true)
	if err != nil || ready.ProvisioningState != EnvironmentProvisioningReady {
		t.Fatalf("CompleteEnvironmentProvisioning(ready) = %#v, %v", ready, err)
	}
	if ready.ID != record.ID || ready.VolumeDir != record.VolumeDir {
		t.Fatal("provisioning transitions changed stable identity")
	}
	if _, err := CompleteEnvironmentProvisioning(retrying, originalTask, true); err == nil {
		t.Fatal("stale create Task completed a retried Environment")
	}
}
