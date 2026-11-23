//go:build linux

package monitoringreader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/volume"
)

func TestMonitoringMeasuresOwnedBindVolumeWithoutDockerUsageData(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root-owned Environment volume root")
	}
	reader, engine, request := sampleFixture(t)
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	reader.volumeRoot = t.TempDir()
	if err := os.Chmod(reader.volumeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	scope := environmentpath.Scope{ProjectKind: environmentpath.ProjectKindTenant,
		TenantID: "tnt_" + suffix, ProjectID: "prj_" + suffix, EnvironmentID: "env_" + suffix}
	directory, err := environmentpath.Derive(reader.volumeRoot, scope)
	if err != nil {
		t.Fatal(err)
	}
	device := filepath.Join(directory, "data")
	if err := os.MkdirAll(device, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(device, "sentinel"), []byte("test payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := &agentpb.MonitoringVolumeTarget{
		VolumeId:       "vol_" + suffix,
		EnvironmentId:  scope.EnvironmentID,
		ComposeProject: "gp-env_" + strings.ToLower(suffix),
		VolumeKey:      "data",
		DockerName:     "gp_vol_vol_" + strings.ToLower(suffix),
	}
	request.IncludeStorage = true
	request.Volumes = []*agentpb.MonitoringVolumeTarget{target}
	engine.volume = volume.Volume{
		Name: target.DockerName, Driver: "local", Options: map[string]string{"device": device, "type": "none", "o": "bind"},
		Labels: map[string]string{
			"com.groundplane.managed": "true", "com.groundplane.kind": "volume",
			"com.groundplane.tenant-id": scope.TenantID, "com.groundplane.project-id": scope.ProjectID,
			"com.groundplane.environment-id": scope.EnvironmentID,
			"com.docker.compose.project":     target.ComposeProject, "com.docker.compose.volume": target.VolumeKey,
		},
	}
	measured := reader.sampleVolumes(t.Context(), request)
	if len(measured) != 1 || measured[0].UsedBytes == nil || *measured[0].UsedBytes != 12 {
		t.Fatalf("volume measurement = %v; want 12 bytes", measured)
	}
	engine.volume.Labels["com.groundplane.project-id"] = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if got := reader.sampleVolumes(t.Context(), request); got[0].UsedBytes != nil {
		t.Fatalf("misattributed volume reported %v", got)
	}
}
