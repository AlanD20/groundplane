package composehelper

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const managedEnsureVolumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestEnsureManagedComposeVolumesCreatesAndVerifiesMissingVolume(t *testing.T) {
	artifact, volume, device := managedEnsureFixture()
	fake := runner.NewFake()
	call := 0
	fake.RunFunc = func(context.Context, runner.RunCmdOpts) (runner.Result, error) {
		call++
		switch call {
		case 1:
			return runner.Result{ExitCode: 1}, errors.New("not found")
		case 2:
			return runner.Result{}, nil
		case 3:
			return runner.Result{Stdout: managedEnsureInspection(volume, device)}, nil
		default:
			t.Fatalf("unexpected Runner call %d", call)
			return runner.Result{}, nil
		}
	}
	failure, err := ensureManagedComposeVolumes(context.Background(), fake, artifact)
	if err != nil || failure != nil {
		t.Fatalf("ensureManagedComposeVolumes() = %#v, %v", failure, err)
	}
	wantCreate := []string{
		"volume", "create", "--driver", "local",
		"--opt", "type=none", "--opt", "o=bind", "--opt", "device=" + device,
		"--label", "com.docker.compose.project=gp-environment",
		"--label", "com.docker.compose.volume=app-data",
		"--label", "com.groundplane.kind=volume",
		"--label", "com.groundplane.managed=true",
		volume.DockerName,
	}
	if len(fake.Calls) != 3 || !reflect.DeepEqual(fake.Calls[1].Args, wantCreate) {
		t.Fatalf("Runner calls = %#v", fake.Calls)
	}
}

func TestEnsureManagedComposeVolumesRejectsForeignExistingVolume(t *testing.T) {
	artifact, volume, device := managedEnsureFixture()
	fake := runner.NewFake()
	fake.RunFunc = func(context.Context, runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{
			Stdout: []byte(
				`{"Name":"` + volume.DockerName + `","Driver":"local","Labels":{},"Options":{"type":"none","o":"bind","device":"` + device + `"}}`,
			),
		}, nil
	}
	failure, err := ensureManagedComposeVolumes(context.Background(), fake, artifact)
	if err != nil || failure == nil || failure.ExitCode != 1 || len(fake.Calls) != 1 {
		t.Fatalf("ensureManagedComposeVolumes() = %#v, %v; calls=%d", failure, err, len(fake.Calls))
	}
}

func managedEnsureFixture() (*agentpb.ComposeArtifact, *agentpb.ComposeVolume, string) {
	volume := &agentpb.ComposeVolume{
		VolumeId: managedEnsureVolumeID, ComposeName: "app-data",
		DockerName: "gp_vol_vol_01arz3ndektsv4rrffq69g5fav",
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.kind", Value: "volume"},
			{Key: "com.groundplane.managed", Value: "true"},
		},
	}
	directory := "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	return &agentpb.ComposeArtifact{
			ProjectName:         "gp-environment",
			AuthorizedVolumeDir: directory,
			Volumes:             []*agentpb.ComposeVolume{volume},
		},
		volume, filepath.Join(
			directory,
			volume.ComposeName,
		)
}

func managedEnsureInspection(volume *agentpb.ComposeVolume, device string) []byte {
	return []byte(
		`{"Name":"` + volume.DockerName + `","Driver":"local","Labels":{"com.groundplane.kind":"volume","com.groundplane.managed":"true","com.docker.compose.project":"gp-environment","com.docker.compose.volume":"app-data"},"Options":{"type":"none","o":"bind","device":"` + device + `"}}`,
	)
}
