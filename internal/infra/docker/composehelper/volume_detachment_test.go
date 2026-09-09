package composehelper

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestManagedVolumeRemovalRequiresConsumerAbsence(t *testing.T) {
	for _, mode := range []string{"bind held", "named volume held", "inspection unavailable", "detached"} {
		t.Run(mode, func(t *testing.T) {
			plan := volumeEnsureRequest(t).Plan
			artifact, volume := plan.Artifacts[0], plan.Artifacts[0].Volumes[0]
			device := artifact.AuthorizedVolumeDir + "/" + volume.ComposeName
			removed := 0
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
				switch {
				case strings.HasPrefix(strings.Join(opts.Args, " "), "container ls "):
					return runner.Result{Stdout: []byte(strings.Repeat("a", 64))}, nil
				case opts.Args[0] == "container":
					if mode == "inspection unavailable" {
						return runner.Result{ExitCode: 1}, errs.New(errs.KindRequestFailed, "unavailable")
					}
					mounts := []struct{ Name, Source string }{}
					if mode == "bind held" {
						mounts = append(mounts, struct{ Name, Source string }{Source: device})
					}
					if mode == "named volume held" {
						mounts = append(mounts, struct{ Name, Source string }{Name: volume.DockerName})
					}
					encoded, err := json.Marshal(mounts)
					return runner.Result{Stdout: encoded}, err
				case opts.Args[1] == "ls":
					return runner.Result{Stdout: []byte(volume.DockerName)}, nil
				case opts.Args[1] == "inspect":
					labels := map[string]string{
						"com.docker.compose.project": artifact.ProjectName,
						"com.docker.compose.volume":  volume.ComposeName,
					}
					for _, label := range volume.ExpectedLabels {
						labels[label.Key] = label.Value
					}
					encoded, err := json.Marshal(
						managedVolumeInspection{Name: volume.DockerName, Driver: "local", Labels: labels,
							Options: map[string]string{"type": "none", "o": "bind", "device": device}},
					)
					return runner.Result{Stdout: encoded}, err
				case opts.Args[1] == "rm":
					removed++
					return runner.Result{}, nil
				default:
					t.Fatalf("unexpected Docker command: %v", opts.Args)
					return runner.Result{}, nil
				}
			}
			response, err := executeManagedVolumeRemove(context.Background(), fake, 30,
				&agentpb.ManagedVolumeRemove{VolumeId: volume.VolumeId, DockerName: volume.DockerName}, plan)
			if mode == "detached" {
				if err != nil || response.ExitCode != 0 || removed != 1 {
					t.Fatalf("detached control: %v", err)
				}
			} else if removed != 0 || (err == nil && response.ExitCode == 0) {
				t.Fatalf("unproved detachment authorized removal: mode=%s removed=%d error=%v", mode, removed, err)
			}
		})
	}
}
