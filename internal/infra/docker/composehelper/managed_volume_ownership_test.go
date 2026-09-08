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

// Rationale: a pre-hook volume must have both Groundplane and Compose ownership
// before a Script can mount it; otherwise the observer rejects startup.
func TestManagedVolumePreparationCreatesObservableOwnedBind(t *testing.T) {
	artifact := volumePreparationArtifact()
	fake := &volumePreparationRunner{}
	response, err := ensureManagedComposeVolumes(t.Context(), fake, artifact)
	if err != nil || response != nil {
		t.Fatalf("prepare: response=%v error=%v", response, err)
	}
	actual := fake.volume
	if actual.Labels["com.docker.compose.project"] != artifact.ProjectName ||
		actual.Labels["com.docker.compose.volume"] != "data" ||
		actual.Labels["com.groundplane.managed"] != "true" ||
		actual.Options["device"] != artifact.AuthorizedVolumeDir+"/data" ||
		actual.Options["type"] != "none" || actual.Options["o"] != "bind" {
		t.Fatalf("volume is not an observable owned managed bind: %+v", actual)
	}
	if len(artifact.Volumes[0].ExpectedLabels) != 1 {
		t.Fatal("preparation mutated frozen artifact labels")
	}
}

// Rationale: existing foreign or Script-created volumes must never be adopted
// by silently supplying missing Compose labels after data already exists.
func TestManagedVolumePreparationRejectsForeignComposeOwnership(t *testing.T) {
	for _, label := range []string{"com.docker.compose.project", "com.docker.compose.volume"} {
		t.Run(label, func(t *testing.T) {
			artifact := volumePreparationArtifact()
			fake := &volumePreparationRunner{exists: true, volume: managedVolumeInspection{
				Name: artifact.Volumes[0].DockerName, Driver: "local",
				Labels: map[string]string{"com.groundplane.managed": "true",
					"com.docker.compose.project": artifact.ProjectName, "com.docker.compose.volume": "data"},
				Options: map[string]string{
					"device": artifact.AuthorizedVolumeDir + "/data",
					"o":      "bind",
					"type":   "none",
				},
			}}
			delete(fake.volume.Labels, label)
			response, err := ensureManagedComposeVolumes(t.Context(), fake, artifact)
			if err == nil && response == nil {
				t.Fatal("accepted missing Compose ownership")
			}
			if fake.creates != 0 {
				t.Fatal("existing volume was mutated")
			}
		})
	}
}

func volumePreparationArtifact() *agentpb.ComposeArtifact {
	return &agentpb.ComposeArtifact{ProjectName: "gp-environment", AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Volumes: []*agentpb.ComposeVolume{{ComposeName: "data", DockerName: "gp_vol_test",
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.managed", Value: "true"}}}}}
}

type volumePreparationRunner struct {
	volume  managedVolumeInspection
	exists  bool
	creates int
}

func (fake *volumePreparationRunner) Run(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
	if len(opts.Args) < 2 || opts.Args[0] != "volume" {
		return runner.Result{}, errs.New(errs.KindInternal, "unexpected non-volume command")
	}
	switch opts.Args[1] {
	case "inspect":
		if !fake.exists {
			return runner.Result{ExitCode: 1}, errs.New(errs.KindStateConflict, "absent volume")
		}
		encoded, err := json.Marshal(fake.volume)
		return runner.Result{Stdout: encoded}, err
	case "create":
		fake.creates++
		fake.exists = true
		fake.volume = managedVolumeInspection{Name: opts.Args[len(opts.Args)-1], Driver: "local",
			Labels: map[string]string{}, Options: map[string]string{}}
		for index := 2; index < len(opts.Args)-1; index++ {
			if opts.Args[index] != "--label" && opts.Args[index] != "--opt" {
				continue
			}
			key, value, ok := strings.Cut(opts.Args[index+1], "=")
			if !ok {
				return runner.Result{}, errs.New(errs.KindInternal, "invalid volume option")
			}
			if opts.Args[index] == "--label" {
				fake.volume.Labels[key] = value
			} else {
				fake.volume.Options[key] = value
			}
			index++
		}
		return runner.Result{}, nil
	default:
		return runner.Result{}, errs.New(errs.KindInternal, "unexpected volume mutation")
	}
}

func (fake *volumePreparationRunner) Stream(
	ctx context.Context,
	opts runner.RunCmdOpts,
	_ func(bool, string),
) (runner.Result, error) {
	return fake.Run(ctx, opts)
}
