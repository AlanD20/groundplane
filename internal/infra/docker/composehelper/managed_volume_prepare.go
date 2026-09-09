package composehelper

import (
	"context"
	"maps"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type ensuredVolume struct {
	Name       string            `yaml:"name"`
	Driver     string            `yaml:"driver"`
	External   bool              `yaml:"external"`
	DriverOpts map[string]string `yaml:"driver_opts"`
	Labels     map[string]string `yaml:"labels"`
}

func executeManagedVolumeEnsure(
	ctx context.Context,
	taskRunner runner.Runner,
	timeout uint32,
	artifact *agentpb.ComposeArtifact,
	ensure *agentpb.ManagedVolumeEnsure,
) (*agentpb.ComposeHelperResponse, error) {
	var selected *agentpb.ComposeVolume
	for _, volume := range artifact.GetVolumes() {
		if volume.GetVolumeId() == ensure.GetVolumeId() {
			selected = volume
			break
		}
	}
	if selected == nil {
		return nil, errs.New(errs.KindValidationFailed, "managed volume preparation selection is absent")
	}
	var document struct {
		Volumes map[string]ensuredVolume `yaml:"volumes"`
	}
	if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &document); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	volume, found := document.Volumes[selected.GetComposeName()]
	options := map[string]string{
		"type":   "none",
		"o":      "bind",
		"device": filepath.Join(artifact.GetAuthorizedVolumeDir(), selected.GetComposeName()),
	}
	if !found || volume.External || volume.Name != selected.GetDockerName() || volume.Driver != "local" ||
		!maps.Equal(volume.DriverOpts, options) {
		return nil, errs.New(errs.KindValidationFailed, "managed volume rendered bind differs from sealed authority")
	}
	for _, label := range selected.GetExpectedLabels() {
		if volume.Labels[label.GetKey()] != label.GetValue() {
			return nil, errs.New(
				errs.KindValidationFailed,
				"managed volume rendered ownership differs from sealed authority",
			)
		}
	}
	for key := range volume.Labels {
		if strings.HasPrefix(key, "com.docker.compose.") {
			return nil, errs.New(errs.KindValidationFailed, "managed volume contains reserved Compose labels")
		}
	}
	owned := proto.CloneOf(artifact)
	owned.Volumes = []*agentpb.ComposeVolume{proto.CloneOf(selected)}
	owned.Volumes[0].ExpectedLabels = nil
	for key, value := range volume.Labels {
		owned.Volumes[0].ExpectedLabels = append(
			owned.Volumes[0].ExpectedLabels,
			&agentpb.LabelPair{Key: key, Value: value},
		)
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	if ensure.RequireExisting {
		inspection, exists, failure, err := inspectManagedComposeVolume(executionCtx, taskRunner, selected.DockerName)
		if err != nil || failure != nil {
			return failure, err
		}
		if !exists || !managedVolumeMatches(inspection, owned.Volumes[0], options["device"], artifact.ProjectName) {
			return failedResponse(1), nil
		}
		return completedResponse(), nil
	}
	response, err := ensureManagedComposeVolumes(executionCtx, taskRunner, owned)
	if err != nil || response != nil {
		return response, err
	}
	return completedResponse(), nil
}
