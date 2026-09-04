package scriptrunner

import (
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateOwnedContainer(
	inspected client.ContainerInspectResult,
	containerID string,
	request scriptexecution.Request,
	prepared preparedBody,
) error {
	value := inspected.Container
	projection := request.Projection
	if !validDockerContainerID(containerID) || value.ID != containerID ||
		value.Name != "/"+projection.Name || value.Config == nil || value.HostConfig == nil || value.State == nil ||
		value.Config.Image != projection.Image ||
		value.Config.User != strconv.FormatUint(uint64(projection.Uid), 10)+":"+strconv.FormatUint(uint64(projection.Gid), 10) ||
		!slices.Equal([]string(value.Config.Entrypoint), projection.Entrypoint) ||
		!slices.Equal([]string(value.Config.Cmd), projection.Command) ||
		!hasExactSealedEnvironment(value.Config.Env, scriptEnvironment(projection.Environment, request.Entries)) ||
		value.Config.WorkingDir != projection.WorkingDir ||
		value.HostConfig.LogConfig.Type != "none" ||
		!hasExactGroundplaneLabels(value.Config.Labels, pairMap(projection.Labels)) {
		return errs.New(errs.KindStateConflict, "Script runner: captured container ownership evidence does not match")
	}
	bodyMounts := 0
	for _, value := range value.Mounts {
		if value.Destination != bodyTarget {
			continue
		}
		bodyMounts++
		if value.Type != mount.TypeBind || value.Source != prepared.hostPath || value.RW {
			return errs.New(errs.KindStateConflict, "Script runner: captured container body mount does not match")
		}
	}
	if bodyMounts != 1 {
		return errs.New(errs.KindStateConflict, "Script runner: captured container body mount is missing or ambiguous")
	}
	for _, entry := range request.Entries {
		if entry.Binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		expectedSource := filepath.Join(filepath.Dir(prepared.hostPath), entryArtifactLeaf(entry.Binding))
		matches := 0
		for _, mounted := range value.Mounts {
			if mounted.Destination == entry.Binding.FileTarget {
				matches++
				if mounted.Type != mount.TypeBind || mounted.Source != expectedSource || mounted.RW {
					return errs.New(errs.KindStateConflict, "Script runner: captured container Entry mount does not match")
				}
			}
		}
		if matches != 1 {
			return errs.New(errs.KindStateConflict, "Script runner: captured container Entry mount is missing or ambiguous")
		}
	}
	return nil
}

func hasExactSealedEnvironment(effective, sealed []string) bool {
	expected := make(map[string]string, len(sealed))
	for _, item := range sealed {
		key, value, found := strings.Cut(item, "=")
		if !found || key == "" {
			return false
		}
		if _, exists := expected[key]; exists {
			return false
		}
		expected[key] = value
	}

	seen := make(map[string]int, len(expected))
	for _, item := range effective {
		key, value, found := strings.Cut(item, "=")
		want, owned := expected[key]
		if !owned {
			continue
		}
		if !found || value != want {
			return false
		}
		seen[key]++
		if seen[key] != 1 {
			return false
		}
	}
	for key := range expected {
		if seen[key] != 1 {
			return false
		}
	}
	return true
}

func hasExactGroundplaneLabels(effective, sealed map[string]string) bool {
	owned := make(map[string]string, len(sealed))
	for key, value := range effective {
		if strings.HasPrefix(key, "com.groundplane.") {
			owned[key] = value
		}
	}
	return maps.Equal(owned, sealed)
}
