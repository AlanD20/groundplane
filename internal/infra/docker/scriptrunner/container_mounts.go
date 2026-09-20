package scriptrunner

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
	"path/filepath"
	"strings"
)

func dockerMounts(
	values []*agentpb.ScriptRunnerMount,
	bodyPath string,
	entries []*agentpb.ScriptEntryArtifact,
) ([]mount.Mount, error) {
	result := make([]mount.Mount, 0, len(values)+len(entries)+1)
	result = append(result, mount.Mount{
		Type: mount.TypeBind, Source: bodyPath, Target: bodyTarget, ReadOnly: true,
		BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
	})
	for _, value := range values {
		item := value.RenderedMount
		if item == nil || item.Type != "volume" {
			return nil, errs.New(errs.KindInternal, "Script runner: unsupported sealed mount")
		}
		result = append(result, mount.Mount{
			Type: mount.TypeVolume, Source: item.Source, Target: item.Target, ReadOnly: item.ReadOnly,
			Consistency:   mount.Consistency(item.Consistency),
			VolumeOptions: &mount.VolumeOptions{NoCopy: item.VolumeNoCopy, Subpath: item.VolumeSubpath},
		})
	}
	for _, entry := range entries {
		if entry.Binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		result = append(result, mount.Mount{
			Type:        mount.TypeBind,
			Source:      filepath.Join(filepath.Dir(bodyPath), entryArtifactLeaf(entry.Binding)),
			Target:      entry.Binding.FileTarget,
			ReadOnly:    true,
			BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
		})
	}
	return result, nil
}

func tmpfsMap(values []string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		target, options, _ := strings.Cut(value, ":")
		result[target] = options
	}
	return result
}
