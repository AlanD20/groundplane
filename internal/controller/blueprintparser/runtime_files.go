package blueprintparser

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/types"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// SelectRuntimeFiles returns the exact immutable companion files referenced by
// the final normalized Compose project. Submitted bytes replace older bytes at
// the same path; unreferenced historical files are not carried forward.
func SelectRuntimeFiles(
	project *types.Project,
	submitted []core.BlueprintFile,
	previous []core.BlueprintFile,
) ([]core.BlueprintFile, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint runtime file project is missing")
	}
	if err := core.ValidateBlueprintFiles(submitted); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := core.ValidateNormalizedBlueprintFiles(previous); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	available := make(map[string]core.BlueprintFile, len(previous)+len(submitted))
	for _, file := range previous {
		available[file.Path] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	for _, file := range submitted {
		available[file.Path] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	selected := make(map[string]core.BlueprintFile)
	selectReference := func(reference string, tree bool, optional bool) error {
		reference = path.Clean(reference)
		if file, found := available[reference]; found {
			selected[reference] = file
			return nil
		}
		if tree {
			prefix := strings.TrimSuffix(reference, "/") + "/"
			matched := false
			for filename, file := range available {
				if strings.HasPrefix(filename, prefix) {
					selected[filename] = file
					matched = true
				}
			}
			if matched {
				return nil
			}
		}
		if optional {
			return nil
		}
		return errs.New(errs.KindInternal, "Blueprint normalized project references unavailable runtime files")
	}
	selectService := func(service types.ServiceConfig) error {
		for _, file := range service.EnvFiles {
			if err := selectReference(file.Path, false, !bool(file.Required)); err != nil {
				return err
			}
		}
		for _, file := range service.LabelFiles {
			if err := selectReference(file, false, false); err != nil {
				return err
			}
		}
		for _, volume := range service.Volumes {
			if volume.Type == types.VolumeTypeBind {
				if err := selectReference(volume.Source, true, false); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, service := range project.Services {
		if err := selectService(service); err != nil {
			return nil, err
		}
	}
	for _, service := range project.DisabledServices {
		if err := selectService(service); err != nil {
			return nil, err
		}
	}
	for _, config := range project.Configs {
		if config.File != "" {
			if err := selectReference(config.File, false, false); err != nil {
				return nil, err
			}
		}
	}
	for _, volume := range project.Volumes {
		if device := volume.DriverOpts["device"]; device != "" {
			if err := selectReference(device, true, false); err != nil {
				return nil, err
			}
		}
	}
	files := make([]core.BlueprintFile, 0, len(selected))
	for _, file := range selected {
		files = append(files, file)
	}
	sort.Slice(files, func(left int, right int) bool { return files[left].Path < files[right].Path })
	if err := core.ValidateNormalizedBlueprintFiles(files); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	return files, nil
}

func (p *parsePlan) materialize(workspace string) error {
	directories := make([]string, 0, len(p.directories))
	for directory := range p.directories {
		directories = append(directories, directory)
	}
	sort.Strings(directories)
	for _, directory := range directories {
		if err := os.MkdirAll(
			filepath.Join(workspace, filepath.FromSlash(directory)),
			0o700,
		); err != nil {
			return err
		}
	}
	files := make([]string, 0, len(p.referenced))
	for filename := range p.referenced {
		files = append(files, filename)
	}
	sort.Strings(files)
	for _, filename := range files {
		content := p.files[filename]
		if compose, ok := p.compose[filename]; ok {
			content = compose
		}
		fullPath := filepath.Join(workspace, filepath.FromSlash(filename))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(fullPath, content, 0o600); err != nil {
			return err
		}
	}
	return nil
}
