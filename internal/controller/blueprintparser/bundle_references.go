package blueprintparser

import (
	"github.com/AlanD20/groundplane/internal/core"
	composepaths "github.com/compose-spec/compose-go/v2/paths"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

func (p *parsePlan) requireReference(
	baseDir string,
	reference string,
	allowDirectory bool,
	allowMissing bool,
) (string, bool, error) {
	if reference == p.emptyEnv {
		return reference, true, nil
	}
	if reference == "" || strings.Contains(reference, `\`) || strings.HasPrefix(reference, "~") ||
		path.IsAbs(reference) || filepath.IsAbs(reference) || composepaths.IsWindowsAbs(reference) {
		return "", false, validationError("blueprint file reference must be bundle-relative")
	}
	parsed, err := url.Parse(reference)
	if err != nil || parsed.Scheme != "" || strings.HasPrefix(reference, "git@") {
		return "", false, validationError("blueprint remote file reference is forbidden")
	}
	for _, segment := range strings.Split(reference, "/") {
		if segment == ".." {
			return "", false, validationError("blueprint file reference contains traversal")
		}
	}

	resolved := path.Clean(path.Join(baseDir, path.Clean(reference)))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return "", false, validationError("blueprint file reference escapes the bundle")
	}
	if _, exists := p.files[resolved]; exists {
		p.referenced[resolved] = struct{}{}
		return resolved, true, nil
	}
	if allowDirectory {
		prefix := ""
		if resolved != "." {
			prefix = strings.TrimSuffix(resolved, "/") + "/"
		}
		found := false
		for filename := range p.files {
			if prefix == "" || strings.HasPrefix(filename, prefix) {
				p.referenced[filename] = struct{}{}
				found = true
			}
		}
		if found || resolved == baseDir {
			p.directories[resolved] = struct{}{}
			return resolved, true, nil
		}
	}
	if allowMissing {
		return resolved, false, nil
	}
	return "", false, validationError("blueprint file reference is undeclared")
}

func (p *parsePlan) requireRuntimeReference(baseDir string, reference string) error {
	resolved, _, err := p.requireReference(baseDir, reference, true, false)
	if err != nil {
		return err
	}
	if _, exists := p.files[resolved]; exists {
		p.runtime[resolved] = struct{}{}
		return nil
	}
	prefix := ""
	if resolved != "." {
		prefix = strings.TrimSuffix(resolved, "/") + "/"
	}
	for filename := range p.files {
		if prefix == "" || strings.HasPrefix(filename, prefix) {
			p.runtime[filename] = struct{}{}
		}
	}
	return nil
}

func (p *parsePlan) runtimeBlueprintFiles() []core.BlueprintFile {
	files := make([]core.BlueprintFile, 0, len(p.runtime))
	for filename := range p.runtime {
		files = append(files, core.BlueprintFile{
			Path: filename, Content: append([]byte(nil), p.files[filename]...),
		})
	}
	sort.Slice(files, func(left int, right int) bool { return files[left].Path < files[right].Path })
	return files
}
