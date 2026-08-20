package core

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	BlueprintBundleMaxFiles      = 64
	BlueprintBundleMaxFileBytes  = 256 * 1024
	BlueprintBundleMaxTotalBytes = 768 * 1024
	BlueprintBundleMaxPathBytes  = 240
)

var interpolationKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// BlueprintBundle is the closed logical input accepted by the environment
// parser after multipart integrity has been verified. Files are strictly
// path-sorted; ComposeSources is layer-ordered and begins with RootPath.
type BlueprintBundle struct {
	RootPath       string
	ComposeSources []string
	Files          []BlueprintFile
	Interpolation  map[string]string
}

// BlueprintFile is one declared bundle member. Content may be YAML or a
// native Compose companion file; secret bytes are forbidden by the API
// contract before this boundary.
type BlueprintFile struct {
	Path    string
	Content []byte
}

func (b BlueprintBundle) Validate() error {
	if len(b.Files) == 0 {
		return fmt.Errorf("blueprint bundle: at least one file is required")
	}
	if len(b.Files) > BlueprintBundleMaxFiles {
		return fmt.Errorf("blueprint bundle: file count %d exceeds limit %d", len(b.Files), BlueprintBundleMaxFiles)
	}
	if err := validateBundlePath(b.RootPath); err != nil {
		return fmt.Errorf("blueprint bundle: root path: %w", err)
	}
	if len(b.ComposeSources) == 0 || b.ComposeSources[0] != b.RootPath {
		return fmt.Errorf("blueprint bundle: Compose sources must begin with root path %q", b.RootPath)
	}

	filePaths := make(map[string]struct{}, len(b.Files))
	totalBytes := 0
	previousPath := ""
	for index, file := range b.Files {
		if err := validateBundlePath(file.Path); err != nil {
			return fmt.Errorf("blueprint bundle: file %d path: %w", index, err)
		}
		if index > 0 && file.Path <= previousPath {
			return fmt.Errorf("blueprint bundle: files must be unique and sorted by path")
		}
		if len(file.Content) > BlueprintBundleMaxFileBytes {
			return fmt.Errorf(
				"blueprint bundle: file %q size %d exceeds limit %d",
				file.Path,
				len(file.Content),
				BlueprintBundleMaxFileBytes,
			)
		}
		totalBytes += len(file.Content)
		if totalBytes > BlueprintBundleMaxTotalBytes {
			return fmt.Errorf(
				"blueprint bundle: total size %d exceeds limit %d",
				totalBytes,
				BlueprintBundleMaxTotalBytes,
			)
		}
		filePaths[file.Path] = struct{}{}
		previousPath = file.Path
	}

	seenSources := make(map[string]struct{}, len(b.ComposeSources))
	for index, source := range b.ComposeSources {
		if err := validateBundlePath(source); err != nil {
			return fmt.Errorf("blueprint bundle: Compose source %d: %w", index, err)
		}
		if _, exists := filePaths[source]; !exists {
			return fmt.Errorf("blueprint bundle: Compose source %q is not a declared file", source)
		}
		if _, exists := seenSources[source]; exists {
			return fmt.Errorf("blueprint bundle: duplicate Compose source %q", source)
		}
		seenSources[source] = struct{}{}
	}

	keys := make([]string, 0, len(b.Interpolation))
	for key := range b.Interpolation {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := b.Interpolation[key]
		if !interpolationKeyPattern.MatchString(key) {
			return fmt.Errorf("blueprint bundle: interpolation key %q is invalid", key)
		}
		if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("blueprint bundle: interpolation value for %q must be valid NUL-free UTF-8", key)
		}
	}
	return nil
}

// File returns a declared file without copying its immutable input bytes.
func (b BlueprintBundle) File(name string) (BlueprintFile, bool) {
	index := sort.Search(len(b.Files), func(index int) bool {
		return b.Files[index].Path >= name
	})
	if index == len(b.Files) || b.Files[index].Path != name {
		return BlueprintFile{}, false
	}
	return b.Files[index], true
}

func validateBundlePath(value string) error {
	if value == "" {
		return fmt.Errorf("path is required")
	}
	if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("path must be valid NUL-free UTF-8")
	}
	if len(value) > BlueprintBundleMaxPathBytes {
		return fmt.Errorf("path length %d exceeds limit %d", len(value), BlueprintBundleMaxPathBytes)
	}
	if strings.Contains(value, `\`) || strings.HasPrefix(value, "/") {
		return fmt.Errorf("path must be a relative slash-separated path")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return fmt.Errorf("path must be normalized and traversal-free")
	}
	return nil
}
