package swarmgit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func sanitizedGitEnvironment(overrides []string) ([]string, error) {
	result := controlledEnvironment([]string{"GNUPGHOME"})
	allowedOverrides := map[string]struct{}{
		"GIT_AUTHOR_DATE":     {},
		"GIT_AUTHOR_EMAIL":    {},
		"GIT_AUTHOR_NAME":     {},
		"GIT_COMMITTER_DATE":  {},
		"GIT_COMMITTER_EMAIL": {},
		"GIT_COMMITTER_NAME":  {},
	}
	for _, value := range overrides {
		key, _, found := strings.Cut(value, "=")
		if !found {
			return nil, invalid("Git environment override is malformed")
		}
		if _, allowed := allowedOverrides[key]; !allowed {
			return nil, invalid(fmt.Sprintf("Git environment override %s is forbidden", key))
		}
		result = append(result, value)
	}
	return append(result, "GIT_CONFIG_NOSYSTEM=1", "GIT_NO_REPLACE_OBJECTS=1"), nil
}

func controlledEnvironment(passKeys []string) []string {
	result := []string{
		"PATH=" + trustedToolchainPath(),
		"LANG=C",
		"LC_ALL=C",
		"TMPDIR=/tmp",
	}
	for _, key := range passKeys {
		if value, exists := os.LookupEnv(key); exists && value != "" {
			result = append(result, key+"="+value)
		}
	}
	return result
}

func trustedToolchainPath() string {
	return "/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin"
}

func trustedGateToolchainPath(root string) string {
	result := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	var gopathBins []string
	for _, value := range filepath.SplitList(os.Getenv("GOPATH")) {
		if value != "" {
			gopathBins = append(gopathBins, filepath.Join(value, "bin"))
		}
	}
	home := os.Getenv("HOME")
	if len(gopathBins) == 0 && home != "" {
		gopathBins = append(gopathBins, filepath.Join(home, "go", "bin"))
	}
	miseRoot := os.Getenv("MISE_DATA_DIR")
	if miseRoot == "" && home != "" {
		miseRoot = filepath.Join(home, ".local", "share", "mise")
	}
	nodeVersion := pinnedNodeVersion(root)
	var nodeBins []string
	if nodeVersion != "" {
		if miseRoot != "" {
			nodeBins = append(
				nodeBins,
				filepath.Join(miseRoot, "installs", "node", nodeVersion, "bin"),
			)
		}
		if home != "" {
			nodeBins = append(
				nodeBins,
				filepath.Join(home, ".local", "share", "nvm", "versions", "node", "v"+nodeVersion, "bin"),
			)
		}
	}
	allowedDynamic := func(value string) bool {
		for _, gopathBin := range gopathBins {
			if value == gopathBin {
				return true
			}
		}
		for _, nodeBin := range nodeBins {
			if value == nodeBin {
				return true
			}
		}
		return false
	}
	fixed := []string{"/usr/local/go/bin", "/usr/local/bin", "/usr/bin", "/bin"}
	candidates := append(append([]string(nil), fixed...), gopathBins...)
	candidates = append(candidates, nodeBins...)
	candidates = append(candidates, filepath.SplitList(os.Getenv("PATH"))...)
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		canonicalRoot = filepath.Clean(root)
	}
	for _, value := range candidates {
		isFixed := false
		for _, fixedPath := range fixed {
			if value == fixedPath {
				isFixed = true
				break
			}
		}
		if !filepath.IsAbs(value) || filepath.Clean(value) != value ||
			!isFixed && !allowedDynamic(value) || filesystemPathWithin(value, root) {
			continue
		}
		canonical, err := filepath.EvalSymlinks(value)
		if err != nil || !filepath.IsAbs(canonical) || filepath.Clean(canonical) != canonical ||
			filesystemPathWithin(canonical, canonicalRoot) {
			continue
		}
		info, err := os.Stat(canonical)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, exists := seen[canonical]; exists {
			continue
		}
		seen[canonical] = struct{}{}
		result = append(result, canonical)
	}
	return strings.Join(result, string(os.PathListSeparator))
}

func pinnedNodeVersion(root string) string {
	data, err := os.ReadFile(filepath.Join(root, ".node-version"))
	if err != nil {
		return ""
	}
	value := strings.TrimSpace(string(data))
	if value == "" || len(value) > 32 {
		return ""
	}
	for _, character := range value {
		if character != '.' && (character < '0' || character > '9') {
			return ""
		}
	}
	return value
}

func filesystemPathWithin(value, root string) bool {
	relative, err := filepath.Rel(root, value)
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
