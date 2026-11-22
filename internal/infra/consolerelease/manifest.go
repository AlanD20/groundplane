// Package consolerelease owns verified, immutable Console bundles and their
// active selection. It never serves an arbitrary host directory.
package consolerelease

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	manifestName       = "console.json"
	manifestSchema     = 1
	maximumFiles       = 2048
	maximumFileBytes   = 16 << 20
	maximumBundleBytes = 64 << 20
)

var (
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	releasePattern = regexp.MustCompile(
		`^console/(?:v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)|ref/[0-9a-f]{40})$`,
	)
)

type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Schema       int       `json:"schema"`
	Release      string    `json:"release"`
	SourceCommit string    `json:"source_commit"`
	APISHA256    string    `json:"api_sha256"`
	CreatedAt    time.Time `json:"created_at"`
	Files        []File    `json:"files"`
}

type Artifact struct {
	Release        string `json:"release"`
	BundleSHA256   string `json:"bundle_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
	APISHA256      string `json:"api_sha256"`
}

func (artifact Artifact) Valid() bool {
	return releasePattern.MatchString(artifact.Release) && digestPattern.MatchString(artifact.BundleSHA256) &&
		digestPattern.MatchString(artifact.ManifestSHA256) && digestPattern.MatchString(artifact.APISHA256)
}

func APIHash(document []byte) string { return digest(document) }

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func encodeManifest(manifest Manifest) ([]byte, string, error) {
	if err := manifest.validate(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, "", err
	}
	return canonical, digest(canonical), nil
}

func decodeManifest(value []byte) (Manifest, string, error) {
	if len(value) == 0 || len(value) > 512<<10 {
		return Manifest{}, "", errs.New(errs.KindStateConflict, "Console manifest is empty or oversized")
	}
	manifest, err := jcs.Decode[Manifest](value)
	if err != nil {
		return Manifest{}, "", errs.New(errs.KindStateConflict, "Console manifest is invalid")
	}
	canonical, identity, err := encodeManifest(manifest)
	if err != nil || !slices.Equal(canonical, value) {
		return Manifest{}, "", errs.New(errs.KindStateConflict, "Console manifest is not canonical")
	}
	return manifest, identity, nil
}

func (manifest Manifest) validate() error {
	if manifest.Schema != manifestSchema || !releasePattern.MatchString(manifest.Release) ||
		!digestPattern.MatchString(manifest.APISHA256) || !commitPattern(manifest.SourceCommit) ||
		manifest.CreatedAt.Before(time.Unix(0, 0)) || manifest.CreatedAt.Nanosecond() != 0 ||
		len(manifest.Files) == 0 || len(manifest.Files) > maximumFiles {
		return errs.New(errs.KindValidationFailed, "Console manifest authority is invalid")
	}
	seen, hasIndex := make(map[string]struct{}, len(manifest.Files)), false
	var total int64
	previous := ""
	for _, file := range manifest.Files {
		if !validAssetPath(file.Path) || file.Size <= 0 || file.Size > maximumFileBytes ||
			!digestPattern.MatchString(file.SHA256) || file.Path <= previous {
			return errs.New(errs.KindValidationFailed, "Console manifest file inventory is invalid")
		}
		if _, duplicate := seen[file.Path]; duplicate {
			return errs.New(errs.KindValidationFailed, "Console manifest repeats a file")
		}
		seen[file.Path], previous = struct{}{}, file.Path
		hasIndex = hasIndex || file.Path == "index.html"
		total += file.Size
		if total > maximumBundleBytes {
			return errs.New(errs.KindValidationFailed, "Console bundle exceeds its unpacked bound")
		}
	}
	if !hasIndex {
		return errs.New(errs.KindValidationFailed, "Console manifest has no index")
	}
	return nil
}

func validAssetPath(name string) bool {
	if name == "" || len(name) > 512 || !utf8.ValidString(name) || name == manifestName || !fs.ValidPath(name) ||
		path.Clean(name) != name || strings.ContainsAny(name, "\\\x00") {
		return false
	}
	for _, element := range strings.Split(name, "/") {
		if element == "" || strings.HasPrefix(element, ".") || strings.HasPrefix(element, "_") {
			return false
		}
	}
	return true
}

func commitPattern(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}
