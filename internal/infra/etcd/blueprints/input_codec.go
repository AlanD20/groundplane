package blueprints

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"path"
	"strings"
	"time"
	"unicode/utf8"
)

func encodeEnvironmentBlueprintManifest(revision EnvironmentBlueprintRevision) ([]byte, error) {
	if err := ValidateEnvironmentBlueprintRevision(revision); err != nil {
		return nil, err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation:  cloneEnvironmentBlueprintInterpolation(revision.Interpolation),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return recordcodec.Encode("environment-blueprint-revision", manifest)
}

func decodeEnvironmentBlueprintManifest(value []byte) (environmentBlueprintManifest, error) {
	manifest, err := recordcodec.Decode[environmentBlueprintManifest](
		value,
		"environment-blueprint-revision",
	)
	if err != nil {
		return environmentBlueprintManifest{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil || manifest.CreatedAt != createdAt.UTC().Format(time.RFC3339Nano) {
		return environmentBlueprintManifest{}, CorruptEnvironmentBlueprint()
	}
	manifest.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	if err := validateEnvironmentBlueprintManifest(manifest); err != nil {
		return environmentBlueprintManifest{}, CorruptEnvironmentBlueprint()
	}
	return manifest, nil
}

func ValidateEnvironmentBlueprintRevision(revision EnvironmentBlueprintRevision) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, revision.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revision.RevisionID); err != nil {
		return err
	}
	if err := recordcodec.ValidateTimestamp("Blueprint revision created_at", revision.CreatedAt); err != nil {
		return err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: revision.ComposeSources,
		Interpolation:  revision.Interpolation,
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return validateEnvironmentBlueprintManifest(manifest)
}

func validateEnvironmentBlueprintManifest(manifest environmentBlueprintManifest) error {
	if recordcodec.ValidateID(ids.KindEnvironment, manifest.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, manifest.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint revision identity is invalid")
	}
	if err := validateEnvironmentBlueprintPath(manifest.RootPath); err != nil {
		return err
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > EnvironmentBlueprintMaxFiles ||
		len(manifest.ComposeSources) == 0 || manifest.ComposeSources[0] != manifest.RootPath {
		return errs.New(errs.KindValidationFailed, "Blueprint revision file namespace is invalid")
	}
	declared := make(map[string]struct{}, len(manifest.Files))
	totalBytes := 0
	previous := ""
	for index, file := range manifest.Files {
		if validateEnvironmentBlueprintPath(file.Path) != nil ||
			(index > 0 && file.Path <= previous) ||
			file.Size < 0 ||
			file.Size > EnvironmentBlueprintMaxFileBytes ||
			len(file.SHA256) != sha256.Size*2 {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision file metadata is invalid",
			)
		}
		decoded, err := hex.DecodeString(file.SHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != file.SHA256 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file digest is invalid")
		}
		totalBytes += file.Size
		if totalBytes > environmentBlueprintMaxTotalBytes {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision exceeds its total size limit",
			)
		}
		declared[file.Path] = struct{}{}
		previous = file.Path
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if validateEnvironmentBlueprintPath(source) != nil {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is invalid",
			)
		}
		if _, exists := declared[source]; !exists {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is undeclared",
			)
		}
		if _, duplicate := seenSources[source]; duplicate {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose sources are duplicated",
			)
		}
		seenSources[source] = struct{}{}
	}
	for key, value := range manifest.Interpolation {
		if !environmentBlueprintInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision interpolation is invalid",
			)
		}
	}
	return nil
}

func validateEnvironmentBlueprintPath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(
			value,
		) > EnvironmentBlueprintMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	return nil
}

func cloneEnvironmentBlueprintInterpolation(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func CorruptEnvironmentBlueprint() error {
	return errs.New(errs.KindInternal, "Environment Blueprint revision is corrupt")
}
