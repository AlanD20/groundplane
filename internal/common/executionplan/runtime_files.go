package executionplan

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// RuntimeFileUse selects configuration sources, not volume data. Bind sources
// may be directories or non-configuration mounts; the source ledger determines
// which managed files they contain. Env/config/secret file references are exact.
type RuntimeFileUse struct {
	Destination string
	Bind        bool
}

// RuntimeConfigurationFiles reads captured Compose only. It does not interpolate
// variables, read env files, resolve host paths or consult desired state.
func RuntimeConfigurationFiles(ctx context.Context, artifact *agentpb.ComposeArtifact) ([]RuntimeFileUse, error) {
	if ctx == nil || artifact == nil || RejectUnknown(artifact) != nil ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		!path.IsAbs(
			artifact.AuthorizedVolumeDir,
		) || path.Clean(artifact.AuthorizedVolumeDir) != artifact.AuthorizedVolumeDir {
		return nil, invalidRuntimeFiles()
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !bytes.Equal(digest[:], artifact.YamlSha256) {
		return nil, invalidRuntimeFiles()
	}
	project, err := loadCapturedComposeProject(ctx, artifact)
	if err != nil {
		return nil, err
	}
	uses := make(map[string]bool)
	add := func(source string, bind bool) error {
		if source == "" || path.Clean(source) != source || strings.ContainsRune(source, 0) {
			return invalidRuntimeFiles()
		}
		destination, ok := strings.CutPrefix(source, artifact.AuthorizedVolumeDir+"/")
		if !ok || destination == "" || destination == "." || strings.HasPrefix(destination, "../") {
			return invalidRuntimeFiles()
		}
		prior, found := uses[destination]
		uses[destination] = bind && (!found || prior)
		return nil
	}
	for _, service := range project.Services {
		for _, file := range service.EnvFiles {
			if err := add(file.Path, false); err != nil {
				return nil, err
			}
		}
		for _, mount := range service.Volumes {
			if mount.Type == composetypes.VolumeTypeBind {
				if err := add(mount.Source, true); err != nil {
					return nil, err
				}
			}
		}
		for _, reference := range service.Configs {
			definition, found := project.Configs[reference.Source]
			if !found {
				return nil, invalidRuntimeFiles()
			}
			if definition.File != "" {
				if err := add(definition.File, false); err != nil {
					return nil, err
				}
			}
		}
		for _, reference := range service.Secrets {
			definition, found := project.Secrets[reference.Source]
			if !found {
				return nil, invalidRuntimeFiles()
			}
			if definition.File != "" {
				if err := add(definition.File, false); err != nil {
					return nil, err
				}
			}
		}
	}
	result := make([]RuntimeFileUse, 0, len(uses))
	for destination, bind := range uses {
		result = append(result, RuntimeFileUse{Destination: destination, Bind: bind})
	}
	slices.SortFunc(result, func(a, b RuntimeFileUse) int { return strings.Compare(a.Destination, b.Destination) })
	return result, nil
}

func invalidRuntimeFiles() error {
	return errs.New(errs.KindStateConflict, "Captured runtime configuration files are not confined to its Environment")
}
