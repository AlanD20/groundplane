package composehelper

import (
	"encoding/json"
	"path/filepath"
	"strings"

	component "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type ComponentActionRecipe struct {
	relativePath   string
	containerPath  string
	imageReference string
	imagePlatform  component.OCIPlatform
	validateArgs   []string
	activateArgs   []string
}

func NewComponentActionRecipe(
	relativePath string,
	containerPath string,
	imageReference string,
	imagePlatform component.OCIPlatform,
	validateArgs []string,
	activateArgs []string,
) (ComponentActionRecipe, error) {
	if !validRelativeComponentConfigPath(relativePath) ||
		!filepath.IsAbs(containerPath) || filepath.Clean(containerPath) != containerPath ||
		!validImmutableImageReference(
			imageReference,
		) || !strings.HasSuffix(imageReference, "@sha256:"+imagePlatform.ChildDigest) ||
		managedimage.Verify(
			"sha256:"+imagePlatform.ConfigDigest,
			nil,
			"sha256:"+imagePlatform.ChildDigest,
			"sha256:"+imagePlatform.ConfigDigest,
			ocispec.Platform{
				OS:           imagePlatform.OS,
				Architecture: imagePlatform.Architecture,
				Variant:      imagePlatform.Variant,
			},
		) != nil ||
		!validComponentCommand(validateArgs) || !validComponentCommand(activateArgs) {
		return ComponentActionRecipe{}, errs.New(errs.KindInternal, "compiled Component action recipe is invalid")
	}
	return ComponentActionRecipe{
		relativePath: relativePath, containerPath: containerPath, imageReference: imageReference, imagePlatform: imagePlatform,
		validateArgs: append([]string(nil), validateArgs...), activateArgs: append([]string(nil), activateArgs...),
	}, nil
}

func (recipe ComponentActionRecipe) matchesImage(output []byte) bool {
	parts := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(parts) != 3 || parts[0] != recipe.imageReference {
		return false
	}
	var descriptor *ocispec.Descriptor
	if json.Unmarshal([]byte(parts[2]), &descriptor) != nil {
		return false
	}
	p := recipe.imagePlatform
	return managedimage.Verify(parts[1], descriptor, "sha256:"+p.ChildDigest, "sha256:"+p.ConfigDigest,
		ocispec.Platform{OS: p.OS, Architecture: p.Architecture, Variant: p.Variant}) == nil
}
