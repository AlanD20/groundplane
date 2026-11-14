package backingservices

import (
	"math"
	"path"
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/docker/go-units"
)

// Creation settings enter both desired state and the first sealed Compose
// artifact. A later edit must never be needed to bound an initial startup.
func configureCreation(spec *adapters.CreationSpec, input apiTypes.BackingServiceCreate, custom bool) error {
	if input.Resources != nil {
		resources := *input.Resources
		if resources.CPUs < 0 || resources.CPUs > math.MaxFloat32 || math.IsNaN(resources.CPUs) ||
			math.IsInf(resources.CPUs, 0) {
			return errs.New(errs.KindValidationFailed, "Backing CPU limit must be a finite nonnegative number")
		}
		if resources.Mem != "" {
			bytes, err := units.RAMInBytes(resources.Mem)
			if err != nil || bytes <= 0 || resources.Mem != strings.TrimSpace(resources.Mem) {
				return errs.New(errs.KindValidationFailed, "Backing memory limit must be a positive size such as 1536m")
			}
			spec.MemoryBytes = bytes
		}
		spec.Resources = core.Resources{Mem: resources.Mem, CPUs: resources.CPUs}
	}
	if volume := input.CustomVolume; volume != nil {
		if !custom {
			return errs.New(
				errs.KindValidationFailed,
				"Only Custom Backings accept an operator-selected initial Volume",
			)
		}
		if !slug.Valid(volume.Slug) || !path.IsAbs(volume.MountPath) ||
			path.Clean(volume.MountPath) != volume.MountPath ||
			volume.MountPath == "/" ||
			strings.ContainsAny(volume.MountPath, "\x00\r\n\\") {
			return errs.New(
				errs.KindValidationFailed,
				"Custom Volume requires a valid slug and a clean absolute mount path other than /",
			)
		}
		spec.VolumeSlug, spec.VolumeKey, spec.MountPath = volume.Slug, volume.Slug, volume.MountPath
	}
	return nil
}
