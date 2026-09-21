package environmentprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumManagedComponentRuntimeSources = 2

func ValidateManagedComponentRuntimeSources(projection EnvironmentComposeProjection) error {
	if len(projection.ManagedComponentRuntimeSources) > MaximumManagedComponentRuntimeSources {
		return errs.New(errs.KindValidationFailed, "managed Component runtime source count is invalid")
	}
	components := make(map[core.ComponentKind]componentrecord.Record, len(projection.Components))
	for _, component := range projection.Components {
		components[component.Desired.Kind] = component
	}
	previousKind := core.ComponentKind("")
	for _, source := range projection.ManagedComponentRuntimeSources {
		component, known := components[source.ComponentKind]
		digest, digestErr := hex.DecodeString(source.ArtifactSHA256)
		if !known || source.ComponentKind <= previousKind || component.Desired.ID != source.ComponentID ||
			ids.Validate(ids.KindComponent, source.ComponentID) != nil ||
			ids.Validate(ids.KindService, source.ServiceID) != nil || source.ComposeName == "" ||
			ids.Validate(ids.KindTask, source.RevisionID) != nil ||
			ids.Validate(ids.KindConfig, source.ArtifactID) != nil || digestErr != nil || len(digest) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "managed Component runtime source is invalid or unsorted")
		}
		if component.Desired.Enabled && (len(component.Runtime.GeneratedServices) != 1 ||
			component.Runtime.GeneratedServices[0] != source.ServiceID) {
			return errs.New(errs.KindValidationFailed, "enabled Component runtime source identity changed")
		}
		previousKind = source.ComponentKind
	}
	return nil
}
