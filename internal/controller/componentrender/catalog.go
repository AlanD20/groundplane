package componentrender

import (
	"crypto/sha256"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"path"
)

type EnvironmentComponentPlanFunc func(
	core.Environment,
	core.Component,
) (componentsdk.EnvironmentPlan, error)

type EnvironmentHTTPRouterProjectionFunc func(
	core.Environment,
	core.Component,
) (componentsdk.HTTPRouterInput, error)

type EnvironmentHTTPRouterPlanFunc func(
	componentsdk.HTTPRouterInput,
	core.Component,
) (componentsdk.EnvironmentPlan, error)

// EnvironmentManagedConfigurationRegistration binds a registered Component's
// generic managed-config action to the immutable file it activates.
type EnvironmentManagedConfigurationRegistration struct {
	SourcePath string
	ActionID   componentsdk.ActionID
}

// EnvironmentComponentRegistration is the composition-root seam between one
// build-time registered implementation and Controller-owned plan validation.
type EnvironmentComponentRegistration struct {
	Kind                 core.ComponentKind
	Definition           componentsdk.Definition
	CatalogDigest        [sha256.Size]byte
	Plan                 EnvironmentComponentPlanFunc
	ProjectHTTPRouter    EnvironmentHTTPRouterProjectionFunc
	PlanHTTPRouter       EnvironmentHTTPRouterPlanFunc
	ManagedConfiguration *EnvironmentManagedConfigurationRegistration
}

type GeneratedEnvironmentService struct {
	ComponentID string
	Name        string
	Definition  componentsdk.ManagedService
}

type GeneratedEnvironmentFile struct {
	ComponentID string
	Path        string
	Content     []byte
}

type EnvironmentComponentRender struct {
	Services []GeneratedEnvironmentService
	Files    []GeneratedEnvironmentFile
}

func ValidateEnvironmentComponentCatalog(catalog []EnvironmentComponentRegistration) error {
	seen := make(map[core.ComponentKind]struct{}, len(catalog))
	for _, registration := range catalog {
		if registration.Kind == "" || registration.Plan == nil ||
			registration.Definition.Validate() != nil ||
			registration.Definition.Implementation() != componentsdk.ImplementationKey(registration.Kind) ||
			zeroComponentDigest(registration.CatalogDigest) {
			return errs.New(errs.KindInternal, "Environment Component registration is invalid")
		}
		if managed := registration.ManagedConfiguration; managed != nil {
			action, found := registration.Definition.FindAction(managed.ActionID)
			if managed.SourcePath == "" || path.IsAbs(managed.SourcePath) ||
				path.Clean(managed.SourcePath) != managed.SourcePath ||
				!found ||
				action.Capability() != componentsdk.CapabilityManagedConfig ||
				action.Operation() != componentsdk.OperationActivate {
				return errs.New(errs.KindInternal, "Environment Component managed configuration is invalid")
			}
		}
		providesRouter := false
		for _, capability := range registration.Definition.Provides() {
			providesRouter = providesRouter || capability == componentsdk.CapabilityHTTPRouter
		}
		if registration.ProjectHTTPRouter != nil != (registration.PlanHTTPRouter != nil) ||
			registration.ProjectHTTPRouter != nil && (!providesRouter || registration.ManagedConfiguration == nil) {
			return errs.New(errs.KindInternal, "HTTP router Component registration is incomplete")
		}
		if _, duplicate := seen[registration.Kind]; duplicate {
			return errs.New(errs.KindInternal, "Environment Component catalog repeats a kind")
		}
		seen[registration.Kind] = struct{}{}
	}
	return nil
}

func CloneEnvironmentComponentCatalog(
	catalog []EnvironmentComponentRegistration,
) []EnvironmentComponentRegistration {
	return append([]EnvironmentComponentRegistration(nil), catalog...)
}
