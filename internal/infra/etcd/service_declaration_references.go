package etcd

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Inspect the normalized immutable declaration, not a rendered artifact. A
// declaration can be retired before Attach facts allow any runtime compilation.
type serviceDeclarationDocument struct {
	Services map[string]serviceDeclarationDependencies `yaml:"services"`
}

type serviceDeclarationDependencies struct {
	DependsOn yaml.Node `yaml:"depends_on"`
}

func requireServiceDeclarationUnreferenced(input environmentprojection.EnvironmentDesiredInput, name string) error {
	var document serviceDeclarationDocument
	if yaml.Unmarshal(input.Input.NormalizedCompose, &document) != nil {
		return errs.New(errs.KindInternal, "Service omission Compose declaration is corrupt")
	}
	if _, found := document.Services[name]; found {
		return errs.New(errs.KindStateConflict, "Service name is still declared in the current Blueprint")
	}
	for _, service := range document.Services {
		depends := service.DependsOn
		switch depends.Kind {
		case 0:
		case yaml.MappingNode:
			for index := 0; index+1 < len(depends.Content); index += 2 {
				if depends.Content[index].Value == name {
					return serviceDeclarationReferenceConflict()
				}
			}
		case yaml.SequenceNode:
			for _, dependency := range depends.Content {
				if dependency.Value == name {
					return serviceDeclarationReferenceConflict()
				}
			}
		default:
			return errs.New(errs.KindInternal, "Service dependency declaration is corrupt")
		}
	}
	for _, entry := range input.Input.Entries {
		if slices.Contains(entry.Exposure, name) {
			return serviceDeclarationReferenceConflict()
		}
	}
	for _, attach := range input.Input.Attachments {
		if attach.Service == name {
			return serviceDeclarationReferenceConflict()
		}
	}
	for _, route := range input.Input.Routes {
		if route.Target == name {
			return serviceDeclarationReferenceConflict()
		}
	}
	for _, extension := range input.Input.ServiceExtensions {
		if _, found := extension.DependsOn[name]; found {
			return serviceDeclarationReferenceConflict()
		}
	}
	for _, group := range input.Input.ReleaseGroups {
		if slices.Contains(group.Services, name) || slices.Contains(group.Order, name) {
			return serviceDeclarationReferenceConflict()
		}
	}
	return nil
}

func serviceDeclarationReferenceConflict() error {
	return errs.New(errs.KindResourceInUse, "Service is still referenced by another Environment resource")
}
