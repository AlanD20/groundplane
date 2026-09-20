package adapters

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreationSpec is the resolved shape of a backing service. Managed adapters
// compile every workload detail; custom creation supplies only its image and
// canonical service name.
type CreationSpec struct {
	ServiceName   string
	Image         string
	VolumeSlug    string
	VolumeKey     string
	MountPath     string
	Command       []string
	HealthCommand []string
	Expose        []string
	HealthTCP     string
	Environment   []CreationEnvironment
}

// CreationEnvironment declares one generated environment entry. Entries with
// the same non-empty BootstrapKey receive the same Controller-generated value.
type CreationEnvironment struct {
	Name         string
	Literal      string
	BootstrapKey string
	Secret       bool
}

type creationAdapter interface {
	CreationSpec(core.BackingAuthentication) CreationSpec
}

// BackingCreationSpec resolves the immutable creation contract for one managed
// adapter. Custom creation is resolved separately from its operator image.
func BackingCreationSpec(key string, authentication core.BackingAuthentication) (CreationSpec, error) {
	adapter, ok := Get(key)
	if !ok {
		return CreationSpec{}, errs.Newf(errs.KindValidationFailed, "unsupported backing-service adapter %q", key)
	}
	creator, ok := adapter.(creationAdapter)
	if !ok || adapter.Custom() {
		return CreationSpec{}, errs.Newf(
			errs.KindValidationFailed,
			"adapter %q cannot create a managed backing service",
			key,
		)
	}
	authentication, err := core.ResolveBackingAuthentication(adapter.SupportsAuthenticationModes(), authentication)
	if err != nil {
		return CreationSpec{}, err
	}
	spec := creator.CreationSpec(authentication)
	spec.Image = adapter.DefaultImage()
	if err := validateCreationSpec(spec); err != nil {
		return CreationSpec{}, err
	}
	return cloneCreationSpec(spec), nil
}

func validateCreationSpec(spec CreationSpec) error {
	if strings.TrimSpace(spec.ServiceName) == "" || strings.TrimSpace(spec.Image) == "" ||
		strings.TrimSpace(spec.VolumeSlug) == "" ||
		strings.TrimSpace(spec.VolumeKey) == "" || strings.TrimSpace(spec.MountPath) == "" ||
		strings.TrimSpace(
			spec.HealthTCP,
		) == "" || len(spec.HealthCommand) < 2 || len(spec.Expose) == 0 || len(spec.Environment) == 0 {
		return errs.New(errs.KindInternal, "backing-service adapter has an incomplete creation spec")
	}
	seenNames := make(map[string]struct{}, len(spec.Environment))
	bootstrap := false
	for _, entry := range spec.Environment {
		if strings.TrimSpace(entry.Name) == "" {
			return errs.New(errs.KindInternal, "backing-service adapter has an empty environment name")
		}
		if _, exists := seenNames[entry.Name]; exists {
			return errs.Newf(errs.KindInternal, "backing-service adapter repeats environment name %q", entry.Name)
		}
		seenNames[entry.Name] = struct{}{}
		if entry.BootstrapKey == "" {
			if entry.Secret || entry.Literal == "" {
				return errs.Newf(errs.KindInternal, "backing-service environment %q has no value source", entry.Name)
			}
			continue
		}
		if entry.Literal != "" || !entry.Secret {
			return errs.Newf(errs.KindInternal, "backing-service bootstrap environment %q must be secret", entry.Name)
		}
		bootstrap = true
	}
	if !bootstrap {
		return errs.New(errs.KindInternal, "backing-service adapter has no bootstrap credential")
	}
	return nil
}

// HasVolume reports whether creation owns a persistent managed Volume.
func (spec CreationSpec) HasVolume() bool {
	return spec.VolumeSlug != "" || spec.VolumeKey != "" || spec.MountPath != ""
}

// HasEnvironment reports whether creation materializes an Environment env file.
func (spec CreationSpec) HasEnvironment() bool {
	return len(spec.Environment) != 0
}

// HasHealthcheck reports whether creation can wait for native health.
func (spec CreationSpec) HasHealthcheck() bool {
	return spec.HealthTCP != "" || len(spec.HealthCommand) != 0
}

func cloneCreationSpec(spec CreationSpec) CreationSpec {
	spec.Command = append([]string(nil), spec.Command...)
	spec.HealthCommand = append([]string(nil), spec.HealthCommand...)
	spec.Expose = append([]string(nil), spec.Expose...)
	spec.Environment = append([]CreationEnvironment(nil), spec.Environment...)
	return spec
}
