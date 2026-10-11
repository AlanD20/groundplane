package dnsrecords

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/composekey"
)

const MaxEnvironmentRecords = 256

// EnvironmentSpec is normalized authoring intention. Zone is a Compose network
// name in the owning Environment; stable identities and observed addresses are
// selected later and cannot be supplied in a Blueprint.
type EnvironmentSpec struct {
	Namespace Namespace `yaml:"namespace" json:"namespace"`
	Address   string    `yaml:"address,omitempty" json:"address,omitempty"`
	Service   string    `yaml:"service,omitempty" json:"service,omitempty"`
	Zone      string    `yaml:"zone,omitempty" json:"zone,omitempty"`
}

func (spec EnvironmentSpec) Hostname(label string) string {
	return label + "." + spec.Namespace.Suffix()
}

func (spec EnvironmentSpec) Validate(label string) error {
	if err := ValidateEnvironmentHostname(label, spec.Namespace); err != nil {
		return err
	}
	if spec.Address != "" {
		if spec.Service != "" || spec.Zone != "" {
			return invalidEnvironmentDNS("DNS record must select an IPv4 address or a Service and Zone, not both")
		}
		return ValidateDNSRecords([]DNSRecord{{Hostname: spec.Hostname(label), Address: spec.Address}})
	}
	if composekey.Validate(spec.Service) != nil || composekey.Validate(spec.Zone) != nil {
		return invalidEnvironmentDNS("Environment DNS Service target requires a Service name and a Zone name")
	}
	return nil
}

// ValidateEnvironmentSpecs is also used by strict stored-input decoding, so
// accepting a record never depends on reinterpreting its original YAML.
func ValidateEnvironmentSpecs(specs map[string]EnvironmentSpec) error {
	if len(specs) > MaxEnvironmentRecords {
		return invalidEnvironmentDNS("Environment DNS may declare at most 256 records")
	}
	labels := make([]string, 0, len(specs))
	for label := range specs {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	hostnames := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		spec := specs[label]
		if err := spec.Validate(label); err != nil {
			return err
		}
		hostname := spec.Hostname(label)
		if _, exists := hostnames[hostname]; exists {
			return invalidEnvironmentDNS("Environment DNS hostnames must be unique")
		}
		hostnames[hostname] = struct{}{}
	}
	return nil
}

// CaptureNamespace records only the labels relevant to the selected scope.
// Callers pass authored/current owner labels, never labels from a wider target.
func CaptureNamespace(scope NamespaceScope, tenant, project, environment string) (Namespace, error) {
	if scope == "" {
		scope = NamespaceEnvironment
	}
	namespace := Namespace{Scope: scope, Tenant: tenant}
	if scope == NamespaceEnvironment || scope == NamespaceProject {
		namespace.Project = project
	}
	if scope == NamespaceEnvironment {
		namespace.Environment = environment
	}
	if err := namespace.Validate(); err != nil {
		return Namespace{}, err
	}
	return namespace, nil
}

// ValidateNamespaceChanges permits a retained record to keep its captured
// namespace through owner renames. New or explicitly changed namespaces must
// belong to the current owning hierarchy, not another Tenant or Project.
func ValidateNamespaceChanges(
	previous, next map[string]EnvironmentSpec,
	tenant, project, environment string,
) error {
	if err := ValidateEnvironmentSpecs(next); err != nil {
		return err
	}
	labels := make([]string, 0, len(next))
	for label := range next {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		spec := next[label]
		if prior, exists := previous[label]; exists && prior.Namespace == spec.Namespace {
			continue
		}
		current, err := CaptureNamespace(spec.Namespace.Scope, tenant, project, environment)
		if err != nil {
			return err
		}
		if current != spec.Namespace {
			return invalidEnvironmentDNS(
				"DNS namespace must match its owning hierarchy; only an existing record may retain labels from before a rename",
			)
		}
	}
	return nil
}
