package blueprintparser

import (
	"fmt"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/compose-spec/compose-go/v2/types"
)

func normalizeDNS(
	specs map[string]dnsrecords.EnvironmentSpec,
	metadata core.EnvelopeMetadata,
) (map[string]dnsrecords.EnvironmentSpec, error) {
	normalized := make(map[string]dnsrecords.EnvironmentSpec, len(specs))
	labels := sortedDNSLabels(specs)
	for _, label := range labels {
		spec := specs[label]
		namespace := spec.Namespace
		if namespace.Tenant == "" && namespace.Project == "" && namespace.Environment == "" {
			captured, err := dnsrecords.CaptureNamespace(
				namespace.Scope, metadata.Tenant, metadata.Project, metadata.Environment,
			)
			if err != nil {
				return nil, validationError(fmt.Sprintf("DNS record %q: %s", label, err.Error()))
			}
			spec.Namespace = captured
		}
		if err := spec.Validate(label); err != nil {
			return nil, validationError(fmt.Sprintf("DNS record %q: %s", label, err.Error()))
		}
		normalized[label] = spec
	}
	if err := dnsrecords.ValidateEnvironmentSpecs(normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

func validateDNSZoneReferences(specs map[string]dnsrecords.EnvironmentSpec, project *types.Project) error {
	for _, label := range sortedDNSLabels(specs) {
		spec := specs[label]
		if spec.Service == "" {
			continue
		}
		if _, exists := project.Networks[spec.Zone]; !exists {
			return validationError(fmt.Sprintf("DNS record %q refers to undeclared Zone %q", label, spec.Zone))
		}
		if service, exists := project.Services[spec.Service]; exists {
			if _, connected := service.Networks[spec.Zone]; !connected {
				return validationError(
					fmt.Sprintf(
						"DNS record %q: Service %q is not connected to Zone %q",
						label,
						spec.Service,
						spec.Zone,
					),
				)
			}
			if err := domain.RequireTCPProxyPort(service.Expose); err != nil {
				return validationError(
					fmt.Sprintf(
						"DNS record %q: Service %q requires an exposed TCP port for its stable proxy",
						label,
						spec.Service,
					),
				)
			}
		}
		// A missing Service is valid waiting intention. The captured binding
		// requires an acknowledged proxy before it can produce a DNS answer.
	}
	return nil
}

func sortedDNSLabels(specs map[string]dnsrecords.EnvironmentSpec) []string {
	labels := make([]string, 0, len(specs))
	for label := range specs {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}
