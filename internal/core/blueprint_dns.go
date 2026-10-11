package core

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateBlueprintDNSNames checks one candidate as a whole. Looking only at
// stored hostname claims misses a DNS record and Route introduced together.
func ValidateBlueprintDNSNames(records map[string]dnsrecords.EnvironmentSpec, routes []RouteSpec) error {
	if err := dnsrecords.ValidateEnvironmentSpecs(records); err != nil {
		return err
	}
	routeNames := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		if route.Hostname != "" {
			routeNames[route.Hostname] = struct{}{}
		}
	}
	labels := make([]string, 0, len(records))
	for label := range records {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		hostname := records[label].Hostname(label)
		if _, used := routeNames[hostname]; used {
			return errs.Newf(errs.KindValidationFailed,
				"DNS record %q and a Route declare the same hostname %q; use distinct hostnames", label, hostname)
		}
	}
	return nil
}
