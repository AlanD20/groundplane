package dnsrecords

import (
	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
)

// DNSRecord targets an explicit IPv4 address or a Service's stable proxy in a
// selected Zone. Labels are never resource references.
type DNSRecord struct {
	Hostname  string `yaml:"hostname" json:"hostname"`
	Address   string `yaml:"address,omitempty" json:"address,omitempty"`
	ServiceID string `yaml:"service_id,omitempty" json:"service_id,omitempty"`
	ZoneID    string `yaml:"zone_id,omitempty" json:"zone_id,omitempty"`
}

func ValidateDNSRecords(records []DNSRecord) error {
	if len(records) > 256 {
		return errs.New(errs.KindValidationFailed, "at most 256 DNS records are allowed")
	}
	seen := map[string]bool{}
	for _, record := range records {
		if !dnsname.Valid(record.Hostname) || record.Hostname == imagefetch.RegistryHostname || seen[record.Hostname] {
			return errs.New(
				errs.KindValidationFailed,
				"DNS hostnames must be unique canonical names, not the managed registry name",
			)
		}
		seen[record.Hostname] = true
		if record.Address != "" {
			address, err := netip.ParseAddr(record.Address)
			if err != nil || !address.Is4() || address.String() != record.Address || address.IsUnspecified() ||
				address.IsMulticast() ||
				record.ServiceID != "" ||
				record.ZoneID != "" {
				return errs.New(
					errs.KindValidationFailed,
					"DNS record requires an IPv4 address or a Service and Zone, not both",
				)
			}
		} else if ids.Validate(ids.KindService, record.ServiceID) != nil || ids.Validate(ids.KindNetwork, record.ZoneID) != nil {
			return errs.New(errs.KindValidationFailed, "DNS Service target requires stable Service and Zone ids")
		}
	}
	return nil
}
