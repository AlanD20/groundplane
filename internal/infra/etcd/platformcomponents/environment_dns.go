package platformcomponents

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentDNSCapture joins a record's exact authored revision to a selected
// binding. Capturing it is preparation; only a successful resolver observation
// proves that its answer, or its waiting state, was applied.
type EnvironmentDNSCapture struct {
	DesiredRevisionID string                        `json:"desired_revision_id"`
	Binding           dnsrecords.EnvironmentBinding `json:"binding"`
}

type EnvironmentDNSResolution struct {
	Captures []EnvironmentDNSCapture
	Hosts    []PlatformDNSHost
}

func CloneEnvironmentDNS(captures []EnvironmentDNSCapture) []EnvironmentDNSCapture {
	return slices.Clone(captures)
}

// Every captured answer must match the same sealed host set. Waiting records
// cannot appear there, including through an unrelated Route or platform record.
func ValidateEnvironmentDNS(captures []EnvironmentDNSCapture, hosts []PlatformDNSHost) error {
	answers := make(map[string]string)
	for _, host := range hosts {
		for _, name := range host.Hostnames {
			answers[name] = host.Address
		}
	}
	previous := ""
	counts := make(map[string]int)
	for _, capture := range captures {
		if ids.Validate(ids.KindTask, capture.DesiredRevisionID) != nil {
			return errs.New(errs.KindValidationFailed, "Environment DNS capture requires its authored revision")
		}
		if err := capture.Binding.Validate(); err != nil {
			return err
		}
		record := capture.Binding.Record
		name := record.Hostname()
		if name <= previous {
			return errs.New(errs.KindValidationFailed, "Environment DNS captures must have unique sorted hostnames")
		}
		previous = name
		counts[record.EnvironmentID]++
		if counts[record.EnvironmentID] > dnsrecords.MaxEnvironmentRecords {
			return errs.New(errs.KindValidationFailed, "Environment DNS capture exceeds its record limit")
		}
		address, serving := answers[name]
		if capture.Binding.Address == "" {
			if serving {
				return errs.New(errs.KindValidationFailed, "waiting Environment DNS record cannot advertise an address")
			}
		} else if !serving || address != capture.Binding.Address {
			return errs.New(errs.KindValidationFailed, "Environment DNS answer differs from its captured binding")
		}
	}
	return nil
}
