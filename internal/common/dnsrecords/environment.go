package dnsrecords

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Namespace retains the labels selected by the operator. It is not recomputed
// from the current hierarchy: renaming an owner does not rename its DNS records.
type Namespace struct {
	Scope       NamespaceScope `yaml:"scope" json:"scope"`
	Tenant      string         `yaml:"tenant" json:"tenant"`
	Project     string         `yaml:"project,omitempty" json:"project,omitempty"`
	Environment string         `yaml:"environment,omitempty" json:"environment,omitempty"`
}

type NamespaceScope string

const (
	NamespaceEnvironment NamespaceScope = "environment"
	NamespaceProject     NamespaceScope = "project"
	NamespaceTenant      NamespaceScope = "tenant"
)

func (namespace Namespace) Validate() error {
	if !validLabel(namespace.Tenant) {
		return invalidEnvironmentDNS("DNS namespace requires a lowercase Tenant label")
	}
	switch namespace.Scope {
	case NamespaceEnvironment:
		if !validLabel(namespace.Project) || !validLabel(namespace.Environment) {
			return invalidEnvironmentDNS("Environment DNS namespace requires Project and Environment labels")
		}
	case NamespaceProject:
		if !validLabel(namespace.Project) || namespace.Environment != "" {
			return invalidEnvironmentDNS("Project DNS namespace requires a Project label and no Environment label")
		}
	case NamespaceTenant:
		if namespace.Project != "" || namespace.Environment != "" {
			return invalidEnvironmentDNS("Tenant DNS namespace cannot contain Project or Environment labels")
		}
	default:
		return invalidEnvironmentDNS("DNS namespace scope must be environment, project or tenant")
	}
	return nil
}

func (namespace Namespace) Suffix() string {
	switch namespace.Scope {
	case NamespaceEnvironment:
		return namespace.Environment + "." + namespace.Project + "." + namespace.Tenant + ".internal"
	case NamespaceProject:
		return namespace.Project + "." + namespace.Tenant + ".internal"
	case NamespaceTenant:
		return namespace.Tenant + ".internal"
	default:
		return ""
	}
}

// EnvironmentRecord is desired intention, not an active answer. ServiceName is
// resolved only within EnvironmentID; a missing Service leaves the name reserved.
// Namespace affects the hostname, never ownership or target lookup.
type EnvironmentRecord struct {
	ID            string    `json:"id"`
	EnvironmentID string    `json:"environment_id"`
	Label         string    `yaml:"label" json:"label"`
	Namespace     Namespace `yaml:"namespace" json:"namespace"`
	Address       string    `yaml:"address,omitempty" json:"address,omitempty"`
	ServiceName   string    `yaml:"service,omitempty" json:"service,omitempty"`
	ZoneID        string    `json:"zone_id,omitempty"`
}

func (record EnvironmentRecord) Hostname() string {
	return record.Label + "." + record.Namespace.Suffix()
}

func (record EnvironmentRecord) Validate() error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindDNSRecord, record.ID) != nil {
		return invalidEnvironmentDNS("Environment DNS requires a stable record id and owner")
	}
	if err := ValidateEnvironmentHostname(record.Label, record.Namespace); err != nil {
		return err
	}
	if record.Address != "" {
		if record.ServiceName != "" || record.ZoneID != "" {
			return invalidEnvironmentDNS("DNS record must select an IPv4 address or a Service and Zone, not both")
		}
		return ValidateDNSRecords([]DNSRecord{{Hostname: record.Hostname(), Address: record.Address}})
	}
	if composekey.Validate(record.ServiceName) != nil ||
		ids.Validate(ids.KindNetwork, record.ZoneID) != nil {
		return invalidEnvironmentDNS("Environment DNS Service target requires a Service name and stable Zone id")
	}
	return nil
}

// ValidateEnvironmentHostname checks authored names without inventing a runtime
// owner or Zone id. Both Blueprint input and bound records use this boundary.
func ValidateEnvironmentHostname(label string, namespace Namespace) error {
	if !validLabel(label) {
		return invalidEnvironmentDNS("Environment DNS requires a lowercase record label")
	}
	if err := namespace.Validate(); err != nil {
		return err
	}
	if !dnsname.Valid(label + "." + namespace.Suffix()) {
		return invalidEnvironmentDNS("Environment DNS hostname exceeds the supported hostname length")
	}
	if label+"."+namespace.Suffix() == imagefetch.RegistryHostname {
		return invalidEnvironmentDNS("Environment DNS cannot use the managed registry hostname")
	}
	return nil
}

// EnvironmentBinding captures an answer selected from acknowledged runtime.
// It is kept separate from desired intention so waiting does not advertise an
// address, and withdrawing an answer never discards the operator's record.
type EnvironmentBinding struct {
	Record          EnvironmentRecord `json:"record"`
	ServiceID       string            `json:"service_id,omitempty"`
	Address         string            `json:"address,omitempty"`
	RuntimeRevision int64             `json:"runtime_revision,omitempty"`
}

func (binding EnvironmentBinding) Validate() error {
	if err := binding.Record.Validate(); err != nil {
		return err
	}
	if binding.Record.Address != "" {
		if binding.Address != binding.Record.Address || binding.ServiceID != "" || binding.RuntimeRevision != 0 {
			return invalidEnvironmentDNS("explicit-IP DNS answer must match its declared address")
		}
		return nil
	}
	if binding.ServiceID == "" && binding.Address == "" && binding.RuntimeRevision == 0 {
		return nil // Waiting for Service; the hostname remains owned but has no answer.
	}
	if ids.Validate(ids.KindService, binding.ServiceID) != nil || binding.RuntimeRevision <= 0 ||
		binding.Address == "" {
		return invalidEnvironmentDNS("Service DNS answer requires an acknowledged Service runtime and proxy address")
	}
	return ValidateDNSRecords([]DNSRecord{{Hostname: binding.Record.Hostname(), Address: binding.Address}})
}

// Answer returns only captured, non-waiting answers. The resource references
// accompany Service answers so existing resolver recovery can protect them.
func (binding EnvironmentBinding) Answer() (DNSRecord, bool) {
	if binding.Address == "" {
		return DNSRecord{}, false
	}
	if binding.ServiceID == "" {
		return DNSRecord{Hostname: binding.Record.Hostname(), Address: binding.Address}, true
	}
	return DNSRecord{
		Hostname: binding.Record.Hostname(), ServiceID: binding.ServiceID, ZoneID: binding.Record.ZoneID,
	}, true
}

func validLabel(value string) bool {
	return !strings.Contains(value, ".") && dnsname.Valid(value)
}

func invalidEnvironmentDNS(detail string) error {
	return errs.New(errs.KindValidationFailed, detail)
}
