// Package imagefetch defines immutable input for explicit image delivery.
package imagefetch

import (
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

const RegistryHostname = "registry.groundplane.internal"
const RegistryAuthority = RegistryHostname + ":5000"

// RegistryAddress follows the first trusted private Controller listener. A
// loopback-only installation has no Runner admission or private registry access.
func RegistryAddress(listeners []string) netip.Addr {
	for _, listener := range listeners {
		endpoint, err := netip.ParseAddrPort(listener)
		if err == nil && endpoint.Addr().Is4() && endpoint.Addr().IsPrivate() {
			return endpoint.Addr()
		}
	}
	return netip.MustParseAddr("127.0.0.1")
}

// Plan contains no credentials. The manifest and config identities are selected
// before acceptance; execution never resolves Requested again.
type Plan struct {
	Requested      string `json:"requested"`
	Repository     string `json:"repository"`
	ManifestDigest string `json:"manifest_digest"`
	ConfigDigest   string `json:"config_digest"`
	Architecture   string `json:"architecture"`
	Variant        string `json:"variant,omitempty"`
}

func ParseRequested(value string) (reference.Named, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil || len(value) > 512 {
		return nil, errs.New(errs.KindValidationFailed, "image must be a registry reference")
	}
	_, tagged := named.(reference.NamedTagged)
	_, digested := named.(reference.Digested)
	if tagged == digested || digested && !imageref.IsDigestPinned(value) {
		return nil, errs.New(errs.KindValidationFailed, "image must select exactly one explicit tag or SHA-256 digest")
	}
	return named, nil
}

func (plan Plan) Reference() string { return plan.Repository + "@" + plan.ManifestDigest }

func (plan Plan) Validate() error {
	named, err := ParseRequested(plan.Requested)
	if err != nil {
		return err
	}
	if reference.TrimNamed(named).String() != plan.Repository || !imageref.IsDigestPinned(plan.Reference()) ||
		!workloadimage.LocalIDValid(plan.ConfigDigest) ||
		(plan.Architecture != "amd64" && plan.Architecture != "arm64") ||
		(plan.Architecture == "amd64" && plan.Variant != "") ||
		(plan.Architecture == "arm64" && plan.Variant != "" && plan.Variant != "v8") {
		return errs.New(errs.KindValidationFailed, "image fetch requires a pinned Linux host-platform manifest")
	}
	return nil
}
