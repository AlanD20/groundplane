// Package edgeconnectioncheck defines the closed read-only Tunnel and public
// access diagnostic. Source capture selects an applied origin and connector;
// neither the helper nor the public check chooses new desired configuration.
package edgeconnectioncheck

import (
	"encoding/hex"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	HelperArgument     = "--groundplane-edge-connection-check"
	MaximumInputBytes  = 4096
	MaximumOutputBytes = 16384
	MaximumAddresses   = 16
	StageTimeout       = 5 * time.Second
	PrivateTimeout     = 12 * time.Second
	PublicTimeout      = 17 * time.Second
)

type PublicTarget struct {
	Hostname string `json:"hostname"`
	Path     string `json:"path"`
}

func (target PublicTarget) Validate() error {
	if len(target.Hostname) == 0 || len(target.Hostname) > 253 || !strings.Contains(target.Hostname, ".") ||
		strings.HasSuffix(target.Hostname, ".") {
		return errs.New(errs.KindValidationFailed, "Route check requires a concrete public DNS hostname")
	}
	for _, label := range strings.Split(target.Hostname, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errs.New(errs.KindValidationFailed, "Route check hostname is invalid")
		}
		for _, character := range label {
			if character != '-' && !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') {
				return errs.New(errs.KindValidationFailed, "Route check hostname is invalid")
			}
		}
	}
	if _, err := netip.ParseAddr(target.Hostname); err == nil {
		return errs.New(errs.KindValidationFailed, "Route check requires a DNS name, not an IP address")
	}
	parsed, err := url.ParseRequestURI(target.Path)
	if err != nil || len(target.Path) > 2048 || !strings.HasPrefix(target.Path, "/") ||
		strings.HasPrefix(target.Path, "//") ||
		parsed.IsAbs() ||
		parsed.Host != "" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.ForceQuery ||
		strings.ContainsAny(target.Path, "*\\#\r\n") {
		return errs.New(
			errs.KindValidationFailed,
			"Route check requires a concrete absolute path without query or fragment",
		)
	}
	return nil
}

type Request struct {
	RequestID       string       `json:"request_id"`
	EnvironmentID   string       `json:"environment_id"`
	ComponentID     string       `json:"component_id"`
	ServiceID       string       `json:"service_id"`
	RouteID         string       `json:"route_id"`
	OriginServiceID string       `json:"origin_service_id"`
	RuntimeSHA256   string       `json:"runtime_sha256"`
	ReadyURL        string       `json:"ready_url"`
	OriginAddress   string       `json:"origin_address"`
	Target          PublicTarget `json:"target"`
}

func (request Request) Validate() error {
	if ids.Validate(ids.KindOperation, request.RequestID) != nil ||
		ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		ids.Validate(ids.KindComponent, request.ComponentID) != nil ||
		ids.Validate(ids.KindService, request.ServiceID) != nil ||
		!request.validOriginIdentity() ||
		len(request.RuntimeSHA256) != 64 ||
		strings.ToLower(request.RuntimeSHA256) != request.RuntimeSHA256 {
		return errs.New(errs.KindValidationFailed, "Tunnel check captured identity is invalid")
	}
	if _, err := hex.DecodeString(request.RuntimeSHA256); err != nil {
		return errs.New(errs.KindValidationFailed, "Tunnel check runtime digest is invalid")
	}
	ready, err := url.Parse(request.ReadyURL)
	if err != nil || ready.Scheme != "http" || ready.User != nil || ready.Path != "/ready" || ready.RawPath != "" ||
		ready.RawQuery != "" || ready.Fragment != "" || ready.Opaque != "" || ready.ForceQuery {
		return errs.New(errs.KindValidationFailed, "Tunnel readiness endpoint is invalid")
	}
	address, err := netip.ParseAddrPort(ready.Host)
	if err != nil || !address.Addr().IsLoopback() || address.Port() == 0 {
		return errs.New(errs.KindValidationFailed, "Tunnel readiness must use its compiled loopback endpoint")
	}
	origin, err := netip.ParseAddrPort(request.OriginAddress)
	if err != nil || !origin.Addr().Is4() || !origin.Addr().IsPrivate() || origin.Port() == 0 {
		return errs.New(errs.KindValidationFailed, "Tunnel origin must use its captured private serving address")
	}
	return request.Target.Validate()
}

func (request Request) validOriginIdentity() bool {
	if request.RouteID != "" {
		return ids.Validate(ids.KindRoute, request.RouteID) == nil && request.OriginServiceID == ""
	}
	return ids.Validate(ids.KindService, request.OriginServiceID) == nil && request.OriginServiceID != request.ServiceID
}

// TargetID identifies the owning operator resource, not the generated connector
// Service. Both source variants use the same execution and cleanup contract.
func (request Request) TargetID() string {
	if request.RouteID != "" {
		return request.RouteID
	}
	return request.ComponentID
}

type StageName string

const (
	Tunnel StageName = "tunnel_connection"
	Origin StageName = "origin_http"
	DNS    StageName = "public_dns"
	TLS    StageName = "public_tls"
	HTTP   StageName = "public_http"
)

type Outcome string

const (
	Passed    Outcome = "passed"
	Protected Outcome = "protected"
	Failed    Outcome = "failed"
	Skipped   Outcome = "skipped"
)

type Failure string

const (
	NoFailure         Failure = ""
	ConnectionFailure Failure = "connection"
	DNSFailure        Failure = "dns"
	UnsafeAddress     Failure = "non_public_address"
	TLSFailure        Failure = "tls"
	HTTPFailure       Failure = "http_response"
	NotReady          Failure = "connector_not_ready"
	TimeoutFailure    Failure = "timeout"
	Canceled          Failure = "canceled"
	ProtocolFailure   Failure = "protocol"
)

type Stage struct {
	Name       StageName     `json:"name"`
	Outcome    Outcome       `json:"outcome"`
	Failure    Failure       `json:"failure,omitempty"`
	HTTPStatus int           `json:"http_status,omitempty"`
	Elapsed    time.Duration `json:"elapsed"`
}

func validateStages(started, finished time.Time, stages []Stage, names []StageName, maximum time.Duration) error {
	if started.IsZero() || finished.Before(started) || finished.Sub(started) > maximum || len(stages) != len(names) {
		return errs.New(errs.KindValidationFailed, "edge diagnostic lifetime or stages are invalid")
	}
	for index, stage := range stages {
		if stage.Name != names[index] || stage.Elapsed < 0 || stage.Elapsed > finished.Sub(started) ||
			stage.Elapsed > StageTimeout+time.Second ||
			stage.HTTPStatus != 0 &&
				(stage.HTTPStatus < 100 || stage.HTTPStatus > 599 || stage.Name != HTTP && stage.Name != Origin && stage.Name != Tunnel) {
			return errs.New(errs.KindValidationFailed, "edge diagnostic stage evidence is invalid")
		}
		switch stage.Outcome {
		case Passed:
			if stage.Failure != NoFailure || stage.Name == Tunnel && stage.HTTPStatus != 200 ||
				(stage.Name == Origin || stage.Name == HTTP) && (stage.HTTPStatus < 200 || stage.HTTPStatus >= 400) {
				return errs.New(errs.KindValidationFailed, "edge diagnostic successful stage is contradictory")
			}
		case Protected:
			if stage.Failure != NoFailure || stage.Name != Origin && stage.Name != HTTP ||
				stage.HTTPStatus != 401 && stage.HTTPStatus != 403 {
				return errs.New(errs.KindValidationFailed, "edge diagnostic protected response is invalid")
			}
		case Skipped:
			if stage.Failure != NoFailure || stage.HTTPStatus != 0 || stage.Elapsed != 0 {
				return errs.New(errs.KindValidationFailed, "edge diagnostic skipped stage has evidence")
			}
		case Failed:
			switch stage.Failure {
			case ConnectionFailure,
				DNSFailure,
				UnsafeAddress,
				TLSFailure,
				HTTPFailure,
				NotReady,
				TimeoutFailure,
				Canceled,
				ProtocolFailure:
			default:
				return errs.New(errs.KindValidationFailed, "edge diagnostic failure is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "edge diagnostic outcome is invalid")
		}
	}
	return nil
}

type PrivateResult struct {
	RequestID     string    `json:"request_id"`
	RuntimeSHA256 string    `json:"runtime_sha256"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	Stages        []Stage   `json:"stages"`
}

func (result PrivateResult) Validate(request Request) error {
	if request.Validate() != nil || result.RequestID != request.RequestID ||
		result.RuntimeSHA256 != request.RuntimeSHA256 {
		return errs.New(errs.KindValidationFailed, "Tunnel diagnostic result differs from its captured request")
	}
	if err := validateStages(result.StartedAt, result.FinishedAt, result.Stages, []StageName{Tunnel, Origin}, PrivateTimeout+time.Second); err != nil {
		return err
	}
	if result.Stages[0].Outcome == Skipped || result.Stages[1].Outcome == Skipped {
		return errs.New(errs.KindValidationFailed, "Tunnel and origin checks are independent and must both run")
	}
	return nil
}

type PublicResult struct {
	Target               PublicTarget `json:"target"`
	StartedAt            time.Time    `json:"started_at"`
	FinishedAt           time.Time    `json:"finished_at"`
	Stages               []Stage      `json:"stages"`
	Addresses            []string     `json:"addresses"`
	SelectedAddress      string       `json:"selected_address,omitempty"`
	TLSVersion           uint16       `json:"tls_version,omitempty"`
	CertificateExpiresAt *time.Time   `json:"certificate_expires_at,omitempty"`
}

func (result PublicResult) Validate(target PublicTarget) error {
	if target.Validate() != nil || result.Target != target || len(result.Addresses) > MaximumAddresses {
		return errs.New(errs.KindValidationFailed, "public Route diagnostic identity is invalid")
	}
	if err := validateStages(result.StartedAt, result.FinishedAt, result.Stages, []StageName{DNS, TLS, HTTP}, PublicTimeout+time.Second); err != nil {
		return err
	}
	if result.Stages[0].Outcome != Passed && result.Stages[0].Outcome != Failed ||
		result.Stages[1].Outcome != Passed && result.Stages[1].Outcome != Failed &&
			result.Stages[1].Outcome != Skipped {
		return errs.New(errs.KindValidationFailed, "public Route diagnostic DNS or TLS outcome is invalid")
	}
	seen := make(map[string]bool, len(result.Addresses))
	for _, value := range result.Addresses {
		address, err := netip.ParseAddr(value)
		if err != nil || !PublicAddress(address) || seen[value] {
			return errs.New(errs.KindValidationFailed, "public Route diagnostic address is invalid")
		}
		seen[value] = true
	}
	if result.Stages[0].Outcome == Passed {
		if len(result.Addresses) == 0 || !seen[result.SelectedAddress] || result.Stages[1].Outcome == Skipped {
			return errs.New(errs.KindValidationFailed, "public Route diagnostic lacks its DNS address")
		}
	} else if len(result.Addresses) != 0 || result.SelectedAddress != "" || result.Stages[1].Outcome != Skipped || result.Stages[2].Outcome != Skipped {
		return errs.New(errs.KindValidationFailed, "public Route diagnostic has evidence after failed DNS")
	}
	if result.Stages[1].Outcome != Passed {
		if result.TLSVersion != 0 || result.CertificateExpiresAt != nil || result.Stages[2].Outcome != Skipped {
			return errs.New(errs.KindValidationFailed, "public Route diagnostic has evidence after failed TLS")
		}
	} else if (result.TLSVersion != 0x0303 && result.TLSVersion != 0x0304) || result.CertificateExpiresAt == nil || !result.CertificateExpiresAt.After(result.StartedAt) || result.Stages[2].Outcome == Skipped {
		return errs.New(errs.KindValidationFailed, "public Route diagnostic TLS evidence is incomplete")
	}
	return nil
}

// Public probes cannot turn a provider-controlled DNS answer into a request to
// host management, loopback, private networks or metadata endpoints.
func PublicAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() ||
		address.Zone() != "" {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix(
		"192.0.2.0/24",
	), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix(
		"203.0.113.0/24",
	), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix(
		"2001::/32",
	), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"),
}
