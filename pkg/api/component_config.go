package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// componentConfigWire deliberately keeps raw fields so that an explicitly
// supplied null cannot be mistaken for an omitted discriminator field.
type componentConfigWire struct {
	Implementation    json.RawMessage `json:"implementation"`
	Records           json.RawMessage `json:"records"`
	ZoneIDs           json.RawMessage `json:"zone_ids"`
	CaddyfileTemplate json.RawMessage `json:"caddyfile_template"`
	Alias             json.RawMessage `json:"alias"`
	SecretID          json.RawMessage `json:"secret_id"`
	UpstreamAuto      json.RawMessage `json:"upstream_auto"`
	UpstreamResolvers json.RawMessage `json:"upstream_resolvers"`
	Forwarders        json.RawMessage `json:"forwarders"`
	TailnetDelegation json.RawMessage `json:"tailnet_delegation"`
	CorefileTemplate  json.RawMessage `json:"corefile_template"`
}

func (config ComponentConfig) MarshalJSON() ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	switch {
	case config.Caddy != nil:
		return json.Marshal(struct {
			Implementation    string   `json:"implementation"`
			ZoneIDs           []string `json:"zone_ids"`
			CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
			Alias             string   `json:"alias,omitempty"`
		}{"caddy", config.Caddy.ZoneIDs, config.Caddy.CaddyfileTemplate, config.Caddy.Alias})
	case config.Envoy != nil:
		return json.Marshal(struct {
			Implementation string `json:"implementation"`
			*EnvoyComponentConfig
		}{"envoy", config.Envoy})
	case config.CloudflareTunnel != nil:
		return json.Marshal(struct {
			Implementation string   `json:"implementation"`
			ZoneIDs        []string `json:"zone_ids"`
			SecretID       string   `json:"secret_id"`
		}{"cloudflare-tunnel", config.CloudflareTunnel.ZoneIDs, config.CloudflareTunnel.SecretID})
	default:
		// Do not use omitempty here: empty resolver and forwarder lists are
		// meaningful configured state and must remain JSON arrays.
		return json.Marshal(struct {
			Implementation    string                  `json:"implementation"`
			Records           []dnsrecords.DNSRecord  `json:"records,omitempty"`
			CorefileTemplate  string                  `json:"corefile_template"`
			UpstreamAuto      bool                    `json:"upstream_auto"`
			UpstreamResolvers []string                `json:"upstream_resolvers"`
			Forwarders        []ComponentDNSForwarder `json:"forwarders"`
			TailnetDelegation bool                    `json:"tailnet_delegation"`
		}{
			"coredns",
			config.CoreDNS.Records,
			config.CoreDNS.CorefileTemplate,
			config.CoreDNS.UpstreamAuto,
			config.CoreDNS.UpstreamResolvers,
			config.CoreDNS.Forwarders,
			config.CoreDNS.TailnetDelegation,
		})
	}
}

func (config *ComponentConfig) UnmarshalJSON(data []byte) error {
	var wire componentConfigWire
	if err := decodeComponentConfigWire(data, &wire); err != nil {
		return err
	}
	implementation, err := decodeComponentImplementation(wire.Implementation)
	if err != nil {
		return err
	}
	switch implementation {
	case "caddy", "envoy":
		if present(wire.SecretID) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) ||
			present(wire.Forwarders) ||
			present(wire.TailnetDelegation) || present(wire.Records) || present(wire.CorefileTemplate) {
			return malformedComponentConfig("component config router variant is incomplete or mixed")
		}
		zoneIDs, err := decodeStringArray(wire.ZoneIDs, "zone_ids")
		if err != nil {
			return err
		}
		var alias string
		if present(wire.Alias) {
			if err := decodeRequiredField(wire.Alias, "alias", &alias); err != nil {
				return err
			}
		}
		var template string
		if present(wire.CaddyfileTemplate) {
			if err := decodeRequiredField(wire.CaddyfileTemplate, "caddyfile_template", &template); err != nil {
				return err
			}
		}
		if implementation == "envoy" {
			if present(wire.CaddyfileTemplate) {
				return malformedComponentConfig("Envoy config does not accept caddyfile_template")
			}
			*config = ComponentConfig{Envoy: &EnvoyComponentConfig{ZoneIDs: zoneIDs, Alias: alias}}
		} else {
			*config = ComponentConfig{Caddy: &CaddyComponentConfig{ZoneIDs: zoneIDs, CaddyfileTemplate: template, Alias: alias}}
		}
	case "cloudflare-tunnel":
		if (present(wire.CaddyfileTemplate) || present(wire.Alias)) || present(wire.UpstreamAuto) ||
			present(wire.UpstreamResolvers) ||
			present(wire.Forwarders) ||
			present(wire.TailnetDelegation) || present(wire.Records) || present(wire.CorefileTemplate) {
			return malformedComponentConfig("component config Cloudflare Tunnel variant is mixed")
		}
		var secretID string
		if err := decodeRequiredField(wire.SecretID, "secret_id", &secretID); err != nil {
			return err
		}
		zoneIDs, err := decodeStringArray(wire.ZoneIDs, "zone_ids")
		if err != nil {
			return err
		}
		*config = ComponentConfig{
			CloudflareTunnel: &CloudflareTunnelComponentConfig{ZoneIDs: zoneIDs, SecretID: secretID},
		}
	case "coredns":
		if present(wire.ZoneIDs) || (present(wire.CaddyfileTemplate) || present(wire.Alias)) ||
			present(wire.SecretID) ||
			!present(wire.CorefileTemplate) ||
			!present(wire.UpstreamAuto) ||
			!present(wire.UpstreamResolvers) ||
			!present(wire.Forwarders) ||
			!present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config CoreDNS variant is incomplete or mixed")
		}
		var corefileTemplate string
		if err := decodeRequiredField(wire.CorefileTemplate, "corefile_template", &corefileTemplate); err != nil {
			return err
		}
		var upstreamAuto, tailnetDelegation bool
		if err := decodeRequiredField(wire.UpstreamAuto, "upstream_auto", &upstreamAuto); err != nil {
			return err
		}
		upstreamResolvers, err := decodeStringArray(wire.UpstreamResolvers, "upstream_resolvers")
		if err != nil {
			return err
		}
		forwarders, err := decodeForwarders(wire.Forwarders, "forwarders")
		if err != nil {
			return err
		}
		if err := decodeRequiredField(wire.TailnetDelegation, "tailnet_delegation", &tailnetDelegation); err != nil {
			return err
		}
		records, err := decodeDNSRecords(wire.Records)
		if err != nil {
			return err
		}
		*config = ComponentConfig{CoreDNS: &CoreDNSComponentConfig{
			Records:          records,
			CorefileTemplate: corefileTemplate,
			UpstreamAuto:     upstreamAuto, UpstreamResolvers: upstreamResolvers,
			Forwarders: forwarders, TailnetDelegation: tailnetDelegation,
		}}
	}
	return config.validate()
}

func (config ComponentConfig) validate() error {
	branches := 0
	if config.Caddy != nil {
		branches++
	}
	if config.Envoy != nil {
		branches++
		if !validComponentZoneIDs(config.Envoy.ZoneIDs) || !validRouterAlias(config.Envoy.Alias) {
			return invalidComponentConfig("Envoy config requires valid zone_ids and alias")
		}
	}
	if config.CloudflareTunnel != nil {
		branches++
	}
	if config.CoreDNS != nil {
		if err := dnsrecords.ValidateDNSRecords(config.CoreDNS.Records); err != nil {
			return err
		}
		branches++
	}
	if branches != 1 {
		return invalidComponentConfig("component config requires exactly one variant")
	}
	if config.Caddy != nil && (!validComponentZoneIDs(config.Caddy.ZoneIDs) || !validRouterAlias(config.Caddy.Alias)) {
		return invalidComponentConfig("component config Caddy zone_ids is invalid")
	}
	if config.CloudflareTunnel != nil && (!validStableID(config.CloudflareTunnel.SecretID, "sec_") ||
		!validComponentZoneIDs(config.CloudflareTunnel.ZoneIDs)) {
		return invalidComponentConfig("component config Cloudflare Tunnel zone_ids or secret_id is invalid")
	}
	if config.CoreDNS != nil {
		if config.CoreDNS.CorefileTemplate == "" || config.CoreDNS.UpstreamResolvers == nil ||
			config.CoreDNS.Forwarders == nil {
			return invalidComponentConfig("component config CoreDNS arrays are required")
		}
		for _, forwarder := range config.CoreDNS.Forwarders {
			if forwarder.Resolvers == nil {
				return invalidComponentConfig("component config CoreDNS forwarder resolvers are required")
			}
		}
	}
	return nil
}

func (config ComponentConfig) Validate() error { return config.validate() }

func decodeComponentConfigWire(data []byte, target any) error {
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return malformedComponentConfig("component config must be an object")
	}
	if err := rejectDuplicateJSONMembers(data); err != nil {
		return malformedComponentConfig("component config contains invalid or duplicate members")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return malformedComponentConfig("component config must be an object: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return malformedComponentConfig("component config contains trailing data")
	}
	return nil
}

func decodeRequiredField(raw json.RawMessage, name string, target any) error {
	if !present(raw) {
		return malformedComponentConfig("component config field %s is required", name)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return malformedComponentConfig("component config field %s must not be null", name)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return malformedComponentConfig("component config field %s is invalid: %v", name, err)
	}
	return nil
}

func decodeStringArray(raw json.RawMessage, name string) ([]string, error) {
	var values []json.RawMessage
	if err := decodeRequiredField(raw, name, &values); err != nil {
		return nil, err
	}
	result := make([]string, len(values))
	for index, value := range values {
		if err := decodeRequiredField(value, fmt.Sprintf("%s[%d]", name, index), &result[index]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type componentDNSForwarderWire struct {
	Domain    json.RawMessage `json:"domain"`
	Resolvers json.RawMessage `json:"resolvers"`
}

func decodeForwarders(raw json.RawMessage, name string) ([]ComponentDNSForwarder, error) {
	var values []json.RawMessage
	if err := decodeRequiredField(raw, name, &values); err != nil {
		return nil, err
	}
	result := make([]ComponentDNSForwarder, len(values))
	for index, value := range values {
		var wire componentDNSForwarderWire
		if err := decodeComponentConfigWire(value, &wire); err != nil {
			return nil, malformedComponentConfig("component config field %s[%d] is invalid: %v", name, index, err)
		}
		var domain string
		if err := decodeRequiredField(wire.Domain, fmt.Sprintf("%s[%d].domain", name, index), &domain); err != nil {
			return nil, err
		}
		resolvers, err := decodeStringArray(wire.Resolvers, fmt.Sprintf("%s[%d].resolvers", name, index))
		if err != nil {
			return nil, err
		}
		result[index] = ComponentDNSForwarder{Domain: domain, Resolvers: resolvers}
	}
	return result, nil
}

func malformedComponentConfig(format string, args ...any) error {
	return errs.Newf(errs.KindMalformedRequest, format, args...)
}

func invalidComponentConfig(format string, args ...any) error {
	return errs.Newf(errs.KindValidationFailed, format, args...)
}

func present(raw json.RawMessage) bool { return raw != nil }

func validRouterAlias(value string) bool {
	return value == "" || dnsname.ValidWithin(value, 63) && !strings.Contains(value, ".")
}

func validStableID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+26 {
		return false
	}
	for _, char := range value[len(prefix):] {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", char) {
			return false
		}
	}
	return true
}

func decodeComponentImplementation(raw json.RawMessage) (string, error) {
	var implementation string
	if err := decodeRequiredField(raw, "implementation", &implementation); err != nil {
		return "", err
	}
	switch implementation {
	case "caddy", "envoy", "cloudflare-tunnel", "coredns":
		return implementation, nil
	default:
		return "", malformedComponentConfig("unknown component implementation %q", implementation)
	}
}
