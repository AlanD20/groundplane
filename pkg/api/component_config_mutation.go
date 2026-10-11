package api

import (
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
)

type componentConfigMutationWire struct {
	Implementation    json.RawMessage `json:"implementation"`
	Records           json.RawMessage `json:"records"`
	ZoneIDs           json.RawMessage `json:"zone_ids"`
	CaddyfileTemplate json.RawMessage `json:"caddyfile_template"`
	Alias             json.RawMessage `json:"alias"`
	Credential        json.RawMessage `json:"credential"`
	UpstreamAuto      json.RawMessage `json:"upstream_auto"`
	UpstreamResolvers json.RawMessage `json:"upstream_resolvers"`
	Forwarders        json.RawMessage `json:"forwarders"`
	TailnetDelegation json.RawMessage `json:"tailnet_delegation"`
	CorefileTemplate  json.RawMessage `json:"corefile_template"`
}

func (input ComponentConfigMutationInput) MarshalJSON() ([]byte, error) {
	if err := input.validate(); err != nil {
		return nil, err
	}
	switch {
	case input.Caddy != nil:
		return json.Marshal(struct {
			Implementation    string   `json:"implementation"`
			ZoneIDs           []string `json:"zone_ids"`
			CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
			Alias             string   `json:"alias,omitempty"`
		}{"caddy", input.Caddy.ZoneIDs, input.Caddy.CaddyfileTemplate, input.Caddy.Alias})
	case input.Envoy != nil:
		return json.Marshal(struct {
			Implementation string `json:"implementation"`
			*EnvoyComponentConfigMutationInput
		}{"envoy", input.Envoy})
	case input.CloudflareTunnel != nil:
		return json.Marshal(struct {
			Implementation string                          `json:"implementation"`
			ZoneIDs        []string                        `json:"zone_ids"`
			Credential     CloudflareTunnelCredentialInput `json:"credential"`
		}{"cloudflare-tunnel", input.CloudflareTunnel.ZoneIDs, input.CloudflareTunnel.Credential})
	default:
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
			input.CoreDNS.Records,
			*input.CoreDNS.CorefileTemplate,
			*input.CoreDNS.UpstreamAuto, *input.CoreDNS.UpstreamResolvers,
			*input.CoreDNS.Forwarders, *input.CoreDNS.TailnetDelegation,
		})
	}
}

func (input *ComponentConfigMutationInput) UnmarshalJSON(data []byte) error {
	var wire componentConfigMutationWire
	if err := decodeComponentConfigWire(data, &wire); err != nil {
		return err
	}
	implementation, err := decodeComponentImplementation(wire.Implementation)
	if err != nil {
		return err
	}
	switch implementation {
	case "caddy", "envoy":
		if present(wire.Credential) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) ||
			present(wire.Forwarders) ||
			present(wire.TailnetDelegation) || present(wire.Records) || present(wire.CorefileTemplate) {
			return malformedComponentConfig("component config mutation router variant is incomplete or mixed")
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
			*input = ComponentConfigMutationInput{
				Envoy: &EnvoyComponentConfigMutationInput{ZoneIDs: zoneIDs, Alias: alias},
			}
		} else {
			*input = ComponentConfigMutationInput{Caddy: &CaddyComponentConfigMutationInput{ZoneIDs: zoneIDs, CaddyfileTemplate: template, Alias: alias}}
		}
	case "cloudflare-tunnel":
		if (present(wire.CaddyfileTemplate) || present(wire.Alias)) || present(wire.UpstreamAuto) ||
			present(wire.UpstreamResolvers) ||
			present(wire.Forwarders) ||
			present(wire.TailnetDelegation) || present(wire.Records) || present(wire.CorefileTemplate) {
			return malformedComponentConfig("component config mutation Cloudflare Tunnel variant is mixed")
		}
		var credential CloudflareTunnelCredentialInput
		if err := decodeRequiredField(wire.Credential, "credential", &credential); err != nil {
			return err
		}
		zoneIDs, err := decodeStringArray(wire.ZoneIDs, "zone_ids")
		if err != nil {
			return err
		}
		*input = ComponentConfigMutationInput{
			CloudflareTunnel: &CloudflareTunnelComponentConfigMutationInput{ZoneIDs: zoneIDs, Credential: credential},
		}
	case "coredns":
		if present(wire.ZoneIDs) || (present(wire.CaddyfileTemplate) || present(wire.Alias)) ||
			present(wire.Credential) ||
			!present(wire.CorefileTemplate) ||
			!present(wire.UpstreamAuto) ||
			!present(wire.UpstreamResolvers) ||
			!present(wire.Forwarders) ||
			!present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config mutation CoreDNS variant is incomplete or mixed")
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
		*input = ComponentConfigMutationInput{CoreDNS: &CoreDNSComponentConfigMutationInput{
			Records:          records,
			CorefileTemplate: &corefileTemplate,
			UpstreamAuto:     &upstreamAuto, UpstreamResolvers: &upstreamResolvers,
			Forwarders: &forwarders, TailnetDelegation: &tailnetDelegation,
		}}
	}
	return input.validate()
}

func (input ComponentConfigMutationInput) validate() error {
	branches := 0
	if input.Caddy != nil {
		branches++
	}
	if input.Envoy != nil {
		branches++
		if !validComponentZoneIDs(input.Envoy.ZoneIDs) || !validRouterAlias(input.Envoy.Alias) {
			return invalidComponentConfig("Envoy config requires valid zone_ids and alias")
		}
	}
	if input.CloudflareTunnel != nil {
		branches++
	}
	if input.CoreDNS != nil {
		if err := dnsrecords.ValidateDNSRecords(input.CoreDNS.Records); err != nil {
			return err
		}
		branches++
	}
	if branches != 1 {
		return invalidComponentConfig("component config mutation requires exactly one variant")
	}
	if input.Caddy != nil && (!validComponentZoneIDs(input.Caddy.ZoneIDs) || !validRouterAlias(input.Caddy.Alias)) {
		return invalidComponentConfig("component config mutation Caddy zone_ids is invalid")
	}
	if input.CloudflareTunnel != nil {
		if !validComponentZoneIDs(input.CloudflareTunnel.ZoneIDs) {
			return invalidComponentConfig("component config mutation Cloudflare Tunnel zone_ids is invalid")
		}
		if err := input.CloudflareTunnel.Credential.Validate(); err != nil {
			return err
		}
	}
	if input.CoreDNS != nil {
		if input.CoreDNS.CorefileTemplate == nil || input.CoreDNS.UpstreamAuto == nil ||
			input.CoreDNS.UpstreamResolvers == nil ||
			input.CoreDNS.Forwarders == nil ||
			input.CoreDNS.TailnetDelegation == nil {
			return invalidComponentConfig("component config mutation CoreDNS variant requires every field")
		}
		if *input.CoreDNS.CorefileTemplate == "" || *input.CoreDNS.UpstreamResolvers == nil ||
			*input.CoreDNS.Forwarders == nil {
			return invalidComponentConfig("component config mutation CoreDNS arrays are required")
		}
		for _, forwarder := range *input.CoreDNS.Forwarders {
			if forwarder.Resolvers == nil {
				return invalidComponentConfig("component config mutation CoreDNS forwarder resolvers are required")
			}
		}
	}
	return nil
}

func (input ComponentConfigMutationInput) Validate() error { return input.validate() }
