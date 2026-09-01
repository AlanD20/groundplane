package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// componentConfigWire deliberately keeps raw fields so that an explicitly
// supplied null cannot be mistaken for an omitted discriminator field.
type componentConfigWire struct {
	ZoneID            json.RawMessage `json:"zone_id"`
	CaddyfileTemplate json.RawMessage `json:"caddyfile_template"`
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
			ZoneID            string `json:"zone_id"`
			CaddyfileTemplate string `json:"caddyfile_template,omitempty"`
		}{config.Caddy.ZoneID, config.Caddy.CaddyfileTemplate})
	case config.CloudflareTunnel != nil:
		return json.Marshal(struct {
			SecretID string `json:"secret_id"`
		}{config.CloudflareTunnel.SecretID})
	default:
		// Do not use omitempty here: empty resolver and forwarder lists are
		// meaningful configured state and must remain JSON arrays.
		return json.Marshal(struct {
			CorefileTemplate  string                  `json:"corefile_template"`
			UpstreamAuto      bool                    `json:"upstream_auto"`
			UpstreamResolvers []string                `json:"upstream_resolvers"`
			Forwarders        []ComponentDNSForwarder `json:"forwarders"`
			TailnetDelegation bool                    `json:"tailnet_delegation"`
		}{
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
	if componentConfigBranches(wire) != 1 {
		return malformedComponentConfig("component config requires exactly one variant")
	}
	switch {
	case present(wire.ZoneID) || present(wire.CaddyfileTemplate):
		if present(wire.SecretID) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config Caddy variant is incomplete or mixed")
		}
		var zoneID, template string
		if err := decodeRequiredField(wire.ZoneID, "zone_id", &zoneID); err != nil {
			return err
		}
		if present(wire.CaddyfileTemplate) {
			if err := decodeRequiredField(wire.CaddyfileTemplate, "caddyfile_template", &template); err != nil {
				return err
			}
		}
		*config = ComponentConfig{Caddy: &CaddyComponentConfig{ZoneID: zoneID, CaddyfileTemplate: template}}
	case present(wire.SecretID):
		if present(wire.ZoneID) || present(wire.CaddyfileTemplate) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config Cloudflare Tunnel variant is mixed")
		}
		var secretID string
		if err := decodeRequiredField(wire.SecretID, "secret_id", &secretID); err != nil {
			return err
		}
		*config = ComponentConfig{CloudflareTunnel: &CloudflareTunnelComponentConfig{SecretID: secretID}}
	default:
		if present(wire.ZoneID) || present(wire.CaddyfileTemplate) || present(wire.SecretID) || !present(wire.CorefileTemplate) || !present(wire.UpstreamAuto) || !present(wire.UpstreamResolvers) || !present(wire.Forwarders) || !present(wire.TailnetDelegation) {
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
		*config = ComponentConfig{CoreDNS: &CoreDNSComponentConfig{
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
	if config.CloudflareTunnel != nil {
		branches++
	}
	if config.CoreDNS != nil {
		branches++
	}
	if branches != 1 {
		return invalidComponentConfig("component config requires exactly one variant")
	}
	if config.Caddy != nil && !validStableID(config.Caddy.ZoneID, "net_") {
		return invalidComponentConfig("component config Caddy zone_id is invalid")
	}
	if config.CloudflareTunnel != nil && !validStableID(config.CloudflareTunnel.SecretID, "sec_") {
		return invalidComponentConfig("component config Cloudflare Tunnel secret_id is invalid")
	}
	if config.CoreDNS != nil {
		if config.CoreDNS.CorefileTemplate == "" || config.CoreDNS.UpstreamResolvers == nil || config.CoreDNS.Forwarders == nil {
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

func componentConfigBranches(wire componentConfigWire) int {
	branches := 0
	if present(wire.ZoneID) || present(wire.CaddyfileTemplate) {
		branches++
	}
	if present(wire.SecretID) {
		branches++
	}
	if present(wire.CorefileTemplate) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
		branches++
	}
	return branches
}

func decodeComponentConfigWire(data []byte, target any) error {
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return malformedComponentConfig("component config must be an object")
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

type componentConfigMutationWire struct {
	ZoneID            json.RawMessage `json:"zone_id"`
	CaddyfileTemplate json.RawMessage `json:"caddyfile_template"`
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
			ZoneID            string `json:"zone_id"`
			CaddyfileTemplate string `json:"caddyfile_template,omitempty"`
		}{input.Caddy.ZoneID, input.Caddy.CaddyfileTemplate})
	case input.CloudflareTunnel != nil:
		return json.Marshal(struct {
			Credential CloudflareTunnelCredentialInput `json:"credential"`
		}{input.CloudflareTunnel.Credential})
	default:
		return json.Marshal(struct {
			CorefileTemplate  string                  `json:"corefile_template"`
			UpstreamAuto      bool                    `json:"upstream_auto"`
			UpstreamResolvers []string                `json:"upstream_resolvers"`
			Forwarders        []ComponentDNSForwarder `json:"forwarders"`
			TailnetDelegation bool                    `json:"tailnet_delegation"`
		}{
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
	branches := 0
	if present(wire.ZoneID) || present(wire.CaddyfileTemplate) {
		branches++
	}
	if present(wire.Credential) {
		branches++
	}
	if present(wire.CorefileTemplate) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
		branches++
	}
	if branches != 1 {
		return malformedComponentConfig("component config mutation requires exactly one variant")
	}
	switch {
	case present(wire.ZoneID) || present(wire.CaddyfileTemplate):
		if present(wire.Credential) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config mutation Caddy variant is incomplete or mixed")
		}
		var zoneID, template string
		if err := decodeRequiredField(wire.ZoneID, "zone_id", &zoneID); err != nil {
			return err
		}
		if present(wire.CaddyfileTemplate) {
			if err := decodeRequiredField(wire.CaddyfileTemplate, "caddyfile_template", &template); err != nil {
				return err
			}
		}
		*input = ComponentConfigMutationInput{Caddy: &CaddyComponentConfigMutationInput{ZoneID: zoneID, CaddyfileTemplate: template}}
	case present(wire.Credential):
		if present(wire.ZoneID) || present(wire.CaddyfileTemplate) || present(wire.UpstreamAuto) || present(wire.UpstreamResolvers) || present(wire.Forwarders) || present(wire.TailnetDelegation) {
			return malformedComponentConfig("component config mutation Cloudflare Tunnel variant is mixed")
		}
		var credential CloudflareTunnelCredentialInput
		if err := decodeRequiredField(wire.Credential, "credential", &credential); err != nil {
			return err
		}
		*input = ComponentConfigMutationInput{CloudflareTunnel: &CloudflareTunnelComponentConfigMutationInput{Credential: credential}}
	default:
		if present(wire.ZoneID) || present(wire.CaddyfileTemplate) || present(wire.Credential) || !present(wire.CorefileTemplate) || !present(wire.UpstreamAuto) || !present(wire.UpstreamResolvers) || !present(wire.Forwarders) || !present(wire.TailnetDelegation) {
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
		*input = ComponentConfigMutationInput{CoreDNS: &CoreDNSComponentConfigMutationInput{
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
	if input.CloudflareTunnel != nil {
		branches++
	}
	if input.CoreDNS != nil {
		branches++
	}
	if branches != 1 {
		return invalidComponentConfig("component config mutation requires exactly one variant")
	}
	if input.Caddy != nil && !validStableID(input.Caddy.ZoneID, "net_") {
		return invalidComponentConfig("component config mutation Caddy zone_id is invalid")
	}
	if input.CloudflareTunnel != nil {
		if err := input.CloudflareTunnel.Credential.Validate(); err != nil {
			return err
		}
	}
	if input.CoreDNS != nil {
		if input.CoreDNS.CorefileTemplate == nil || input.CoreDNS.UpstreamAuto == nil || input.CoreDNS.UpstreamResolvers == nil || input.CoreDNS.Forwarders == nil || input.CoreDNS.TailnetDelegation == nil {
			return invalidComponentConfig("component config mutation CoreDNS variant requires every field")
		}
		if *input.CoreDNS.CorefileTemplate == "" || *input.CoreDNS.UpstreamResolvers == nil || *input.CoreDNS.Forwarders == nil {
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

type cloudflareTunnelCredentialWire struct {
	Mode       json.RawMessage `json:"mode"`
	SecretID   json.RawMessage `json:"secret_id"`
	SecretName json.RawMessage `json:"secret_name"`
	Token      json.RawMessage `json:"token"`
}

func (input CloudflareTunnelCredentialInput) MarshalJSON() ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	switch input.Mode {
	case "existing":
		return json.Marshal(struct {
			Mode     string `json:"mode"`
			SecretID string `json:"secret_id"`
		}{input.Mode, input.SecretID})
	default:
		return json.Marshal(struct {
			Mode       string `json:"mode"`
			SecretName string `json:"secret_name"`
			Token      string `json:"token"`
		}{input.Mode, input.SecretName, input.Token})
	}
}

func (input *CloudflareTunnelCredentialInput) UnmarshalJSON(data []byte) error {
	var wire cloudflareTunnelCredentialWire
	if err := decodeComponentConfigWire(data, &wire); err != nil {
		return err
	}
	var mode string
	if err := decodeRequiredField(wire.Mode, "mode", &mode); err != nil {
		return err
	}
	switch mode {
	case "existing":
		if present(wire.SecretName) || present(wire.Token) {
			return malformedComponentConfig("Cloudflare Tunnel existing credential contains exclusive new fields")
		}
		var secretID string
		if err := decodeRequiredField(wire.SecretID, "secret_id", &secretID); err != nil {
			return err
		}
		*input = CloudflareTunnelCredentialInput{Mode: mode, SecretID: secretID}
	case "new":
		if present(wire.SecretID) {
			return malformedComponentConfig("Cloudflare Tunnel new credential contains exclusive existing field")
		}
		var secretName, token string
		if err := decodeRequiredField(wire.SecretName, "secret_name", &secretName); err != nil {
			return err
		}
		if err := decodeRequiredField(wire.Token, "token", &token); err != nil {
			return err
		}
		*input = CloudflareTunnelCredentialInput{Mode: mode, SecretName: secretName, Token: token}
	default:
		return malformedComponentConfig("Cloudflare Tunnel credential mode is invalid")
	}
	return input.Validate()
}

func (input CloudflareTunnelCredentialInput) Validate() error {
	switch input.Mode {
	case "existing":
		if !validStableID(input.SecretID, "sec_") || input.SecretName != "" || input.Token != "" {
			return invalidComponentConfig("Cloudflare Tunnel existing credential requires only a valid secret_id")
		}
	case "new":
		if input.SecretID != "" || input.SecretName == "" || input.Token == "" {
			return invalidComponentConfig("Cloudflare Tunnel new credential requires secret_name and token only")
		}
	default:
		return invalidComponentConfig("Cloudflare Tunnel credential mode is invalid")
	}
	return nil
}
