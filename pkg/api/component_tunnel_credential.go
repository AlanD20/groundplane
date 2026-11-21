package api

import "encoding/json"

type cloudflareTunnelCredentialWire struct {
	Mode       json.RawMessage `json:"mode"`
	TokenEntry json.RawMessage `json:"token_entry"`
	SecretID   json.RawMessage `json:"secret_id"`
	SecretName json.RawMessage `json:"secret_name"`
	Token      json.RawMessage `json:"token"`
}

func (input CloudflareTunnelCredentialInput) MarshalJSON() ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	switch input.Mode {
	case "environment":
		return json.Marshal(struct {
			Mode       string `json:"mode"`
			TokenEntry string `json:"token_entry"`
		}{input.Mode, input.TokenEntry})
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
	case "environment":
		if present(wire.SecretID) || present(wire.SecretName) || present(wire.Token) {
			return malformedComponentConfig("Environment Tunnel credential accepts only token_entry")
		}
		var key string
		if err := decodeRequiredField(wire.TokenEntry, "token_entry", &key); err != nil {
			return err
		}
		*input = CloudflareTunnelCredentialInput{Mode: mode, TokenEntry: key}
	case "existing":
		if present(wire.TokenEntry) || present(wire.SecretName) || present(wire.Token) {
			return malformedComponentConfig("Cloudflare Tunnel existing credential contains exclusive new fields")
		}
		var secretID string
		if err := decodeRequiredField(wire.SecretID, "secret_id", &secretID); err != nil {
			return err
		}
		*input = CloudflareTunnelCredentialInput{Mode: mode, SecretID: secretID}
	case "new":
		if present(wire.TokenEntry) || present(wire.SecretID) {
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
	if input.Mode != "environment" && input.TokenEntry != "" {
		return invalidComponentConfig("token_entry requires Environment credential mode")
	}
	switch input.Mode {
	case "environment":
		if input.TokenEntry == "" || input.SecretID != "" || input.SecretName != "" || input.Token != "" {
			return invalidComponentConfig("Environment Tunnel credential requires only token_entry")
		}
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
