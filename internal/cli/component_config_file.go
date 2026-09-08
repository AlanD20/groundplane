package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type cloudflareTunnelConfigFile struct {
	ZoneIDs    []string                                 `json:"zone_ids"`
	Credential apiTypes.CloudflareTunnelCredentialInput `json:"credential"`
}

func decodeCloudflareTunnelConfigFile(
	value string,
	allowMissingZoneIDs bool,
) (apiTypes.ComponentConfigMutationInput, error) {
	var file cloudflareTunnelConfigFile
	decoder := json.NewDecoder(bytes.NewReader([]byte(value)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return apiTypes.ComponentConfigMutationInput{}, fmt.Errorf(
			"component config file must contain one JSON object: %w",
			err,
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return apiTypes.ComponentConfigMutationInput{}, fmt.Errorf(
				"component config file must contain one JSON object",
			)
		}
		return apiTypes.ComponentConfigMutationInput{}, fmt.Errorf(
			"component config file must contain one JSON object: %w",
			err,
		)
	}
	if err := file.Credential.Validate(); err != nil {
		return apiTypes.ComponentConfigMutationInput{}, err
	}
	if len(file.ZoneIDs) == 0 && !allowMissingZoneIDs {
		return apiTypes.ComponentConfigMutationInput{}, fmt.Errorf(
			"Cloudflare Tunnel config requires zone_ids or explicit --zone/--create-zone placement",
		)
	}
	return apiTypes.ComponentConfigMutationInput{
		CloudflareTunnel: &apiTypes.CloudflareTunnelComponentConfigMutationInput{
			ZoneIDs:    append([]string(nil), file.ZoneIDs...),
			Credential: file.Credential,
		},
	}, nil
}
