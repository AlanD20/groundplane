package handlers

import "github.com/danielgtaylor/huma/v2"

func dnsAddressRecordSchema() *huma.Schema {
	return &huma.Schema{Type: huma.TypeObject, AdditionalProperties: false, Properties: map[string]*huma.Schema{
		"hostname": {Type: huma.TypeString}, "address": {Type: huma.TypeString, Format: "ipv4"},
	}, Required: []string{"hostname", "address"}}
}

func dnsServiceRecordSchema() *huma.Schema {
	return &huma.Schema{Type: huma.TypeObject, AdditionalProperties: false, Properties: map[string]*huma.Schema{
		"hostname": {
			Type: huma.TypeString,
		}, "service_id": {Type: huma.TypeString, Pattern: "^svc_[0-9A-HJKMNP-TV-Z]{26}$"}, "zone_id": {Type: huma.TypeString, Pattern: "^net_[0-9A-HJKMNP-TV-Z]{26}$"},
	}, Required: []string{"hostname", "service_id", "zone_id"}}
}
