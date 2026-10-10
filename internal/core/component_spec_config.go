package core

// ComponentConfigForSpec projects authored Component settings without runtime
// identities or defaults. Admission validates the closed implementation first.
func ComponentConfigForSpec(spec ComponentSpec) ComponentConfig {
	switch spec.Implementation {
	case ComponentKindIngressCaddy:
		if len(spec.Settings.ZoneIDs) == 0 && spec.ImplementationConfig.CaddyfileTemplate == "" &&
			spec.Settings.Alias == "" {
			return ComponentConfig{}
		}
		return ComponentConfig{Caddy: &CaddyComponentConfig{
			ZoneIDs:           append([]string(nil), spec.Settings.ZoneIDs...),
			CaddyfileTemplate: spec.ImplementationConfig.CaddyfileTemplate, Alias: spec.Settings.Alias,
		}}
	case ComponentKindEdgeCloudflare:
		if spec.Settings.SecretID == "" && len(spec.Settings.ZoneIDs) == 0 {
			return ComponentConfig{}
		}
		return ComponentConfig{CloudflareTunnel: &CloudflareTunnelComponentConfig{
			ZoneIDs: append([]string(nil), spec.Settings.ZoneIDs...), SecretID: spec.Settings.SecretID,
		}}
	default:
		return ComponentConfig{}
	}
}
