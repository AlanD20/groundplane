package api

// Component is the generic environment-component record. Caddy and Cloudflare
// Tunnel are component KINDS ("caddy", "cloudflare-tunnel"),
// not bespoke resources — see blueprint.md, "x-gp-components".
type Component struct {
	ID                string           `json:"id"`
	Owner             string           `json:"owner" enum:"environment,platform"`
	OwnerID           string           `json:"owner_id,omitempty"`
	EnvironmentID     string           `json:"environment_id"`
	Kind              string           `json:"kind"`
	Enabled           bool             `json:"enabled"`
	Config            *ComponentConfig `json:"config"`
	GeneratedServices []string         `json:"generated_services,omitempty"`
	PinnedIPv4        string           `json:"pinned_ipv4,omitempty"`
	Healthy           bool             `json:"healthy"`
	Status            string           `json:"status" enum:"disabled,pending,healthy,degraded,unknown"`
}

// Router is the READ-ONLY projection grouping ingress components — GET
// only, never PUT (api-cli.md: "router | GET /environments/{id}/router
// read-only projection grouping ingress components").
type Router struct {
	Caddy  *ComponentProjection `json:"caddy,omitempty"`
	Tunnel *ComponentProjection `json:"tunnel,omitempty"`
}

type ComponentProjection struct {
	ComponentID string `json:"component_id"`
	Enabled     bool   `json:"enabled"`
	PinnedIPv4  string `json:"pinned_ipv4,omitempty"`
}

// ComponentConfig is the complete desired configuration singleton for one
// Component. Runtime and secret material are deliberately absent.
type ComponentConfig struct {
	Caddy            *CaddyComponentConfig            `json:"-"`
	CloudflareTunnel *CloudflareTunnelComponentConfig `json:"-"`
	CoreDNS          *CoreDNSComponentConfig          `json:"-"`
}

// ComponentConfigResponse is the generator-safe response envelope for the
// config singleton. Its nested config is null while disabled or unconfigured.
type ComponentConfigResponse struct {
	Config       *ComponentConfig    `json:"config"`
	ManagedFiles []ManagedConfigFile `json:"managed_files"`
}

// ManagedConfigFile is a side-effect-free Controller projection of one
// registered Component's authored template and current rendered content.
type ManagedConfigFile struct {
	Path     string `json:"path"`
	Template string `json:"template"`
	Rendered string `json:"rendered"`
}

type CaddyComponentConfig struct {
	Alias             string   `json:"alias,omitempty"`
	ZoneIDs           []string `json:"zone_ids"`
	CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
}

type CloudflareTunnelComponentConfig struct {
	ZoneIDs  []string `json:"zone_ids"`
	SecretID string   `json:"secret_id"`
}

type CoreDNSComponentConfig struct {
	CorefileTemplate  string                  `json:"corefile_template"`
	UpstreamAuto      bool                    `json:"upstream_auto"`
	UpstreamResolvers []string                `json:"upstream_resolvers"`
	Forwarders        []ComponentDNSForwarder `json:"forwarders"`
	TailnetDelegation bool                    `json:"tailnet_delegation"`
}

type ComponentDNSForwarder struct {
	Domain    string   `json:"domain"`
	Resolvers []string `json:"resolvers"`
}

type ComponentConfigMutationRequest struct {
	Config ComponentConfigMutationInput `json:"config"`
}

type ComponentConfigMutationInput struct {
	Caddy            *CaddyComponentConfigMutationInput            `json:"-"`
	CloudflareTunnel *CloudflareTunnelComponentConfigMutationInput `json:"-"`
	CoreDNS          *CoreDNSComponentConfigMutationInput          `json:"-"`
}

type CaddyComponentConfigMutationInput struct {
	Alias             string   `json:"alias,omitempty"`
	ZoneIDs           []string `json:"zone_ids"`
	CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
}

type CloudflareTunnelComponentConfigMutationInput struct {
	ZoneIDs    []string                        `json:"zone_ids"`
	Credential CloudflareTunnelCredentialInput `json:"credential"`
}

type CoreDNSComponentConfigMutationInput struct {
	CorefileTemplate  *string                  `json:"corefile_template"`
	UpstreamAuto      *bool                    `json:"upstream_auto"`
	UpstreamResolvers *[]string                `json:"upstream_resolvers"`
	Forwarders        *[]ComponentDNSForwarder `json:"forwarders"`
	TailnetDelegation *bool                    `json:"tailnet_delegation"`
}

type CloudflareTunnelCredentialInput struct {
	Mode       string `json:"mode"`
	SecretID   string `json:"secret_id,omitempty"`
	SecretName string `json:"secret_name,omitempty"`
	Token      string `json:"token,omitempty"`
}

type ComponentConfigMutationResult struct {
	Resource        ComponentConfig `json:"resource"`
	ReconcileTaskID *string         `json:"reconcile_task_id"`
}
