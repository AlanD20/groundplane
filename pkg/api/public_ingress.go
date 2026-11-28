package api

// PublicIngressDraft identifies approved Host bind addresses and existing
// Routes. Groundplane resolves all upstreams from acknowledged serving state.
type PublicIngressDraft struct {
	Enabled   bool     `json:"enabled"`
	Addresses []string `json:"addresses" maxItems:"16"`
	RouteIDs  []string `json:"route_ids" maxItems:"512"`
}

type PublicIngressConfiguration struct {
	PublicIngressDraft
	Revision string `json:"revision" pattern:"^(0|[1-9][0-9]{0,18})$"`
	State    string `json:"state" enum:"disabled,pending,serving,failed"`
	TaskID   string `json:"task_id,omitempty"`
	Message  string `json:"message,omitempty"`
}

// PublicIngressRouteOption contains operator-readable Route metadata only.
type PublicIngressRouteOption struct {
	ID          string `json:"id"`
	Hostname    string `json:"hostname"`
	Path        string `json:"path"`
	Environment string `json:"environment"`
	Service     string `json:"service"`
	Unavailable string `json:"unavailable,omitempty"`
}

type PublicIngressState struct {
	Configuration PublicIngressConfiguration `json:"configuration"`
	Routes        []PublicIngressRouteOption `json:"routes"`
}

type PublicIngressCheck struct {
	Scope   string `json:"scope"`
	Status  string `json:"status" enum:"passed,warning,blocked,not_checked"`
	Message string `json:"message"`
}

type PublicIngressReview struct {
	Revision    string               `json:"revision" pattern:"^(0|[1-9][0-9]{0,18})$"`
	ReviewToken string               `json:"review_token,omitempty"`
	CanApply    bool                 `json:"can_apply"`
	Checks      []PublicIngressCheck `json:"checks"`
}

type PublicIngressApplyRequest struct {
	PublicIngressDraft
	ReviewToken string `json:"review_token" minLength:"1" maxLength:"4096"`
}
