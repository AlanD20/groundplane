// Package cloudflaretunnel is the "edge.cloudflare-tunnel" component — the
// outbound-only edge gateway. See mvp.md, "Router (ingress components)":
// the token registers the tunnel but cannot modify DNS; the operator is
// guided through the manual DNS process.
package cloudflaretunnel

import (
	"github.com/sample-tenant/groundplane/internal/components"
	"github.com/sample-tenant/groundplane/internal/core"
	"github.com/sample-tenant/groundplane/pkg/errs"
)

// Register adds this component to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	components.Register(&component{})
}

type component struct{}

func (a *component) Kind() core.ComponentKind { return core.ComponentKindEdgeCloudflare }
func (a *component) Label() string        { return "Cloudflare Tunnel (outbound edge)" }
func (a *component) ConfigSchema() []string {
	return []string{"tunnel_id"} // the token itself lives in the secret store, never in Config — see mvp.md
}

func (a *component) Render(env core.Environment, ad core.Component) (map[string]core.Service, map[string][]byte, error) {
	// TODO: render the cloudflared service wired from the
	// CLOUDFLARE_TUNNEL_TOKEN secret (secrets/.env.edge, exposed only to
	// this service) — see mvp.md's "Router" section.
	return nil, nil, errs.New(errs.CodeNotImplemented, "cloudflaretunnel: Render not implemented")
}

func (a *component) Healthy(env core.Environment, ad core.Component) (bool, error) {
	return false, errs.New(errs.CodeNotImplemented, "cloudflaretunnel: Healthy not implemented")
}
