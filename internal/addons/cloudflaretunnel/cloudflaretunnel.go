// Package cloudflaretunnel is the "edge.cloudflare-tunnel" addon — the
// outbound-only edge gateway. See mvp.md, "Router (ingress add-ons)":
// the token registers the tunnel but cannot modify DNS; the operator is
// guided through the manual DNS process.
package cloudflaretunnel

import (
	"github.com/sample-tenant/groundplane/internal/addons"
	"github.com/sample-tenant/groundplane/internal/core"
	"github.com/sample-tenant/groundplane/pkg/errs"
)

// Register adds this addon to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	addons.Register(&addon{})
}

type addon struct{}

func (a *addon) Kind() core.AddonKind { return core.AddonKindEdgeCloudflare }
func (a *addon) Label() string        { return "Cloudflare Tunnel (outbound edge)" }
func (a *addon) ConfigSchema() []string {
	return []string{"tunnel_id"} // the token itself lives in the secret store, never in Config — see mvp.md
}

func (a *addon) Render(env core.Environment, ad core.Addon) (map[string]core.Service, map[string][]byte, error) {
	// TODO: render the cloudflared service wired from the
	// CLOUDFLARE_TUNNEL_TOKEN secret (secrets/.env.edge, exposed only to
	// this service) — see mvp.md's "Router" section.
	return nil, nil, errs.New(errs.CodeNotImplemented, "cloudflaretunnel: Render not implemented")
}

func (a *addon) Healthy(env core.Environment, ad core.Addon) (bool, error) {
	return false, errs.New(errs.CodeNotImplemented, "cloudflaretunnel: Healthy not implemented")
}
