// Package cloudflaretunnel is the "cloudflare-tunnel" component — the
// outbound-only edge gateway. See mvp.md, "Router (ingress components)":
// the token registers the tunnel but cannot modify DNS; the operator is
// guided through the manual DNS process.
package cloudflaretunnel

import (
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Register adds this component to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	implementation := &component{}
	components.Register(components.Registration{
		Kind:          core.ComponentKindEdgeCloudflare,
		Label:         "Cloudflare Tunnel (outbound edge)",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerEnvironment},
		ApplyStrategy: components.EnvironmentRender,
		ConfigSchema:  []string{"tunnel_id"},
		Environment:   implementation,
	})
}

type component struct{}

func (a *component) Render(env core.Environment, ad core.Component) (map[string]core.Service, map[string][]byte, error) {
	// TODO: render the cloudflared service wired from the
	// CLOUDFLARE_TUNNEL_TOKEN secret (secrets/.env.edge, exposed only to
	// this service) — see mvp.md's "Router" section.
	return nil, nil, errs.New(errs.KindNotImplemented, "cloudflaretunnel: Render not implemented")
}

func (a *component) Healthy(env core.Environment, ad core.Component) (bool, error) {
	return false, errs.New(errs.KindNotImplemented, "cloudflaretunnel: Healthy not implemented")
}
