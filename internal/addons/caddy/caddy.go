// Package caddy is the "ingress.caddy" addon — the entry router. See
// mvp.md, "Router (ingress add-ons)": one host-reachable service on a
// statically pinned IPv4, the documented exception to "no host port
// publishing".
package caddy

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

func (a *addon) Kind() core.AddonKind { return core.AddonKindIngressCaddy }
func (a *addon) Label() string        { return "Caddy (entry router)" }
func (a *addon) ConfigSchema() []string {
	return []string{"caddyfile_template"} // {slot}/{host} placeholders — see mvp.md
}

func (a *addon) Render(env core.Environment, ad core.Addon) (map[string]core.Service, map[string][]byte, error) {
	// TODO: render the Caddy service (pinned IPv4 on the frontend
	// bridge) plus the Caddyfile from ad.Config["caddyfile_template"],
	// validated before reload (StepReload) per the shared render path.
	return nil, nil, errs.New(errs.CodeNotImplemented, "caddy: Render not implemented")
}

func (a *addon) Healthy(env core.Environment, ad core.Addon) (bool, error) {
	return false, errs.New(errs.CodeNotImplemented, "caddy: Healthy not implemented")
}
