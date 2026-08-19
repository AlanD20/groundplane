// Package caddy is the "ingress.caddy" component — the entry router. See
// mvp.md, "Router (ingress components)": one host-reachable service on a
// statically pinned IPv4, the documented exception to "no host port
// publishing".
package caddy

import (
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Register adds this component to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	components.Register(&component{})
}

type component struct{}

func (a *component) Kind() core.ComponentKind { return core.ComponentKindIngressCaddy }
func (a *component) Label() string        { return "Caddy (entry router)" }
func (a *component) ConfigSchema() []string {
	return []string{"caddyfile_template"} // {slot}/{host} placeholders — see mvp.md
}

func (a *component) Render(env core.Environment, ad core.Component) (map[string]core.Service, map[string][]byte, error) {
	// TODO: render the Caddy service (pinned IPv4 on the frontend
	// bridge) plus the Caddyfile from ad.Config["caddyfile_template"],
	// validated before reload (StepReload) per the shared render path.
	return nil, nil, errs.New(errs.CodeNotImplemented, "caddy: Render not implemented")
}

func (a *component) Healthy(env core.Environment, ad core.Component) (bool, error) {
	return false, errs.New(errs.CodeNotImplemented, "caddy: Healthy not implemented")
}
