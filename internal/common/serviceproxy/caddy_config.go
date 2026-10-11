package serviceproxy

import (
	"encoding/json"
	"fmt"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type caddyDocument struct {
	Admin caddyAdmin `json:"admin"`
	Apps  caddyApps  `json:"apps"`
}

type caddyAdmin struct {
	Listen string `json:"listen"`
}
type caddyApps struct {
	HTTP caddyHTTP `json:"http"`
}
type caddyHTTP struct {
	Servers map[string]caddyServer `json:"servers"`
}
type caddyServer struct {
	Listen []string     `json:"listen"`
	Routes []caddyRoute `json:"routes"`
}
type caddyRoute struct {
	Handle []caddyHandler `json:"handle"`
}
type caddyHandler struct {
	Handler    string          `json:"handler"`
	Upstreams  []caddyUpstream `json:"upstreams,omitempty"`
	StatusCode int             `json:"status_code,omitempty"`
}
type caddyUpstream struct {
	Dial string `json:"dial"`
}

func renderCaddy(configuration Configuration) ([]byte, error) {
	servers := make(map[string]caddyServer, len(configuration.Ports))
	for _, port := range configuration.Ports {
		upstreams := make([]caddyUpstream, len(configuration.Upstreams))
		for index, upstream := range configuration.Upstreams {
			upstreams[index] = caddyUpstream{Dial: fmt.Sprintf("%s:%d", upstream, port.Number)}
		}
		handler := caddyHandler{Handler: "reverse_proxy", Upstreams: upstreams}
		if len(upstreams) == 0 {
			handler = caddyHandler{Handler: "static_response", StatusCode: 503}
		}
		servers[listenerName(configuration, port)] = caddyServer{
			Listen: []string{fmt.Sprintf(":%d", port.Number)},
			Routes: []caddyRoute{{Handle: []caddyHandler{handler}}},
		}
	}
	encoded, err := json.Marshal(caddyDocument{Admin: caddyAdmin{Listen: "127.0.0.1:2019"},
		Apps: caddyApps{HTTP: caddyHTTP{Servers: servers}}})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}
