package publicingress

import (
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
)

const (
	// Existing streams stay attached to the previous configuration during
	// a deliberate route reload. A software upgrade keeps Caddy running and
	// does not reload unchanged routing configuration.
	streamDrainWindow     = 5 * time.Minute
	administrationAddress = "127.0.0.1:2019"
)

type caddyConfig struct {
	Admin caddyAdmin `json:"admin"`
	Apps  caddyApps  `json:"apps"`
}

type caddyAdmin struct {
	Listen string `json:"listen"`
	Config struct {
		Persist bool `json:"persist"`
	} `json:"config"`
}

type caddyApps struct {
	HTTP caddyHTTP `json:"http"`
	TLS  *caddyTLS `json:"tls,omitempty"`
}

type caddyHTTP struct {
	Servers struct {
		Plain  caddyServer `json:"public-http"`
		Secure caddyServer `json:"public-https"`
	} `json:"servers"`
}

type caddyTLS struct {
	Automation struct {
		Policies []caddyCertificatePolicy `json:"policies"`
	} `json:"automation"`
}

type caddyCertificatePolicy struct {
	Subjects []string      `json:"subjects"`
	Issuers  []caddyIssuer `json:"issuers"`
}

type caddyIssuer struct {
	Module string `json:"module"`
}

type caddyServer struct {
	Listen         []string                `json:"listen"`
	Protocols      []string                `json:"protocols"`
	Routes         []caddyRoute            `json:"routes"`
	AutomaticHTTPS caddyAutomaticHTTPS     `json:"automatic_https"`
	StrictSNIHost  bool                    `json:"strict_sni_host,omitempty"`
	TLSPolicies    []caddyConnectionPolicy `json:"tls_connection_policies,omitempty"`
}

type caddyConnectionPolicy struct {
	Match *caddySNI `json:"match,omitempty"`
}

type caddySNI struct {
	Names []string `json:"sni"`
}

type caddyAutomaticHTTPS struct {
	Disable          bool `json:"disable,omitempty"`
	DisableRedirects bool `json:"disable_redirects,omitempty"`
}

type caddyRoute struct {
	Match    []caddyMatch   `json:"match,omitempty"`
	Handle   []caddyHandler `json:"handle"`
	Terminal bool           `json:"terminal"`
}

type caddyMatch struct {
	Host []string `json:"host"`
	Path []string `json:"path"`
}

type caddyHandler struct {
	Kind             string                 `json:"handler"`
	StatusCode       int                    `json:"status_code,omitempty"`
	Headers          *caddyHeaderOperations `json:"headers,omitempty"`
	Upstreams        []caddyUpstream        `json:"upstreams,omitempty"`
	Transport        *caddyTransport        `json:"transport,omitempty"`
	StreamCloseDelay time.Duration          `json:"stream_close_delay,omitempty"`
}

type caddyTransport struct {
	Protocol    string        `json:"protocol"`
	DialTimeout time.Duration `json:"dial_timeout"`
}

type caddyUpstream struct {
	Dial string `json:"dial"`
}

type caddyHeaderOperations struct {
	Request  *caddyRequestHeaders `json:"request,omitempty"`
	Location []string             `json:"Location,omitempty"`
}

type caddyRequestHeaders struct {
	Delete []string            `json:"delete"`
	Set    map[string][]string `json:"set,omitempty"`
}

func render(routes []Route) ([]byte, error) {
	var config caddyConfig
	config.Admin.Listen = administrationAddress
	secure := &config.Apps.HTTP.Servers.Secure
	secure.Listen = []string{":443"}
	secure.Protocols = []string{"h1", "h2"}
	secure.StrictSNIHost = true
	secure.AutomaticHTTPS.DisableRedirects = true
	plain := &config.Apps.HTTP.Servers.Plain
	plain.Listen = []string{":80"}
	plain.Protocols = []string{"h1"}
	plain.AutomaticHTTPS.Disable = true

	hosts := make([]string, 0, len(routes))
	for _, route := range routes {
		host := route.Source.Host
		if len(hosts) == 0 || hosts[len(hosts)-1] != host {
			hosts = append(hosts, host)
		}
		match := []caddyMatch{{Host: []string{host}, Path: []string{matchPath(route.Source.Path)}}}
		dial := route.Endpoint.Address
		headers := &caddyRequestHeaders{Delete: []string{
			"Forwarded", "X-Real-IP", "X-Forwarded-Port", "X-Forwarded-Prefix", "X-Forwarded-Ssl",
		}}
		if route.Endpoint.Relay.IsValid() {
			dial = route.Endpoint.Relay
			headers.Set = map[string][]string{
				ingressexecution.RelayOriginHeader: {route.Endpoint.Address.String()},
				ingressexecution.RelayProofHeader:  {route.Endpoint.RelayProof},
			}
		}
		secure.Routes = append(secure.Routes, caddyRoute{
			Match: match, Terminal: true,
			Handle: []caddyHandler{{
				Kind: "reverse_proxy", Upstreams: []caddyUpstream{{Dial: dial.String()}},
				Transport:        &caddyTransport{Protocol: "http", DialTimeout: 3 * time.Second},
				Headers:          &caddyHeaderOperations{Request: headers},
				StreamCloseDelay: streamDrainWindow,
			}},
		})
		plain.Routes = append(plain.Routes, caddyRoute{
			Match: match, Terminal: true,
			Handle: []caddyHandler{{
				Kind: "static_response", StatusCode: 308,
				Headers: &caddyHeaderOperations{Location: []string{"https://" + host + "{http.request.uri}"}},
			}},
		})
	}

	secure.Routes = append(secure.Routes, caddyRoute{
		Handle: []caddyHandler{{Kind: "static_response", StatusCode: 404}}, Terminal: true,
	})
	plain.Routes = append(plain.Routes, caddyRoute{
		Handle: []caddyHandler{{Kind: "static_response", StatusCode: 404}}, Terminal: true,
	})
	secure.TLSPolicies = []caddyConnectionPolicy{{}}
	if len(hosts) > 0 {
		config.Apps.TLS = &caddyTLS{}
		config.Apps.TLS.Automation.Policies = []caddyCertificatePolicy{{
			Subjects: hosts,
			Issuers:  []caddyIssuer{{Module: "acme"}},
		}}
		secure.TLSPolicies[0].Match = &caddySNI{Names: hosts}
	}

	return json.Marshal(config)
}
