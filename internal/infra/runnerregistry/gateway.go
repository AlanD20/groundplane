package runnerregistry

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type GatewayConfig struct {
	Backend           *url.URL
	BackendUsername   string
	BackendCredential string
	TLSRoots          *x509.CertPool
}

// Gateway fronts a private Registry and checks the current job grant for each
// Distribution request. Its backend must be unreachable from Runner networks.
type Gateway struct {
	backend    url.URL
	username   string
	credential string
	transport  http.RoundTripper
	authorizer JobAuthorizer
}

func NewGateway(config GatewayConfig, authorizer JobAuthorizer) (*Gateway, error) {
	backend := config.Backend
	if authorizer == nil || backend == nil || backend.Scheme != "https" || backend.Hostname() == "" ||
		backend.User != nil || backend.Opaque != "" || backend.ForceQuery || backend.RawQuery != "" ||
		backend.Fragment != "" || backend.RawFragment != "" || backend.RawPath != "" ||
		(backend.Path != "" && backend.Path != "/") || config.TLSRoots == nil ||
		!visibleASCII(config.BackendUsername) || strings.Contains(config.BackendUsername, ":") ||
		config.BackendCredential == "" || len(config.BackendCredential) > maxCredentialBytes {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry gateway configuration is invalid")
	}
	return &Gateway{
		backend: *backend, username: config.BackendUsername, credential: config.BackendCredential,
		authorizer: authorizer,
		transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSClientConfig:       &tls.Config{RootCAs: config.TLSRoots.Clone(), MinVersion: tls.VersionTLS12},
			DisableCompression:    true,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}, nil
}

func (gateway *Gateway) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if request.TLS == nil {
		http.Error(w, "Runner registry requires TLS", http.StatusForbidden)
		return
	}
	if !canonicalRegistryPath(request.URL) || len(request.URL.RawQuery) > maxQueryBytes {
		http.Error(w, "Registry request is invalid", http.StatusBadRequest)
		return
	}
	credentials := request.Header.Values("Authorization")
	username, credential, ok := request.BasicAuth()
	if len(credentials) != 1 || len(credentials[0]) > maxCredentialBytes || !ok || !visibleASCII(username) ||
		credential == "" {
		unauthorized(w)
		return
	}
	grant, err := gateway.authorizer.AuthorizeRegistryJob(request.Context(), username, credential)
	if err != nil || !grant.valid(time.Now()) {
		unauthorized(w)
		return
	}
	query, allowed := registryRequest(request, grant)
	if !allowed {
		http.Error(w, "Registry operation is outside the job grant", http.StatusForbidden)
		return
	}
	proxy := httputil.ReverseProxy{
		Transport: gateway.transport,
		Rewrite: func(exchange *httputil.ProxyRequest) {
			exchange.SetURL(&gateway.backend)
			exchange.Out.URL.RawQuery = query.Encode()
			exchange.Out.Header = registryHeaders(exchange.Out.Header, registryRequestHeaders)
			exchange.Out.Trailer = nil
			exchange.Out.SetBasicAuth(gateway.username, gateway.credential)
		},
		ModifyResponse: func(response *http.Response) error {
			return gateway.filterResponse(response, grant.Repository)
		},
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(writer, "Registry backend is unavailable", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, request)
}

var registryRequestHeaders = []string{
	"Accept", "Content-Type", "Content-Range", "Range", "If-Match", "If-None-Match",
	"If-Modified-Since", "If-Unmodified-Since", "User-Agent",
}

var registryResponseHeaders = []string{
	"Content-Type", "Content-Length", "Content-Range", "Content-Encoding", "Accept-Ranges",
	"Docker-Content-Digest", "Docker-Upload-UUID", "Etag", "Last-Modified", "Range", "Retry-After",
}

func registryHeaders(source http.Header, allowed []string) http.Header {
	headers := make(http.Header, len(allowed))
	for _, name := range allowed {
		if values := source.Values(name); len(values) != 0 {
			headers[http.CanonicalHeaderKey(name)] = values
		}
	}
	return headers
}

func (gateway *Gateway) filterResponse(response *http.Response, repository string) error {
	status := response.StatusCode
	if status < http.StatusOK || (status >= 300 && status < 400 && status != http.StatusNotModified) ||
		status == http.StatusUnauthorized || status == http.StatusForbidden || status >= 500 {
		return errs.New(errs.KindInternal, "Registry backend returned an unusable response")
	}
	locations := response.Header.Values("Location")
	response.Header = registryHeaders(response.Header, registryResponseHeaders)
	response.Trailer = nil
	if len(locations) == 0 {
		return nil
	}
	if len(locations) != 1 {
		return errs.New(errs.KindInternal, "Registry backend returned multiple locations")
	}
	location, err := url.Parse(locations[0])
	if err != nil || !canonicalRegistryPath(location) || len(location.RawQuery) > maxQueryBytes ||
		location.Fragment != "" ||
		(location.IsAbs() && (location.Scheme != "https" || !strings.EqualFold(location.Host, gateway.backend.Host))) ||
		(!location.IsAbs() && location.Host != "") {
		return errs.New(errs.KindInternal, "Registry backend returned an invalid location")
	}
	kind := scopedRegistryPath(location.Path, repository)
	query, err := url.ParseQuery(location.RawQuery)
	if err != nil || !registryLocation(kind, query) {
		return errs.New(errs.KindInternal, "Registry backend returned an out-of-scope location")
	}
	location.RawQuery = query.Encode()
	response.Header.Set("Location", location.RequestURI())
	return nil
}
