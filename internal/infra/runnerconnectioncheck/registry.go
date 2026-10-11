package runnerconnectioncheck

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
)

func checkRegistry(ctx context.Context, home string) (int, string) {
	if ctx.Err() != nil {
		return 0, connectionFailure(ctx.Err())
	}
	certificate, err := readFile(
		filepath.Join(home, ".config", "docker", "certs.d", imagefetch.RegistryAuthority, "ca.crt"),
		64<<10,
	)
	if err != nil {
		return 0, "tls"
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return 0, "tls"
	}
	value, err := readFile(filepath.Join(home, ".groundplane-docker", "config.json"), 64<<10)
	if err != nil {
		return 0, "credentials_unavailable"
	}
	defer clear(value)
	var configuration struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if json.Unmarshal(value, &configuration) != nil {
		return 0, "credentials_unavailable"
	}
	credential := configuration.Auths[imagefetch.RegistryAuthority].Auth
	decoded, err := base64.StdEncoding.DecodeString(credential)
	defer clear(decoded)
	separator := bytes.IndexByte(decoded, ':')
	if err != nil || len(decoded) > 4096 || separator <= 0 || separator >= len(decoded)-1 {
		return 0, "credentials_unavailable"
	}
	transport := newTransport()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	client := boundedClient(transport)
	defer client.CloseIdleConnections()
	status, body, failure := get(ctx, client, "https://"+imagefetch.RegistryAuthority+"/v2/", "Basic "+credential, 4096)
	if failure != "" {
		return status, failure
	}
	if status != http.StatusOK {
		return status, statusFailure(status)
	}
	if !json.Valid(body) {
		return status, "protocol"
	}
	return status, ""
}
