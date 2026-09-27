// Package registryimages resolves private registry content and explicitly loads
// it into host Docker. The workload Deploy resolver remains read-only.
package registryimages

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/types/registry"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const maximumDocumentBytes = 4 << 20

type Engine interface {
	ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error)
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
}

type Client struct {
	http         *http.Client
	engine       Engine
	username     string
	password     string
	architecture string
	ownedEngine  io.Closer
}

func New(engine Engine, caPEM []byte, username, password, architecture string) (*Client, error) {
	if engine == nil || username == "" || password == "" || (architecture != "amd64" && architecture != "arm64") {
		return nil, errs.New(errs.KindInternal, "private registry client configuration is incomplete")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errs.New(errs.KindInternal, "private registry trust is invalid")
	}
	transport := &http.Transport{
		TLSClientConfig:     &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second,
		IdleConnTimeout: time.Minute, MaxIdleConnsPerHost: 2,
	}
	return &Client{
		http: &http.Client{
			Transport: transport, Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		engine: engine, username: username, password: password, architecture: architecture,
	}, nil
}

func (registryClient *Client) Close() error {
	registryClient.http.CloseIdleConnections()
	if registryClient.ownedEngine != nil {
		return registryClient.ownedEngine.Close()
	}
	return nil
}

func (registryClient *Client) document(ctx context.Context, repository, kind, selector string) ([]byte, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://"+imagefetch.RegistryAuthority+"/v2/"+repository+"/"+kind+"/"+selector, nil)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	request.SetBasicAuth(registryClient.username, registryClient.password)
	request.Header.Set("Accept", ocispec.MediaTypeImageIndex+", "+ocispec.MediaTypeImageManifest+
		", application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json")
	response, err := registryClient.http.Do(request)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, "", errs.Newf(errs.KindStateConflict, "private registry returned HTTP %d", response.StatusCode)
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, maximumDocumentBytes+1))
	if err != nil || len(value) == 0 || len(value) > maximumDocumentBytes {
		return nil, "", errs.New(errs.KindStateConflict, "private registry document is unreadable or oversized")
	}
	actual := digest.FromBytes(value).String()
	if (kind == "blobs" || len(selector) > 7 && selector[:7] == "sha256:") && actual != selector {
		return nil, "", errs.New(errs.KindStateConflict, "private registry content differs from the selected digest")
	}
	if advertised := response.Header.Get("Docker-Content-Digest"); advertised != "" && advertised != actual {
		return nil, "", errs.New(errs.KindStateConflict, "private registry content digest is inconsistent")
	}
	return value, actual, nil
}

func (registryClient *Client) dockerAuthentication() (string, error) {
	value, err := json.Marshal(registry.AuthConfig{
		Username: registryClient.username, Password: registryClient.password,
		ServerAddress: imagefetch.RegistryAuthority,
	})
	if err != nil {
		return "", err
	}
	defer clear(value)
	return base64.URLEncoding.EncodeToString(value), nil
}

func decodeDocument[T any](value []byte) (T, error) {
	var decoded T
	if err := json.Unmarshal(value, &decoded); err != nil {
		return decoded, errs.New(errs.KindStateConflict, "private registry document is invalid")
	}
	return decoded, nil
}
