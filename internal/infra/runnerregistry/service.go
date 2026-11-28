// Package runnerregistry exchanges verified Runner job credentials for bounded
// Docker Distribution bearer tokens.
package runnerregistry

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

const (
	maxQueryBytes      = 4096
	maxCredentialBytes = 4096
	tokenLifetime      = time.Minute
)

// Actions is a closed set of permissions on one image repository.
type Actions uint8

const (
	Pull Actions = 1 << iota
	Push
)

type Grant struct {
	Subject    string
	Repository string
	Actions    Actions
	ExpiresAt  time.Time
}

// JobAuthorizer authenticates the credentials and resolves the current job
// grant on every exchange. Revoking the grant prevents another exchange. Stock
// Distribution verifies previously issued tokens locally, including its clock
// skew allowance, until their expiration.
type JobAuthorizer interface {
	AuthorizeRegistryJob(ctx context.Context, username, credential string) (Grant, error)
}

type Config struct {
	Issuer            string
	Service           string
	SigningKey        *rsa.PrivateKey
	SignerCertificate *x509.Certificate
}

// Service is mounted on a dedicated private HTTPS endpoint. Its signing
// certificate must be trusted by the matching Distribution instance.
type Service struct {
	issuer     string
	service    string
	key        *rsa.PrivateKey
	cert       *x509.Certificate
	header     string
	authorizer JobAuthorizer
}

func New(config Config, authorizer JobAuthorizer) (*Service, error) {
	if authorizer == nil || !visibleASCII(config.Issuer) || !visibleASCII(config.Service) ||
		config.SigningKey == nil || config.SigningKey.N == nil || config.SigningKey.N.BitLen() < 2048 || config.SignerCertificate == nil {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry token configuration is invalid")
	}
	if err := config.SigningKey.Validate(); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry signing key is invalid")
	}
	cert, err := x509.ParseCertificate(config.SignerCertificate.Raw)
	if err != nil || !config.SigningKey.PublicKey.Equal(cert.PublicKey) ||
		cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry signing certificate is invalid")
	}
	header, err := encodeJWTPart(tokenHeader{
		Algorithm:        "RS256",
		Type:             "JWT",
		CertificateChain: []string{base64.StdEncoding.EncodeToString(cert.Raw)},
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &Service{
		issuer: config.Issuer, service: config.Service, key: config.SigningKey,
		cert: cert, header: header, authorizer: authorizer,
	}, nil
}

func (service *Service) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if request.TLS == nil {
		http.Error(w, "Registry token exchange requires TLS", http.StatusForbidden)
		return
	}
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(request.URL.RawQuery) > maxQueryBytes {
		http.Error(w, "Registry token request is invalid", http.StatusBadRequest)
		return
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if values := query["service"]; err != nil || len(values) != 1 || values[0] != service.service {
		http.Error(w, "Registry token request is invalid", http.StatusBadRequest)
		return
	}
	authorization := request.Header.Values("Authorization")
	if len(authorization) != 1 || len(authorization[0]) > maxCredentialBytes {
		unauthorized(w)
		return
	}
	username, credential, ok := request.BasicAuth()
	if !ok || !visibleASCII(username) || credential == "" {
		unauthorized(w)
		return
	}
	grant, err := service.authorizer.AuthorizeRegistryJob(request.Context(), username, credential)
	now := time.Now().UTC()
	if err != nil || !grant.valid(now) {
		unauthorized(w)
		return
	}
	token, err := service.mint(now, grant, query["scope"])
	if err != nil {
		http.Error(w, "Registry token exchange unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(token)
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="Groundplane Runner registry"`)
	http.Error(w, "Registry job credentials are invalid", http.StatusUnauthorized)
}

func (grant Grant) valid(now time.Time) bool {
	if !visibleASCII(grant.Subject) || !grant.ExpiresAt.After(now) || grant.Actions == 0 ||
		grant.Actions&^(Pull|Push) != 0 {
		return false
	}
	image, err := reference.WithName(grant.Repository)
	return err == nil && image.Name() == grant.Repository
}

func visibleASCII(value string) bool {
	if len(value) == 0 || len(value) > 255 {
		return false
	}
	for index := range len(value) {
		if value[index] < '!' || value[index] > '~' {
			return false
		}
	}
	return true
}
