package runnerregistry

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const localRegistryRoot = "/etc/groundplane/scoped-runner-registry"

const registryTLSInputLimit = 64 << 10

// NewLocalGateway reads the separately managed Runner listener material and
// the Controller's existing credentials for the loopback Registry.
func NewLocalGateway(
	ctx context.Context,
	endpoint netip.AddrPort,
	authorizer JobAuthorizer,
) (*Gateway, tls.Certificate, error) {
	certificatePEM, err := LocalCertificate(ctx, endpoint)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	keyPEM, err := readLocalTLSInput(ctx, "tls.key", true)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	defer clear(keyPEM)
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, tls.Certificate{}, errs.New(
			errs.KindValidationFailed,
			"Runner registry certificate and private key do not match",
		)
	}
	settings, err := registryconfiguration.Read(ctx)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	backendRoots := x509.NewCertPool()
	if !backendRoots.AppendCertsFromPEM(settings.Certificate) {
		return nil, tls.Certificate{}, errs.New(
			errs.KindValidationFailed,
			"Loopback Registry trust material is invalid",
		)
	}
	gateway, err := NewGateway(GatewayConfig{
		Backend:           &url.URL{Scheme: "https", Host: "127.0.0.1:5000"},
		BackendUsername:   settings.Username,
		BackendCredential: settings.Password,
		TLSRoots:          backendRoots,
	}, authorizer)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	return gateway, certificate, nil
}

// LocalCertificate supplies the managed public PEM for rootless Docker trust.
// The listener certificate is pinned and must be valid for its private IPv4 IP.
func LocalCertificate(ctx context.Context, endpoint netip.AddrPort) ([]byte, error) {
	if !endpoint.IsValid() || endpoint.Port() == 0 || !endpoint.Addr().Is4() || !endpoint.Addr().IsPrivate() {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry listener requires a private IPv4 endpoint")
	}
	data, err := readLocalTLSInput(ctx, "tls.crt", false)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || len(rest) != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Runner registry trust must contain exactly one PEM certificate",
		)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry certificate is invalid")
	}
	if leaf.IsCA || len(leaf.IPAddresses) != 1 || len(leaf.DNSNames) != 0 || len(leaf.EmailAddresses) != 0 ||
		len(leaf.URIs) != 0 ||
		!slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) ||
		!bytes.Equal(leaf.RawIssuer, leaf.RawSubject) ||
		leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature) != nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Runner registry trust requires a self-signed server leaf for exactly one IP",
		)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: endpoint.Addr().String()}); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Runner registry certificate is invalid for its configured IP")
	}
	return data, nil
}

func readLocalTLSInput(ctx context.Context, name string, private bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, path := range []string{filepath.Dir(localRegistryRoot), localRegistryRoot} {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		if !info.IsDir() || !rootOwns(info) || info.Mode().Perm()&0o022 != 0 {
			return nil, errs.New(
				errs.KindStateConflict,
				"Runner registry TLS directory ownership or permissions changed",
			)
		}
	}
	root, err := os.OpenRoot(localRegistryRoot)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	mode := info.Mode().Perm()
	if !info.Mode().IsRegular() || !rootOwns(info) || info.Size() > registryTLSInputLimit ||
		(private && mode != 0o600) || (!private && mode != 0o600 && mode != 0o644) {
		return nil, errs.New(errs.KindStateConflict, "Runner registry TLS file ownership or permissions changed")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errs.New(errs.KindStateConflict, "Runner registry TLS input changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, registryTLSInputLimit+1))
	if err != nil || len(data) > registryTLSInputLimit {
		clear(data)
		return nil, errs.New(errs.KindStateConflict, "Runner registry TLS input could not be read")
	}
	return data, nil
}

func rootOwns(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}
