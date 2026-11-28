package remotehost

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/url"
	"time"

	record "github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	certificateLifetime = 24 * time.Hour
	maximumCSRBytes     = 16 * 1024
)

// Authority signs client-only Agent certificates with the installation's
// explicitly provisioned Remote Agent CA. It never returns a private key.
type Authority struct {
	issuer *x509.Certificate
	signer crypto.Signer
}

func NewAuthority(issuer *x509.Certificate, signer crypto.Signer) (*Authority, error) {
	if issuer == nil || signer == nil {
		return nil, invalidAuthority()
	}
	parsed, err := x509.ParseCertificate(issuer.Raw)
	if err != nil || !parsed.BasicConstraintsValid || !parsed.IsCA ||
		parsed.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, invalidAuthority()
	}
	publicKey, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || !bytes.Equal(publicKey, parsed.RawSubjectPublicKeyInfo) {
		return nil, invalidAuthority()
	}
	return &Authority{issuer: parsed, signer: signer}, nil
}

func invalidAuthority() error {
	return errs.New(errs.KindInternal, "Remote Agent certificate authority is invalid")
}

// ClientRoots contains only the configured Agent CA. Use it on a dedicated
// TLS listener that requires and verifies client certificates.
func (authority *Authority) ClientRoots() *x509.CertPool {
	roots := x509.NewCertPool()
	roots.AddCert(authority.issuer)
	return roots
}

func parseCSR(der []byte) (*x509.CertificateRequest, error) {
	if len(der) == 0 || len(der) > maximumCSRBytes {
		return nil, invalidCSR()
	}
	request, err := x509.ParseCertificateRequest(der)
	if err != nil || !enrollmentKeyAllowed(request.PublicKey) || request.CheckSignature() != nil {
		return nil, invalidCSR()
	}
	return request, nil
}

func enrollmentKeyAllowed(publicKey crypto.PublicKey) bool {
	switch publicKey := publicKey.(type) {
	case *ecdsa.PublicKey:
		return publicKey.Curve.Params().BitSize >= 256
	case ed25519.PublicKey:
		return true
	case *rsa.PublicKey:
		return publicKey.N.BitLen() >= 2048 && publicKey.N.BitLen() <= 4096
	default:
		return false
	}
}

func invalidCSR() error {
	return errs.New(errs.KindValidationFailed, "Remote Agent certificate request is invalid")
}

func (authority *Authority) issue(
	identity record.Identity,
	request *x509.CertificateRequest,
	now time.Time,
) ([]byte, time.Time, error) {
	start := now.UTC().Truncate(time.Second)
	expires := minTime(start.Add(certificateLifetime), authority.issuer.NotAfter)
	if start.Before(authority.issuer.NotBefore) || !expires.After(start) || !identity.Valid() {
		return nil, time.Time{}, invalidAuthority()
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, time.Time{}, errs.Wrap(errs.KindInternal, err)
	}
	serial.SetBit(serial, 128, 1)
	template := &x509.Certificate{
		SerialNumber: serial, NotBefore: start, NotAfter: expires, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{record.IdentityURI(identity)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, authority.issuer, request.PublicKey, authority.signer)
	if err != nil {
		return nil, time.Time{}, errs.Wrap(errs.KindInternal, err)
	}
	return der, expires, nil
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func (authority *Authority) peer(
	state tls.ConnectionState,
	now time.Time,
) (record.Identity, [32]byte, error) {
	if !state.HandshakeComplete || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 ||
		len(state.VerifiedChains) == 0 || state.PeerCertificates[0] == nil {
		return record.Identity{}, [32]byte{}, denied()
	}
	leaf := state.PeerCertificates[0]
	identity, valid := record.CertificateIdentity(leaf)
	if !valid ||
		now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) || now.Before(authority.issuer.NotBefore) ||
		!now.Before(authority.issuer.NotAfter) || leaf.CheckSignatureFrom(authority.issuer) != nil {
		return record.Identity{}, [32]byte{}, denied()
	}
	return identity, sha256.Sum256(leaf.Raw), nil
}
