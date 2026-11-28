package agent

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
)

func TestVerifyRemoteCredentialRequiresHostGenerationAndKey(t *testing.T) {
	expected := remotehost.Identity{HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}
	caKey, certKey := remoteTestKey(t), remoteTestKey(t)
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	ca, err := x509.ParseCertificate(remoteTestCertificate(t, caTemplate, caTemplate, caKey, caKey))
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		URIs: []*url.URL{remotehost.IdentityURI(expected)}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certificate := tls.Certificate{
		Certificate: [][]byte{remoteTestCertificate(t, leaf, ca, certKey, caKey)},
		PrivateKey:  certKey,
	}
	if err := VerifyRemoteCredential(certificate, ca, expected); err != nil {
		t.Fatal(err)
	}
	for name, wrong := range map[string]remotehost.Identity{
		"host":       {HostID: ids.New(ids.KindHost), AgentID: expected.AgentID, Generation: expected.Generation},
		"agent":      {HostID: expected.HostID, AgentID: ids.New(ids.KindAgent), Generation: expected.Generation},
		"generation": {HostID: expected.HostID, AgentID: expected.AgentID, Generation: expected.Generation + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := VerifyRemoteCredential(certificate, ca, wrong); err == nil {
				t.Fatal("accepted a certificate for another enrolled identity")
			}
		})
	}
	certificate.PrivateKey = remoteTestKey(t)
	if err := VerifyRemoteCredential(certificate, ca, expected); err == nil {
		t.Fatal("accepted a certificate with a different private key")
	}
}

func remoteTestKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func remoteTestCertificate(t *testing.T, subject, issuer *x509.Certificate, key, signer ed25519.PrivateKey) []byte {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, subject, issuer, key.Public(), signer)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
