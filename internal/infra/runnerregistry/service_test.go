package runnerregistry

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTokenExchangeGrantsOnlyAuthorizedRepository(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	const username = "job-test"
	const password = "test-job-credential"
	const repository = "jobs/first"
	grant := Grant{Subject: username, Repository: repository, Actions: Push, ExpiresAt: now.Add(30 * time.Second)}
	authorizer := &fixtureAuthorizer{grant: grant, username: username, password: password}
	service, err := New(
		Config{Issuer: "gp-test", Service: "registry-test", SigningKey: key, SignerCertificate: certificate},
		authorizer,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"https://token.example/token?service=registry-test&scope=repository:jobs/first:pull,push&scope=repository:jobs/second:push",
		nil,
	)
	request.SetBasicAuth(username, password)
	record := httptest.NewRecorder()
	service.ServeHTTP(record, request)
	if record.Code != http.StatusOK || record.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("exchange status = %d, cache control = %q", record.Code, record.Header().Get("Cache-Control"))
	}
	var token tokenResponse
	if err := json.NewDecoder(record.Body).Decode(&token); err != nil {
		t.Fatal(err)
	}
	header, payload, signature := splitToken(t, token.Token)
	keyDigest := sha256.Sum256([]byte(header + "." + payload))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, keyDigest[:], decodeTokenPart(t, signature)); err != nil {
		t.Fatalf("token did not verify: %v", err)
	}
	var claims tokenClaims
	if err := json.Unmarshal(decodeTokenPart(t, payload), &claims); err != nil {
		t.Fatal(err)
	}
	if claims.Subject != username || claims.Audience != "registry-test" || claims.Expiration > grant.ExpiresAt.Unix() ||
		token.ExpiresIn <= 0 {
		t.Fatalf("unexpected subject, audience or expiry: %+v", claims)
	}
	if len(claims.Access) != 1 || claims.Access[0].Type != "repository" || claims.Access[0].Name != repository ||
		strings.Join(claims.Access[0].Actions, ",") != "push" {
		t.Fatalf("unexpected signed repository permissions: %+v", claims.Access)
	}

	authorizer.revoked = true
	record = httptest.NewRecorder()
	service.ServeHTTP(record, request)
	if record.Code != http.StatusUnauthorized {
		t.Fatalf("a revoked job received a new token, status = %d", record.Code)
	}
}

func splitToken(t *testing.T, value string) (string, string, string) {
	t.Helper()
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		t.Fatal("token does not have three parts")
	}
	return parts[0], parts[1], parts[2]
}

func decodeTokenPart(t *testing.T, part string) []byte {
	t.Helper()
	value, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type fixtureAuthorizer struct {
	grant    Grant
	username string
	password string
	revoked  bool
}

func (fixture *fixtureAuthorizer) AuthorizeRegistryJob(_ context.Context, username, credential string) (Grant, error) {
	if fixture.revoked || username != fixture.username || credential != fixture.password {
		return Grant{}, errs.New(errs.KindRequestForbidden, "registry job is not authorized")
	}
	return fixture.grant, nil
}
