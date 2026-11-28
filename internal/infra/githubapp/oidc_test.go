package githubapp

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestOIDCVerifierAuthenticatesOnlyTheBoundJob(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const audience = "groundplane-build:op_01J00000000000000000000000"
	encoding := base64.RawURLEncoding
	keySet, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kid": "test-key", "kty": "RSA", "alg": "RS256", "use": "sig",
		"n": encoding.EncodeToString(key.N.Bytes()), "e": "AQAB",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	verifier := NewOIDCVerifier()
	verifier.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != githubJWKSURL || request.Header.Get("Authorization") != "" {
			t.Fatalf("Unexpected signing key request %s", request.URL.Host)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(keySet))}, nil
	})
	now := time.Now()
	claims := map[string]any{
		"iss": githubOIDCIssuer, "aud": audience, "sub": "repo:owner/repo:ref:refs/heads/main",
		"iat": now.Add(-time.Minute).Unix(), "nbf": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Minute).Unix(),
		"repository": "owner/repo", "repository_id": "5", "run_id": "12", "run_attempt": "1", "check_run_id": "15",
		"event_name": "workflow_dispatch", "runner_environment": "self-hosted", "ref": "refs/heads/main",
		"workflow_ref": "owner/repo/.github/workflows/build.yml@refs/heads/main", "workflow_sha": strings.Repeat("a", 40),
		"sha": strings.Repeat("a", 40),
	}
	sign := func() string {
		t.Helper()
		head := encoding.EncodeToString([]byte(`{"alg":"RS256","kid":"test-key"}`))
		body, err := json.Marshal(claims)
		if err != nil {
			t.Fatal(err)
		}
		input := head + "." + encoding.EncodeToString(body)
		digest := sha256.Sum256([]byte(input))
		signature, err := rsa.SignPKCS1v15(nil, key, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		return input + "." + encoding.EncodeToString(signature)
	}
	proofToken := sign()
	proof, err := verifier.Verify(t.Context(), proofToken, audience)
	if err != nil || proof.RepositoryID != 5 || proof.RunID != 12 || proof.RunAttempt != 1 || proof.JobID != 15 {
		t.Fatalf("Bound job proof failed: %+v, %v", proof, err)
	}
	if _, err := verifier.Verify(t.Context(), proofToken, audience+"-other"); err == nil {
		t.Fatal("A valid signature authorized a different deployment")
	}
	parts := strings.Split(proofToken, ".")
	claims["check_run_id"] = "67"
	forgedClaims, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	forged := parts[0] + "." + encoding.EncodeToString(forgedClaims) + "." + parts[2]
	if _, err := verifier.Verify(t.Context(), forged, audience); err == nil {
		t.Fatal("An existing signature authorized tampered job claims")
	}
	claims["check_run_id"] = "99999999999999999999999999999999"
	if _, err := verifier.Verify(t.Context(), sign(), audience); err == nil {
		t.Fatal("An overflowing signed job ID was accepted")
	}
	claims["check_run_id"] = "15"
	claims["exp"] = now.Add(-time.Second).Unix()
	if _, err := verifier.Verify(t.Context(), sign(), audience); err == nil {
		t.Fatal("An expired signed job proof was accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (transport roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	return transport(request)
}
