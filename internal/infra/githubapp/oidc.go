package githubapp

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	githubOIDCIssuer = "https://token.actions.githubusercontent.com"
	githubJWKSURL    = githubOIDCIssuer + "/.well-known/jwks"
	maxProofSize     = 16 << 10
	maxKeySetSize    = 128 << 10
	proofClockSkew   = 30 * time.Second
	maxProofAge      = 10 * time.Minute
	keyCacheLife     = 2 * time.Minute
	keyRefreshDelay  = 15 * time.Second
)

// OIDCProof contains signed GitHub claims. The Controller matches these to the
// captured deployment, exact job and registered Groundplane Runner.
type OIDCProof struct {
	RepositoryID      int64
	RunID             int64
	RunAttempt        int64
	JobID             int64
	Repository        string
	Ref               string
	SHA               string
	WorkflowRef       string
	WorkflowSHA       string
	JobWorkflowRef    string
	JobWorkflowSHA    string
	EventName         string
	RunnerEnvironment string
	Subject           string
	ExpiresAt         time.Time
}

type OIDCVerifier struct {
	http         *http.Client
	mu           sync.Mutex
	keys         map[string]*rsa.PublicKey
	expires      time.Time
	refreshAfter time.Time
}

func NewOIDCVerifier() *OIDCVerifier {
	return &OIDCVerifier{http: &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

type oidcPayload struct {
	Issuer            string      `json:"iss"`
	Audience          string      `json:"aud"`
	Subject           string      `json:"sub"`
	IssuedAt          int64       `json:"iat"`
	NotBefore         int64       `json:"nbf"`
	ExpiresAt         int64       `json:"exp"`
	RepositoryID      json.Number `json:"repository_id"`
	RunID             json.Number `json:"run_id"`
	RunAttempt        json.Number `json:"run_attempt"`
	JobID             json.Number `json:"check_run_id"`
	Repository        string      `json:"repository"`
	Ref               string      `json:"ref"`
	SHA               string      `json:"sha"`
	WorkflowRef       string      `json:"workflow_ref"`
	WorkflowSHA       string      `json:"workflow_sha"`
	JobWorkflowRef    string      `json:"job_workflow_ref"`
	JobWorkflowSHA    string      `json:"job_workflow_sha"`
	EventName         string      `json:"event_name"`
	RunnerEnvironment string      `json:"runner_environment"`
}

func (verifier *OIDCVerifier) Verify(ctx context.Context, token, audience string) (OIDCProof, error) {
	invalid := errs.New(errs.KindScopeUnauthorized, "GitHub Actions job proof is invalid")
	if len(token) == 0 || len(token) > maxProofSize || !boundedText(audience, 255) {
		return OIDCProof{}, invalid
	}
	parts := strings.SplitN(token, ".", 4)
	if len(parts) != 3 {
		return OIDCProof{}, invalid
	}
	encoding := base64.RawURLEncoding.Strict()
	headerBytes, err := encoding.DecodeString(parts[0])
	if err != nil || len(headerBytes) > 2048 {
		return OIDCProof{}, invalid
	}
	var header struct {
		KeyID     string          `json:"kid"`
		Algorithm string          `json:"alg"`
		Critical  json.RawMessage `json:"crit"`
	}
	if json.Unmarshal(headerBytes, &header) != nil || header.Algorithm != "RS256" ||
		!requestPattern.MatchString(header.KeyID) || len(header.Critical) != 0 {
		return OIDCProof{}, invalid
	}
	claimBytes, err := encoding.DecodeString(parts[1])
	if err != nil {
		return OIDCProof{}, invalid
	}
	var payload oidcPayload
	if json.Unmarshal(claimBytes, &payload) != nil {
		return OIDCProof{}, invalid
	}
	proof, valid := payload.proofAt(audience, time.Now())
	if !valid {
		return OIDCProof{}, invalid
	}
	signature, err := encoding.DecodeString(parts[2])
	if err != nil || len(signature) < 256 || len(signature) > 512 {
		return OIDCProof{}, invalid
	}
	key, err := verifier.signingKey(ctx, header.KeyID)
	if err != nil {
		return OIDCProof{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if key == nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil ||
		!proof.ExpiresAt.After(time.Now()) {
		return OIDCProof{}, invalid
	}
	return proof, nil
}

func (payload oidcPayload) proofAt(audience string, now time.Time) (OIDCProof, bool) {
	repositoryID, repositoryErr := payload.RepositoryID.Int64()
	runID, runErr := payload.RunID.Int64()
	attempt, attemptErr := payload.RunAttempt.Int64()
	jobID, jobErr := payload.JobID.Int64()
	issuedAt := time.Unix(payload.IssuedAt, 0)
	notBefore := time.Unix(payload.NotBefore, 0)
	expires := time.Unix(payload.ExpiresAt, 0)
	if payload.Issuer != githubOIDCIssuer || payload.Audience != audience || !boundedText(payload.Subject, 1024) ||
		payload.EventName != "workflow_dispatch" || payload.RunnerEnvironment != "self-hosted" ||
		repositoryErr != nil || runErr != nil || attemptErr != nil || jobErr != nil ||
		repositoryID <= 0 || runID <= 0 || attempt <= 0 || jobID <= 0 ||
		!boundedText(
			payload.Repository,
			255,
		) || !boundedText(payload.Ref, 512) || !boundedText(payload.WorkflowRef, 1024) ||
		!commitPattern.MatchString(payload.SHA) || !commitPattern.MatchString(payload.WorkflowSHA) ||
		payload.IssuedAt <= 0 || payload.NotBefore <= 0 || !expires.After(now) ||
		issuedAt.Before(now.Add(-maxProofAge)) || issuedAt.After(now.Add(proofClockSkew)) ||
		notBefore.After(now.Add(proofClockSkew)) || !expires.After(issuedAt) || !expires.After(notBefore) ||
		expires.After(issuedAt.Add(maxProofAge)) {
		return OIDCProof{}, false
	}
	return OIDCProof{RepositoryID: repositoryID, RunID: runID, RunAttempt: attempt, JobID: jobID,
		Repository: payload.Repository, Ref: payload.Ref, SHA: payload.SHA, WorkflowRef: payload.WorkflowRef,
		WorkflowSHA: payload.WorkflowSHA, JobWorkflowRef: payload.JobWorkflowRef, JobWorkflowSHA: payload.JobWorkflowSHA,
		EventName: payload.EventName, RunnerEnvironment: payload.RunnerEnvironment, Subject: payload.Subject, ExpiresAt: expires.UTC()}, true
}

func (verifier *OIDCVerifier) signingKey(ctx context.Context, keyID string) (*rsa.PublicKey, error) {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	now := time.Now()
	if now.Before(verifier.expires) && verifier.keys[keyID] != nil {
		return verifier.keys[keyID], nil
	}
	if now.Before(verifier.refreshAfter) {
		if now.Before(verifier.expires) {
			return nil, nil
		}
		return nil, errs.New(errs.KindRequestUnavailable, "GitHub Actions signing keys are unavailable")
	}
	// An unrecognized kid may indicate rotation. Bound refreshes even if many
	// requests arrive with unrelated key IDs.
	verifier.refreshAfter = now.Add(keyRefreshDelay)
	keys, err := verifier.fetchSigningKeys(ctx)
	if err != nil {
		return nil, err
	}
	verifier.keys = keys
	verifier.expires = now.Add(keyCacheLife)
	return keys[keyID], nil
}

func (verifier *OIDCVerifier) fetchSigningKeys(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	unavailable := errs.New(errs.KindRequestUnavailable, "GitHub Actions signing keys are unavailable")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, githubJWKSURL, nil)
	if err != nil {
		return nil, unavailable
	}
	request.Header.Set("Accept", "application/json")
	response, err := verifier.http.Do(request)
	if err != nil {
		return nil, unavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, unavailable
	}
	var jwks struct {
		Keys []struct {
			ID        string `json:"kid"`
			Type      string `json:"kty"`
			Algorithm string `json:"alg"`
			Use       string `json:"use"`
			Modulus   string `json:"n"`
			Exponent  string `json:"e"`
		} `json:"keys"`
	}
	if decodeBoundedJSON(response.Body, maxKeySetSize, &jwks) != nil || len(jwks.Keys) == 0 || len(jwks.Keys) > 32 {
		return nil, unavailable
	}
	keys := make(map[string]*rsa.PublicKey, len(jwks.Keys))
	for _, jwk := range jwks.Keys {
		if jwk.Type != "RSA" || jwk.Algorithm != "RS256" || jwk.Use != "sig" {
			continue
		}
		if !requestPattern.MatchString(jwk.ID) || keys[jwk.ID] != nil {
			return nil, unavailable
		}
		key := rsaSigningKey(jwk.Modulus, jwk.Exponent)
		if key == nil {
			return nil, unavailable
		}
		keys[jwk.ID] = key
	}
	if len(keys) == 0 {
		return nil, unavailable
	}
	return keys, nil
}

func rsaSigningKey(modulus, exponent string) *rsa.PublicKey {
	if len(modulus) > 700 || len(exponent) > 7 {
		return nil
	}
	encoding := base64.RawURLEncoding.Strict()
	n, err := encoding.DecodeString(modulus)
	if err != nil {
		return nil
	}
	e, err := encoding.DecodeString(exponent)
	if err != nil {
		return nil
	}
	key := &rsa.PublicKey{N: new(big.Int).SetBytes(n)}
	value := new(big.Int).SetBytes(e)
	if key.N.BitLen() < 2048 || key.N.BitLen() > 4096 || key.N.Bit(0) != 1 ||
		!value.IsInt64() || value.Int64() < 3 || value.Int64() > 1<<31-1 || value.Bit(0) != 1 {
		return nil
	}
	key.E = int(value.Int64())
	return key
}
