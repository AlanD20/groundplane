package runnerregistry

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type tokenHeader struct {
	Algorithm        string   `json:"alg"`
	Type             string   `json:"typ"`
	CertificateChain []string `json:"x5c"`
}

type tokenClaims struct {
	Issuer     string     `json:"iss"`
	Subject    string     `json:"sub"`
	Audience   string     `json:"aud"`
	Expiration int64      `json:"exp"`
	NotBefore  int64      `json:"nbf"`
	IssuedAt   int64      `json:"iat"`
	ID         string     `json:"jti"`
	Access     []resource `json:"access"`
}

type resource struct {
	Type    string   `json:"type"`
	Name    string   `json:"name"`
	Actions []string `json:"actions"`
}

type tokenResponse struct {
	Token     string `json:"token"`
	ExpiresIn int64  `json:"expires_in"`
	IssuedAt  string `json:"issued_at"`
}

func (service *Service) mint(now time.Time, grant Grant, scopes []string) (tokenResponse, error) {
	if now.Before(service.cert.NotBefore) || !now.Before(service.cert.NotAfter) {
		return tokenResponse{}, errs.New(
			errs.KindRequestUnavailable,
			"Runner registry signing certificate is unavailable",
		)
	}
	expires := now.Add(tokenLifetime)
	if grant.ExpiresAt.Before(expires) {
		expires = grant.ExpiresAt
	}
	if service.cert.NotAfter.Before(expires) {
		expires = service.cert.NotAfter
	}
	lifetime := expires.Unix() - now.Unix()
	if lifetime <= 0 {
		return tokenResponse{}, errs.New(errs.KindRequestUnavailable, "Runner registry grant has expired")
	}
	claims := tokenClaims{
		Issuer: service.issuer, Subject: grant.Subject, Audience: service.service,
		Expiration: expires.Unix(), NotBefore: now.Unix(), IssuedAt: now.Unix(), ID: rand.Text(),
		Access: intersect(grant, scopes),
	}
	payload, err := encodeJWTPart(claims)
	if err != nil {
		return tokenResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	input := service.header + "." + payload
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(nil, service.key, crypto.SHA256, digest[:])
	if err != nil {
		return tokenResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return tokenResponse{
		Token:     input + "." + base64.RawURLEncoding.EncodeToString(signature),
		ExpiresIn: lifetime, IssuedAt: now.Truncate(time.Second).Format(time.RFC3339),
	}, nil
}

func intersect(grant Grant, scopes []string) []resource {
	var requested Actions
	prefix := "repository:" + grant.Repository + ":"
	for _, scopesValue := range scopes {
		for _, scope := range strings.Fields(scopesValue) {
			actions, matches := strings.CutPrefix(scope, prefix)
			if !matches {
				continue
			}
			for action := range strings.SplitSeq(actions, ",") {
				switch action {
				case "pull":
					requested |= Pull
				case "push":
					requested |= Push
				}
			}
		}
	}
	var actions []string
	if requested&grant.Actions&Pull != 0 {
		actions = append(actions, "pull")
	}
	if requested&grant.Actions&Push != 0 {
		actions = append(actions, "push")
	}
	if len(actions) == 0 {
		return []resource{}
	}
	return []resource{{Type: "repository", Name: grant.Repository, Actions: actions}}
}

func encodeJWTPart[T tokenHeader | tokenClaims](value T) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}
