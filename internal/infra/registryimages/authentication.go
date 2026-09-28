package registryimages

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Public registries may challenge anonymous pulls with a short-lived bearer
// token. Never send the managed registry's credentials to their token services.
func (registryClient *Client) registryRequest(
	request *http.Request,
	repository string,
	managed bool,
) (*http.Response, error) {
	response, err := registryClient.http.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusUnauthorized && !managed {
		challenge := response.Header.Get("WWW-Authenticate")
		response.Body.Close()
		token, err := registryClient.anonymousToken(request, challenge, repository)
		if err != nil {
			return nil, err
		}
		request = request.Clone(request.Context())
		request.Header.Set("Authorization", "Bearer "+token)
		response, err = registryClient.http.Do(request)
		if err != nil {
			return nil, err
		}
	}
	// Blob CDNs can be on a different origin. Follow HTTPS only, without any
	// authorization headers, and with a fixed bound independent of HTTP defaults.
	for redirects := 0; response.StatusCode >= 300 && response.StatusCode <= 399; redirects++ {
		location, locationErr := response.Location()
		response.Body.Close()
		if managed || redirects == 3 || locationErr != nil || !secureURL(location) {
			return nil, errs.New(errs.KindStateConflict, "registry redirect is unsupported")
		}
		redirect, err := http.NewRequestWithContext(request.Context(), http.MethodGet, location.String(), nil)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		response, err = registryClient.http.Do(redirect)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (registryClient *Client) anonymousToken(request *http.Request, challenge, repository string) (string, error) {
	// Auth parameters and MIME parameters share quoted-string syntax. Translate
	// only separators outside quotes; a scope can itself contain commas.
	scheme, parameters, found := strings.Cut(challenge, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", errs.New(
			errs.KindStateConflict,
			"registry requires credentials; only public images and the GP registry are supported",
		)
	}
	var translated strings.Builder
	quoted, escaped := false, false
	for _, character := range parameters {
		if !quoted && character == ',' {
			character = ';'
		}
		translated.WriteRune(character)
		if escaped {
			escaped = false
		} else if quoted && character == '\\' {
			escaped = true
		} else if character == '"' {
			quoted = !quoted
		}
	}
	_, values, err := mime.ParseMediaType("Bearer;" + translated.String())
	if err != nil {
		return "", errs.New(errs.KindStateConflict, "registry authentication challenge is invalid")
	}
	realm, err := url.Parse(values["realm"])
	if err != nil || !secureURL(realm) {
		return "", errs.New(errs.KindStateConflict, "registry authentication requires HTTPS")
	}
	query := realm.Query()
	query.Set("service", values["service"])
	query.Set("scope", "repository:"+repository+":pull")
	realm.RawQuery = query.Encode()
	tokenRequest, err := http.NewRequestWithContext(request.Context(), http.MethodGet, realm.String(), nil)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	response, err := registryClient.http.Do(tokenRequest)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errs.New(errs.KindStateConflict, "registry denied anonymous pull access")
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	defer clear(value)
	var token struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err != nil || len(value) > 65536 || json.Unmarshal(value, &token) != nil {
		return "", errs.New(errs.KindStateConflict, "registry token response is invalid")
	}
	if token.Token == "" {
		token.Token = token.AccessToken
	}
	if token.Token == "" || strings.ContainsAny(token.Token, "\r\n") {
		return "", errs.New(errs.KindStateConflict, "registry token response is empty or invalid")
	}
	return token.Token, nil
}

func secureURL(value *url.URL) bool {
	return value != nil && value.Scheme == "https" && value.Host != "" && value.User == nil && value.Fragment == ""
}
