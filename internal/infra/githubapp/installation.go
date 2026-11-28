package githubapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

var (
	ownerPattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,38})$`)
	repoPattern  = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,100}$`)
)

type Repository struct {
	ID            int64
	Owner         string
	Name          string
	DefaultBranch string
	Private       bool
	Archived      bool
	Disabled      bool
}

type installation struct {
	scope   Scope
	token   string
	expires time.Time
	repo    Repository
}

type repoRecord struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
	Archived      bool   `json:"archived"`
	Disabled      bool   `json:"disabled"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

func (client *Client) Repository(ctx context.Context, scope Scope) (Repository, error) {
	access, err := client.installation(ctx, scope)
	return access.repo, err
}

// Repositories lists one page through a metadata-only installation token.
func (client *Client) Repositories(
	ctx context.Context,
	installationID int64,
	page, limit int,
) ([]Repository, bool, error) {
	if installationID <= 0 || page < 1 || page > 10000 || limit < 1 || limit > pageSize {
		return nil, false, errs.New(errs.KindValidationFailed, "GitHub installation and repository page are required")
	}
	token, err := client.mint(ctx, installationID, installationTokenParams{
		Permissions: installationPermissions{Metadata: "read"},
	})
	if err != nil {
		return nil, false, err
	}
	var result repositoryPage
	path := "/installation/repositories?per_page=" + id(int64(limit)) + "&page=" + id(int64(page))
	if err := client.requestJSON(ctx, http.MethodGet, path, token.Token, nil, http.StatusOK, &result); err != nil {
		return nil, false, err
	}
	if !validPage(result.TotalCount, len(result.Repositories), page, limit) {
		return nil, false, errs.New(errs.KindStateConflict, "GitHub returned an incomplete repository page")
	}
	repositories := make([]Repository, 0, len(result.Repositories))
	seen := make(map[int64]struct{}, len(result.Repositories))
	for _, record := range result.Repositories {
		repository, err := record.repository()
		if err != nil {
			return nil, false, err
		}
		if _, duplicate := seen[repository.ID]; duplicate {
			return nil, false, errs.New(errs.KindStateConflict, "GitHub returned duplicate repository IDs")
		}
		seen[repository.ID] = struct{}{}
		repositories = append(repositories, repository)
	}
	return repositories, page*limit < result.TotalCount, nil
}

func (client *Client) installation(ctx context.Context, scope Scope) (installation, error) {
	if scope.InstallationID <= 0 || scope.RepositoryID <= 0 {
		return installation{}, errs.New(
			errs.KindValidationFailed,
			"GitHub installation and repository IDs are required",
		)
	}
	now := time.Now()
	client.mu.Lock()
	cached := client.last
	client.mu.Unlock()
	if cached.scope == scope && now.Before(cached.expires) {
		return cached, nil
	}
	access, err := client.scopedInstallation(
		ctx,
		scope,
		installationPermissions{Actions: "write", Contents: "read", Metadata: "read"},
	)
	if err != nil {
		return installation{}, err
	}
	client.mu.Lock()
	client.last = access
	client.mu.Unlock()
	return access, nil
}

func (client *Client) scopedInstallation(
	ctx context.Context,
	scope Scope,
	permissions installationPermissions,
) (installation, error) {
	if scope.InstallationID <= 0 || scope.RepositoryID <= 0 {
		return installation{}, errs.New(
			errs.KindValidationFailed,
			"GitHub installation and repository IDs are required",
		)
	}
	now := time.Now()
	token, err := client.mint(
		ctx,
		scope.InstallationID,
		installationTokenParams{RepositoryIDs: []int64{scope.RepositoryID}, Permissions: permissions},
	)
	if err != nil {
		return installation{}, err
	}
	var result repositoryPage
	if err := client.requestJSON(ctx, http.MethodGet, "/installation/repositories?per_page=2", token.Token, nil,
		http.StatusOK, &result); err != nil {
		return installation{}, err
	}
	if result.TotalCount != 1 || len(result.Repositories) != 1 || result.Repositories[0].ID != scope.RepositoryID {
		return installation{}, errs.New(
			errs.KindScopeUnauthorized,
			"GitHub installation token is not restricted to the selected repository ID",
		)
	}
	repo, err := result.Repositories[0].repository()
	if err != nil {
		return installation{}, err
	}
	return installation{
		scope: scope, token: token.Token,
		expires: minTime(token.ExpiresAt.Add(-time.Minute), now.Add(5*time.Minute)),
		repo:    repo,
	}, nil
}

type installationTokenParams struct {
	RepositoryIDs []int64                 `json:"repository_ids,omitempty"`
	Permissions   installationPermissions `json:"permissions"`
}

type installationPermissions struct {
	Actions                       string `json:"actions,omitempty"`
	Administration                string `json:"administration,omitempty"`
	Contents                      string `json:"contents,omitempty"`
	Metadata                      string `json:"metadata"`
	OrganizationSelfHostedRunners string `json:"organization_self_hosted_runners,omitempty"`
}

type installationToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type repositoryPage struct {
	TotalCount   int          `json:"total_count"`
	Repositories []repoRecord `json:"repositories"`
}

func (item repoRecord) repository() (Repository, error) {
	if item.ID <= 0 || !ownerPattern.MatchString(item.Owner.Login) || !repoPattern.MatchString(item.Name) ||
		item.Name == "." ||
		item.Name == ".." ||
		!validRef(item.DefaultBranch) {
		return Repository{}, errs.New(errs.KindStateConflict, "GitHub returned invalid repository metadata")
	}
	return Repository{ID: item.ID, Owner: item.Owner.Login, Name: item.Name, DefaultBranch: item.DefaultBranch,
		Private: item.Private, Archived: item.Archived, Disabled: item.Disabled}, nil
}

func (client *Client) mint(
	ctx context.Context,
	installationID int64,
	params installationTokenParams,
) (installationToken, error) {
	now := time.Now()
	credential, err := client.appJWT(now)
	if err != nil {
		return installationToken{}, err
	}
	request, err := json.Marshal(params)
	if err != nil {
		return installationToken{}, errs.Wrap(errs.KindInternal, err)
	}
	var token installationToken
	path := "/app/installations/" + id(installationID) + "/access_tokens"
	if err := client.requestJSON(ctx, http.MethodPost, path, credential, request, http.StatusCreated, &token); err != nil {
		return installationToken{}, err
	}
	if !validToken(token.Token) || !token.ExpiresAt.After(now.Add(time.Minute)) {
		return installationToken{}, errs.New(
			errs.KindStateConflict,
			"GitHub returned an invalid installation credential",
		)
	}
	return token, nil
}

func (access installation) path() string {
	return "/repos/" + access.repo.Owner + "/" + access.repo.Name
}

func (client *Client) appJWT(now time.Time) (string, error) {
	const header = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9" // {"alg":"RS256","typ":"JWT"}
	payload, err := json.Marshal(struct {
		IssuedAt int64  `json:"iat"`
		Expires  int64  `json:"exp"`
		Issuer   string `json:"iss"`
	}{IssuedAt: now.Add(-time.Minute).Unix(), Expires: now.Add(9 * time.Minute).Unix(), Issuer: client.appID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	input := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(nil, client.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", errs.New(errs.KindInternal, "Could not sign GitHub App authentication token")
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func parsePrivateKey(input []byte) (*rsa.PrivateKey, error) {
	invalid := errs.New(
		errs.KindValidationFailed,
		"GitHub App private key must be a valid RSA PEM of at least 2048 bits",
	)
	block, rest := pem.Decode(bytes.TrimSpace(input))
	if block == nil || len(bytes.TrimSpace(rest)) > 0 || len(block.Headers) > 0 {
		return nil, invalid
	}
	defer clear(block.Bytes)
	var key *rsa.PrivateKey
	var err error
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		var decoded any
		decoded, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		key, _ = decoded.(*rsa.PrivateKey)
	}
	if err != nil || key == nil || key.N.BitLen() < 2048 || key.Validate() != nil {
		return nil, invalid
	}
	return key, nil
}

func validToken(token string) bool {
	return len(token) != 0 && len(token) <= 4096 && strings.IndexFunc(token, func(char rune) bool {
		return char <= ' ' || char >= 127
	}) == -1
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
