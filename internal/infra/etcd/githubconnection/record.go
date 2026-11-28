// Package githubconnection persists scoped GitHub App connections and their
// separately encrypted signing and webhook credentials.
package githubconnection

import (
	"strings"
	"time"
	"unicode"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	IDKind           ids.Kind = "ghc"
	connectionKind            = "github-app-connection"
	sealedKind                = "github-app-credential"
	plaintextKind             = "github-app-credential-material"
	maxPrivateKey             = 64 << 10
	maxWebhookSecret          = 4 << 10
	maxCiphertext             = 128 << 10
)

// Scope is the exact recorded owner. Resolving access from a descendant scope
// belongs to the Controller's deployment policy.
type Scope struct {
	Kind ids.Kind `json:"kind"`
	ID   string   `json:"id"`
}

func (scope Scope) validate() error {
	switch scope.Kind {
	case ids.KindTenant, ids.KindProject, ids.KindEnvironment:
		return recordcodec.ValidateID(scope.Kind, scope.ID)
	default:
		return errs.New(errs.KindValidationFailed, "GitHub connection owner is invalid")
	}
}

// Connection is the metadata-only projection returned to read callers.
type Connection struct {
	ID             string     `json:"id"`
	Scope          Scope      `json:"scope"`
	Name           string     `json:"name"`
	AppID          int64      `json:"app_id"`
	InstallationID int64      `json:"installation_id"`
	Generation     uint64     `json:"generation"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	RevokedAt      *time.Time `json:"revoked_at,omitempty"`
}

// Credentials contains write-only inputs or borrowed values for a callback.
type Credentials struct {
	PrivateKeyPEM []byte `json:"private_key_pem"`
	WebhookSecret []byte `json:"webhook_secret,omitempty"`
}

type CreateInput struct {
	ID             string
	Scope          Scope
	Name           string
	AppID          int64
	InstallationID int64
	PrivateKeyPEM  []byte
	WebhookSecret  []byte
	CreatedAt      time.Time
}

type credentialIdentity struct {
	ConnectionID   string `json:"connection_id"`
	Scope          Scope  `json:"scope"`
	AppID          int64  `json:"app_id"`
	InstallationID int64  `json:"installation_id"`
	Generation     uint64 `json:"generation"`
}

type sealedCredential struct {
	Identity   credentialIdentity `json:"identity"`
	Ciphertext []byte             `json:"ciphertext"`
}

type credentialMaterial struct {
	Identity    credentialIdentity `json:"identity"`
	Credentials Credentials        `json:"credentials"`
}

func identityOf(record Connection) credentialIdentity {
	return credentialIdentity{
		ConnectionID: record.ID, Scope: record.Scope, AppID: record.AppID,
		InstallationID: record.InstallationID, Generation: record.Generation,
	}
}

func validateConnection(record Connection) error {
	if err := recordcodec.ValidateID(IDKind, record.ID); err != nil {
		return err
	}
	if err := record.Scope.validate(); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("GitHub connection name", record.Name); err != nil {
		return err
	}
	if len(record.Name) > 128 || strings.TrimSpace(record.Name) != record.Name ||
		strings.ContainsFunc(record.Name, unicode.IsControl) {
		return errs.New(errs.KindValidationFailed, "GitHub connection name is invalid")
	}
	if record.AppID <= 0 || record.InstallationID <= 0 || record.Generation == 0 {
		return errs.New(errs.KindValidationFailed, "GitHub connection identity or generation is invalid")
	}
	if !recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "GitHub connection timestamps are invalid")
	}
	if record.RevokedAt != nil && (!recordcodec.IsCanonicalUTC(*record.RevokedAt) ||
		!record.RevokedAt.Equal(record.UpdatedAt) || record.Generation < 2) {
		return errs.New(errs.KindValidationFailed, "GitHub connection revocation is invalid")
	}
	return nil
}

func decodeConnection(value []byte) (Connection, error) {
	record, err := recordcodec.Decode[Connection](value, connectionKind)
	if err != nil {
		return Connection{}, err
	}
	if validateConnection(record) != nil {
		return Connection{}, corruption()
	}
	return record, nil
}

func (value Credentials) valid() bool {
	return len(value.PrivateKeyPEM) > 0 && len(value.PrivateKeyPEM) <= maxPrivateKey &&
		len(value.WebhookSecret) <= maxWebhookSecret
}

func (value Credentials) clear() {
	clear(value.PrivateKeyPEM)
	clear(value.WebhookSecret)
}

func connectionPrefix(scope Scope) string {
	return "/v1/records/github-connections/" + string(scope.Kind) + "/" + scope.ID + "/"
}

func connectionKey(scope Scope, id string) string {
	return connectionPrefix(scope) + id
}

func credentialKey(id string) string {
	return "/v1/private/github-connection-credentials/" + id
}

func corruption() error {
	return errs.New(errs.KindInternal, "GitHub connection record is invalid")
}
