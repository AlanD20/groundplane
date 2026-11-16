package core

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
)

type SecretKind = backupsecret.SecretKind

const (
	SecretKindEnvVar            = backupsecret.SecretKindEnvVar
	SecretKindFile   SecretKind = "file"
)

type SecretScope = backupsecret.SecretScope

const (
	SecretScopeProject  = backupsecret.SecretScopeProject
	SecretScopePlatform = backupsecret.SecretScopePlatform
)

type Secret struct {
	ID         string      `yaml:"id"                   json:"id"` // sec_<ulid>
	Scope      SecretScope `yaml:"scope"                json:"scope"`
	ProjectID  string      `yaml:"project_id,omitempty" json:"project_id,omitempty"`
	Key        string      `yaml:"key"                  json:"key"`
	Kind       SecretKind  `yaml:"kind"                 json:"kind"`
	Ref        string      `yaml:"ref"                  json:"ref"` // generated env-file name or volume-relative file path
	Sensitive  bool        `yaml:"sensitive"            json:"sensitive"`
	Encryption string      `yaml:"encryption"           json:"encryption"`
	UpdatedAt  time.Time   `yaml:"updated_at"           json:"updated_at"`
}
