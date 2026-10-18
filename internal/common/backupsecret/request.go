// Package backupsecret defines the leaf contract between authenticated Agent
// dispatch, durable Backup evidence resolution, and transient slot delivery.
package backupsecret

type CredentialName string

const (
	CredentialAccessKey CredentialName = "access_key"
	CredentialSecretKey CredentialName = "secret_key"
)

type CredentialSource string

const (
	CredentialSourceDirect    CredentialSource = "direct"
	CredentialSourceSecretRef CredentialSource = "secret_ref"
)

type SecretKind string

const SecretKindEnvVar SecretKind = "env_var"

type SecretScope string

const (
	SecretScopeProject  SecretScope = "project"
	SecretScopePlatform SecretScope = "platform"
)
