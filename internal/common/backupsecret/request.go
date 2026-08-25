// Package backupsecret defines the leaf contract between authenticated Agent
// dispatch, durable Backup evidence resolution, and transient slot delivery.
package backupsecret

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

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

// Request binds one secret resolution to the authenticated Agent generation,
// exact durable assignment, sealed plan, and selected step. Callers must not
// infer any field from a Task record after dispatch.
type Request struct {
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	Deadline        time.Time
	StepID          string
	Plan            *agentpb.ExecutionPlan
	Step            *agentpb.ExecutionStep
}
