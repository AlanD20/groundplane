// Package agentprotocol defines the fixed local Controller-Agent channel
// values shared by runtime materialization and Agent startup.
package agentprotocol

const (
	SocketPath        = "/run/groundplane/controller/agent.sock"
	RuntimeConfigPath = "/run/groundplane/agent.yaml"
	TokenPath         = "/run/groundplane/agent.token"
	RawTokenBytes     = 32
	EncodedTokenBytes = 43
)
