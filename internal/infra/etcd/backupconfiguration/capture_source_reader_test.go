package backupconfiguration

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestCaptureSourceAcceptsOrdinaryReusableVariableForOrdinaryEntry(t *testing.T) {
	entry := core.EnvEntry{Kind: core.EntryKindEnv}
	variable := core.Secret{Kind: core.SecretKindEnvVar}
	if !captureSourceSecretMatchesEntry(entry, variable) {
		t.Fatal("ordinary reusable variable was rejected for an ordinary environment Entry")
	}
	variable.Sensitive = true
	if captureSourceSecretMatchesEntry(entry, variable) {
		t.Fatal("confidential reusable value was accepted for an ordinary Entry")
	}
	entry.Secret = true
	if !captureSourceSecretMatchesEntry(entry, variable) {
		t.Fatal("confidential reusable value was rejected for a confidential Entry")
	}
}
