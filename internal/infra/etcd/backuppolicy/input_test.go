package backuppolicy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReplacementInputProtectsConfigEncryptionAndSourceIdentity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	connectorID := ids.NewAt(ids.KindConnector, now, 2)
	config := BackupPolicySourceSelection{Kind: core.BackupSourceConfig, TargetID: environmentID}
	valid := BackupPolicyReplacementInput{
		EnvironmentID: environmentID,
		Frequency:     "*-*-* 03:00:00",
		Keep:          7,
		Encryption:    "age",
		ConnectorID:   connectorID,
		Sources:       []BackupPolicySourceSelection{config},
	}
	if err := ValidateReplacementInput(context.Background(), valid); err != nil {
		t.Fatalf("ValidateReplacementInput(disabled age config) error = %v", err)
	}
	for _, test := range []struct {
		name  string
		input BackupPolicyReplacementInput
	}{
		{name: "config with none", input: func() BackupPolicyReplacementInput {
			input := valid
			input.Encryption = "none"
			return input
		}()},
		{name: "duplicate source identity", input: func() BackupPolicyReplacementInput {
			input := valid
			input.Sources = []BackupPolicySourceSelection{config, config}
			return input
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateReplacementInput(context.Background(), test.input); !errors.Is(
				err, errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("ValidateReplacementInput() error = %v", err)
			}
		})
	}
}
