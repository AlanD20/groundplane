package entryexecution

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Restoration records predecessor fingerprints without duplicating executable
// Compose input or file contents in the Agent's durable execution journal.
type Restoration struct {
	Layout              Layout `json:"layout"`
	BaselineSHA256      string `json:"baseline_sha256"`
	ConfigurationSHA256 string `json:"configuration_sha256,omitempty"`
}

func CaptureRestoration(
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
) (Restoration, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return Restoration{}, err
	}
	result := Restoration{Layout: layout, BaselineSHA256: hex.EncodeToString(input.Baseline.YamlSha256)}
	if input.PriorConfiguration != nil {
		result.ConfigurationSHA256 = hex.EncodeToString(input.PriorConfiguration.Sha256)
	}
	return result, result.Validate(attempt)
}

func (restoration Restoration) Validate(attempt workexecution.Attempt) error {
	if restoration.Layout.Validate(attempt) != nil || restoration.BaselineSHA256 == "" {
		return invalidLayout()
	}
	for _, value := range []string{restoration.BaselineSHA256, restoration.ConfigurationSHA256} {
		if value == "" {
			continue
		}
		digest, err := hex.DecodeString(value)
		if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != value {
			return invalidLayout()
		}
	}
	return nil
}
