package componentexecution

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Restoration retains the expected predecessor identity, not its executable
// contents. The host still requires the matching captured input to restore it.
type Restoration struct {
	Layout              Layout `json:"layout"`
	Absent              bool   `json:"absent"`
	RuntimeSHA256       string `json:"runtime_sha256,omitempty"`
	ConfigurationSHA256 string `json:"configuration_sha256,omitempty"`
}

func CaptureRestoration(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) (Restoration, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return Restoration{}, err
	}
	result := Restoration{Layout: layout, Absent: input.Predecessor.Absent}
	if !result.Absent {
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(input.Predecessor.Runtime)
		if err != nil {
			return Restoration{}, err
		}
		digest := sha256.Sum256(raw)
		clear(raw)
		result.RuntimeSHA256 = hex.EncodeToString(digest[:])
	}
	if input.PriorConfiguration != nil {
		result.ConfigurationSHA256 = hex.EncodeToString(input.PriorConfiguration.Sha256)
	}
	return result, result.Validate(attempt)
}

func (restoration Restoration) Validate(attempt workexecution.Attempt) error {
	if restoration.Layout.Validate(attempt) != nil || restoration.Absent != (restoration.RuntimeSHA256 == "") {
		return invalidProgress()
	}
	for _, value := range []string{restoration.RuntimeSHA256, restoration.ConfigurationSHA256} {
		if value == "" {
			continue
		}
		digest, err := hex.DecodeString(value)
		if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != value {
			return invalidProgress()
		}
	}
	return nil
}
