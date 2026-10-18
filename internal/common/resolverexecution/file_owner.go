package resolverexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// FileTransactionID binds one actual forward attempt to its captured file.
// Explicit Retry receives a different transaction; same-attempt replay does not.
func FileTransactionID(attempt workexecution.Attempt, configuration Configuration) (string, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || configuration.Generation == 0 ||
		configuration.Apply == nil {
		return "", invalid("DNS resolver file ownership requires its actual captured forward attempt")
	}
	digest := sha256.Sum256([]byte(attempt.TaskID + "\x00" + attempt.WorkItemID + "\x00" + attempt.ID + "\x00" + configuration.Apply.ArtifactId))
	return "mct_" + hex.EncodeToString(digest[:]), nil
}

func ValidFileOwner(owner string) bool {
	if !strings.HasPrefix(owner, "mct_") || len(owner) != 68 {
		return false
	}
	decoded, err := hex.DecodeString(owner[4:])
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == owner[4:]
}
