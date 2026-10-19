package materializerrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// One captured file boundary has one helper name. Docker rejects a second
// launch while an orphan still owns that name instead of permitting concurrent
// writers. Content and its digest are deliberately absent from public labels.
type Ownership struct {
	TaskID, StageID, EnvironmentID, Destination string
	Generation                                  uint64
}

func OwnershipFromHeader(header entrymaterialization.Header) Ownership {
	return Ownership{TaskID: header.TaskID(), StageID: header.StepID(), EnvironmentID: header.EnvironmentID(),
		Destination: header.Destination(), Generation: header.Generation()}
}

func helperName(owner Ownership) string {
	digest := sha256.Sum256([]byte(owner.TaskID + "\x00" + owner.StageID + "\x00" + owner.Destination))
	return "gp-materializer-" + hex.EncodeToString(digest[:])
}

func helperLabels(owner Ownership) map[string]string {
	return map[string]string{
		"io.groundplane.role":                     "configuration-materializer",
		"io.groundplane.task":                     owner.TaskID,
		"io.groundplane.stage":                    owner.StageID,
		"io.groundplane.environment":              owner.EnvironmentID,
		"io.groundplane.configuration-generation": strconv.FormatUint(owner.Generation, 10),
	}
}

func validateOwnership(owner Ownership) error {
	if ids.Validate(ids.KindTask, owner.TaskID) != nil || ids.Validate(ids.KindStep, owner.StageID) != nil ||
		ids.Validate(
			ids.KindEnvironment,
			owner.EnvironmentID,
		) != nil || owner.Generation == 0 || owner.Destination == "" {
		return errs.New(errs.KindValidationFailed, "materializer requires captured file ownership")
	}
	return nil
}
