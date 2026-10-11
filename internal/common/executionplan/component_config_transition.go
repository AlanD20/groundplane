package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateContainerConfigTransition(action *agentpb.ComponentApply) error {
	transition := action.GetContainerConfigTransition()
	if transition == nil {
		return nil
	}
	if action.GetManagedConfigContent() || len(transition.GetPreviousSource()) > envoyrouter.MaximumSourceBytes {
		return errs.New(errs.KindValidationFailed, "container configuration transition has invalid source authority")
	}
	desiredDigest := sha256.Sum256(transition.GetDesiredSource())
	if !bytes.Equal(desiredDigest[:], action.GetArtifactDigest()) {
		return errs.New(errs.KindValidationFailed, "desired router configuration does not match its captured digest")
	}
	if _, err := envoyrouter.Read(transition.GetDesiredSource()); err != nil {
		return err
	}
	if len(transition.PreviousSource) == 0 {
		if len(transition.PreviousSourceSha256) != 0 {
			return errs.New(errs.KindValidationFailed, "new router transition contains a previous configuration digest")
		}
		return nil
	}
	digest := sha256.Sum256(transition.PreviousSource)
	if !bytes.Equal(digest[:], transition.PreviousSourceSha256) {
		return errs.New(errs.KindValidationFailed, "previous router configuration does not match its captured digest")
	}
	_, err := envoyrouter.PrepareActivation(transition.PreviousSource, transition.DesiredSource)
	return err
}
