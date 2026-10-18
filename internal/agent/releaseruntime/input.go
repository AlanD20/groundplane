// Package releaseruntime owns native Service Release execution on an Agent host.
package releaseruntime

import (
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func Open(message *agentpb.ReleaseWorkAssignment, identity workexecution.Identity, volumeRoot string) (releaseexecution.Input, error) {
	attempt, release, err := workexecution.DecodeReleaseDelivery(message, identity)
	if err != nil {
		return releaseexecution.Input{}, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		return releaseexecution.Input{}, err
	}
	input := releaseexecution.Input{Attempt: attempt, Release: release, Artifacts: artifacts}
	if err := validateStorage(input, volumeRoot); err != nil {
		return releaseexecution.Input{}, err
	}
	return input, nil
}

func OpenRecovery(message *agentpb.ReleaseRecoveryAssignment, identity workexecution.Identity, volumeRoot string) (releaseexecution.RecoveryInput, error) {
	input, err := releaseexecution.DecodeRecoveryAssignment(message, identity)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	if err := validateStorage(input.Input, volumeRoot); err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	return input, nil
}

func validateStorage(input releaseexecution.Input, volumeRoot string) error {
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return err
	}
	artifacts := input.Artifacts
	owned := []*agentpb.ComposeArtifact{artifacts.Candidate}
	if artifacts.Prior != nil {
		owned = append(owned, artifacts.Prior.Artifact)
		if artifacts.Prior.Retained != nil {
			owned = append(owned, artifacts.Prior.Retained)
		}
	}
	for _, artifact := range owned {
		scope, err := environmentpath.Parse(volumeRoot, artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != input.Release.EnvironmentId {
			return errs.New(errs.KindValidationFailed, "Release artifact volume directory is outside this Agent's Environment storage root")
		}
	}
	return nil
}
