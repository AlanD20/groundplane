package backupstage

import "context"

// PublishedArtifact selects an already-owned immutable descriptor, never a path
// discovered on disk. The caller must still verify its content against captured
// operation evidence before using it for recovery.
func (stage *Stage) PublishedArtifact(ctx context.Context, name string) (*Artifact, ArtifactEvidence, error) {
	if err := contextError(ctx); err != nil {
		return nil, ArtifactEvidence{}, err
	}
	if stage == nil {
		return nil, ArtifactEvidence{}, internalError("staging owner is required")
	}
	if err := validateFinalArtifactName(name); err != nil {
		return nil, ArtifactEvidence{}, err
	}
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if stage.closed || stage.cleaned {
		return nil, ArtifactEvidence{}, internalError("staging owner is closed")
	}
	var selected *Artifact
	for artifact := range stage.artifacts {
		if artifact.final != name {
			continue
		}
		if selected != nil || artifact.state != artifactPublished || artifact.file == nil {
			return nil, ArtifactEvidence{}, internalError("retained artifact is not uniquely published")
		}
		selected = artifact
	}
	if selected == nil {
		return nil, ArtifactEvidence{}, internalError("published staging artifact is unavailable")
	}
	if err := validateRegularFD(ctx, int(selected.file.Fd()), selected.size, 1); err != nil {
		return nil, ArtifactEvidence{}, err
	}
	return selected, selected.evidenceLocked(), nil
}
