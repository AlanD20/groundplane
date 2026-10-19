package etcd

import (
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func releaseRollbackMaterial(intent domain.Intent, state domain.State, terminalAt time.Time) domain.RollbackMaterial {
	references := []string{intent.RenderInputID, intent.CandidateWorkload.LocalImageID}
	digest, _ := domain.Digest(references)
	material := domain.RollbackMaterial{
		ReleaseID: intent.ID, Status: domain.RetentionAvailable,
		References: references, Digest: digest, Revision: 1,
	}
	if state != domain.StateCompleted {
		material.Status = domain.RetentionExpired
		expired := terminalAt
		material.ExpiredAt = &expired
	}
	return material
}
