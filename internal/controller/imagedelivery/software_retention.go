package imagedelivery

import preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"

func (retention imageRetention) software(result preparation.Result, reason string) {
	if result.Controller != nil {
		retention.retain(result.Controller.Artifact.Reference, reason)
		retention.retain(result.Controller.Artifact.ConfigDigest, reason)
	}
	if result.Agent != nil {
		retention.retain(result.Agent.Artifact.Reference, reason)
		retention.retain(result.Agent.Artifact.ConfigDigest, reason)
	}
}
