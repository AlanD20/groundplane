package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintZoneRemovalPublication struct {
	intent environmentchanges.ZoneRemovalIntent
}

func NewBlueprintZoneRemovalPublication(
	task TaskRecord, intent environmentchanges.ZoneRemovalIntent,
) (BlueprintZoneRemovalPublication, error) {
	if intent.Authored == nil || validateZoneRemovalTaskOwner(task, intent) != nil {
		return BlueprintZoneRemovalPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Zone removal publication is invalid",
		)
	}
	return BlueprintZoneRemovalPublication{intent: environmentchanges.CloneZoneRemovalIntent(intent)}, nil
}

func (publication BlueprintZoneRemovalPublication) IsZero() bool {
	return publication.intent.OperationID == ""
}

func (publication *BlueprintZoneRemovalPublication) Clear() {
	if publication == nil {
		return
	}
	clear(publication.intent.AppliedProjection.ComposeArtifact)
	clear(publication.intent.AppliedProjection.NormalizedCompose)
	clear(publication.intent.CandidateProjection.ComposeArtifact)
	clear(publication.intent.CandidateProjection.NormalizedCompose)
	*publication = BlueprintZoneRemovalPublication{}
}
