package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredAttachGenerationReader interface {
	GetBlueprintAttachInputGeneration(
		context.Context,
		string,
		string,
	) (keyvalue.Versioned[blueprintattachinputs.Generation], bool, error)
}

func (service *Service) loadAuthoredAttachGenerationAuthority(
	ctx context.Context,
	input authoredParentInput,
) (map[string]blueprintattachinputs.Generation, error) {
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach input authority is not configured")
	}
	result := make(map[string]blueprintattachinputs.Generation)
	failed := true
	defer func() {
		if failed {
			clearAuthoredAttachGenerationAuthority(result)
		}
	}()
	for _, identity := range input.identities.Attaches {
		generation, found, err := reader.GetBlueprintAttachInputGeneration(
			ctx, input.desired.RevisionID, identity.ID,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		parentOwned := generation.Record.OwnerKind == blueprintattachinputs.OwnerParent &&
			generation.Record.OwnerTaskID == input.desired.RevisionID && generation.Record.Transfer == nil
		childOwned := generation.Record.OwnerKind == blueprintattachinputs.OwnerChild &&
			generation.Record.Transfer != nil &&
			generation.Record.Transfer.ParentTaskID == input.desired.RevisionID &&
			generation.Record.Transfer.ChildTaskID == generation.Record.OwnerTaskID
		if generation.Record.EnvironmentID != input.desired.EnvironmentID ||
			generation.Record.ParentTaskID != input.desired.RevisionID || (!parentOwned && !childOwned) {
			blueprintattachinputs.Clear(&generation.Record)
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach input generation ownership changed")
		}
		result[identity.ID] = generation.Record
	}
	failed = false
	return result, nil
}

func clearAuthoredAttachGenerationAuthority(generations map[string]blueprintattachinputs.Generation) {
	for id, generation := range generations {
		blueprintattachinputs.Clear(&generation)
		generations[id] = generation
	}
}
