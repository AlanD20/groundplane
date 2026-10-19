package blueprint

import (
	"context"
	"slices"
	"sort"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentBlueprintRequirementTaskEdges(
	ctx context.Context,
	repository environmentBlueprintRepository,
	environmentID, rootTaskID string,
	requirements core.BlueprintRequirements,
	revision int64,
) ([]core.BlueprintRequirementTaskEdge, error) {
	if len(requirements.Resolved) == 0 {
		return nil, nil
	}
	pending := make([]string, 0, len(requirements.Resolved))
	for _, requirement := range requirements.Resolved {
		pending = append(pending, requirement.Target.TaskID)
	}
	sort.Strings(pending)
	pending = slices.Compact(pending)
	edges := make([]core.BlueprintRequirementTaskEdge, 0)
	for _, taskID := range pending {
		authority, found, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environmentID, taskID, revision)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		captured, _, err := core.BuildBlueprintRequirementTaskGraph(taskID,
			authority.Record.Requirements, authority.Record.InheritedTaskEdges)
		if err != nil {
			return nil, err
		}
		edges = append(edges, captured...)
	}
	if _, _, err := core.BuildBlueprintRequirementTaskGraph(rootTaskID, requirements, edges); err != nil {
		return nil, err
	}
	return edges, nil
}
func environmentBlueprintRequirements(
	authored []core.Requirement,
	candidates map[string]core.AttachmentSpec,
	current []etcdstore.Versioned[attachrecord.Record],
	readRevision int64,
) (core.BlueprintRequirements, error) {
	available := make([]core.RequirementResolutionTarget, len(current))
	for index, versioned := range current {
		if versioned.ReadRevision != 0 && versioned.ReadRevision != readRevision {
			return core.BlueprintRequirements{}, errs.New(
				errs.KindInternal,
				"Blueprint requirement Attach snapshot changed revision",
			)
		}
		available[index] = core.RequirementResolutionTarget{
			Kind: core.RequirementTargetBackingAttach, Name: versioned.Record.Name, ID: versioned.Record.ID,
			TaskID: versioned.Record.TaskID, Revision: versioned.Revision,
		}
	}
	candidateNames := make([]string, 0, len(candidates))
	for name := range candidates {
		candidateNames = append(candidateNames, name)
	}
	sort.Strings(candidateNames)
	return core.ResolveBlueprintRequirements(authored, available, candidateNames, readRevision)
}
