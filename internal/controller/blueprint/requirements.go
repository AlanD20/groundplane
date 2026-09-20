package blueprint

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintRequirementGateReader interface {
	GetBlueprintRequirementGateAtRevision(
		context.Context,
		string,
		int64,
	) (etcdstore.Versioned[etcd.BlueprintRequirementGate], bool, error)
}

func environmentBlueprintRequirementTaskEdges(
	ctx context.Context,
	repository environmentBlueprintRepository,
	rootTaskID string,
	requirements core.BlueprintRequirements,
	revision int64,
) ([]core.BlueprintRequirementTaskEdge, error) {
	if len(requirements.Resolved) == 0 {
		return nil, nil
	}
	reader, ok := repository.(environmentBlueprintRequirementGateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint requirement gate repository is not configured")
	}
	pending := make([]string, 0, len(requirements.Resolved))
	for _, requirement := range requirements.Resolved {
		pending = append(pending, requirement.Target.TaskID)
	}
	sort.Strings(pending)
	seen := make(map[string]struct{}, len(pending))
	edges := make([]core.BlueprintRequirementTaskEdge, 0)
	for len(pending) != 0 {
		taskID := pending[0]
		pending = pending[1:]
		if _, duplicate := seen[taskID]; duplicate {
			continue
		}
		seen[taskID] = struct{}{}
		gate, found, err := reader.GetBlueprintRequirementGateAtRevision(ctx, taskID, revision)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		edges = append(edges, gate.Record.DAG.TaskEdges...)
		for _, requirement := range gate.Record.DAG.Requirements {
			if _, visited := seen[requirement.TargetTaskID]; !visited {
				pending = append(pending, requirement.TargetTaskID)
			}
		}
		sort.Strings(pending)
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
