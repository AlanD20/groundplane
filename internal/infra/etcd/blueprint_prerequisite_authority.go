package etcd

import (
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintPrerequisiteAuthority retains the exact pre-stage prerequisite
// resolution used by one authored revision. Derived execution authority is
// deliberately separate from the operator-authored desired input.
type BlueprintPrerequisiteAuthority struct {
	Schema             uint8                               `json:"schema"`
	EnvironmentID      string                              `json:"environment_id"`
	RevisionID         string                              `json:"revision_id"`
	Requirements       core.BlueprintRequirements          `json:"requirements"`
	InheritedTaskEdges []core.BlueprintRequirementTaskEdge `json:"inherited_task_edges,omitempty"`
}

func NewBlueprintPrerequisiteAuthority(
	environmentID, revisionID string,
	requirements core.BlueprintRequirements,
	inherited []core.BlueprintRequirementTaskEdge,
) (BlueprintPrerequisiteAuthority, error) {
	edges := slices.Clone(inherited)
	sort.Slice(edges, func(left, right int) bool {
		if edges[left].PrerequisiteTaskID != edges[right].PrerequisiteTaskID {
			return edges[left].PrerequisiteTaskID < edges[right].PrerequisiteTaskID
		}
		return edges[left].DependentTaskID < edges[right].DependentTaskID
	})
	edges = slices.Compact(edges)
	authority := BlueprintPrerequisiteAuthority{
		Schema: 1, EnvironmentID: environmentID, RevisionID: revisionID,
		Requirements: requirements.Clone(), InheritedTaskEdges: edges,
	}
	if err := authority.validate(); err != nil {
		return BlueprintPrerequisiteAuthority{}, err
	}
	return authority, nil
}

func (authority BlueprintPrerequisiteAuthority) Clone() BlueprintPrerequisiteAuthority {
	authority.Requirements = authority.Requirements.Clone()
	authority.InheritedTaskEdges = slices.Clone(authority.InheritedTaskEdges)
	return authority
}

func (authority BlueprintPrerequisiteAuthority) validate() error {
	if authority.Schema != 1 || ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, authority.RevisionID) != nil || authority.Requirements.Validate() != nil ||
		(len(authority.Requirements.Resolved) == 0 && len(authority.InheritedTaskEdges) != 0) {
		return errs.New(errs.KindValidationFailed, "Blueprint prerequisite authority is invalid")
	}
	for index, edge := range authority.InheritedTaskEdges {
		if index != 0 {
			previous := authority.InheritedTaskEdges[index-1]
			if previous.PrerequisiteTaskID > edge.PrerequisiteTaskID ||
				(previous.PrerequisiteTaskID == edge.PrerequisiteTaskID && previous.DependentTaskID >= edge.DependentTaskID) {
				return errs.New(errs.KindValidationFailed, "Blueprint prerequisite authority edges are invalid")
			}
		}
	}
	if _, _, err := core.BuildBlueprintRequirementTaskGraph(
		authority.RevisionID, authority.Requirements, authority.InheritedTaskEdges,
	); err != nil {
		return err
	}
	return nil
}

func (authority BlueprintPrerequisiteAuthority) matchesAuthored(
	desired projectionrecord.EnvironmentDesiredInput,
) bool {
	if authority.EnvironmentID != desired.EnvironmentID || authority.RevisionID != desired.RevisionID {
		return false
	}
	authored, err := core.NormalizeRequirements(desired.Input.Requires)
	if err != nil || len(authored) != len(authority.Requirements.Authored) {
		return false
	}
	for index := range authored {
		stored := authority.Requirements.Authored[index]
		if authored[index].Target != stored.Target || authored[index].Condition != stored.Condition ||
			!slices.Equal(authored[index].Phases, stored.Phases) {
			return false
		}
	}
	return true
}

func blueprintPrerequisiteAuthorityKey(environmentID, revisionID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, revisionID) + "prerequisite-authority"
}

func encodeBlueprintPrerequisiteAuthority(authority BlueprintPrerequisiteAuthority) ([]byte, error) {
	if err := authority.validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint-prerequisite-authority", authority)
}

func decodeBlueprintPrerequisiteAuthority(value []byte) (BlueprintPrerequisiteAuthority, error) {
	authority, err := recordcodec.Decode[BlueprintPrerequisiteAuthority](value, "blueprint-prerequisite-authority")
	if err != nil || authority.validate() != nil {
		return BlueprintPrerequisiteAuthority{}, errs.New(
			errs.KindInternal, "Blueprint prerequisite authority is corrupt",
		)
	}
	return authority, nil
}

type preparedBlueprintPrerequisiteAuthority struct {
	conditions []etcdstore.Condition
	mutation   etcdstore.Mutation
}

func prepareBlueprintPrerequisiteAuthorityPublication(
	authority BlueprintPrerequisiteAuthority,
	desired projectionrecord.EnvironmentDesiredInput,
) (preparedBlueprintPrerequisiteAuthority, error) {
	if err := authority.validate(); err != nil || !authority.matchesAuthored(desired) {
		return preparedBlueprintPrerequisiteAuthority{}, errs.New(
			errs.KindValidationFailed, "Blueprint prerequisite authority does not match authored input",
		)
	}
	value, err := encodeBlueprintPrerequisiteAuthority(authority)
	if err != nil {
		return preparedBlueprintPrerequisiteAuthority{}, err
	}
	key := blueprintPrerequisiteAuthorityKey(authority.EnvironmentID, authority.RevisionID)
	prepared := preparedBlueprintPrerequisiteAuthority{
		conditions: []etcdstore.Condition{{Key: key}},
		mutation:   etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value},
	}
	for _, requirement := range authority.Requirements.Resolved {
		prepared.conditions = append(prepared.conditions, etcdstore.Condition{
			Key: attachrecord.AttachKey(requirement.Target.ID), ModRevision: requirement.Target.Revision,
		})
	}
	return prepared, nil
}

func (prepared *preparedBlueprintPrerequisiteAuthority) clear() {
	if prepared == nil {
		return
	}
	clear(prepared.mutation.Value)
	*prepared = preparedBlueprintPrerequisiteAuthority{}
}

func (repository *EnvironmentBlueprintRepository) GetBlueprintPrerequisiteAuthority(
	ctx context.Context,
	environmentID, revisionID string,
) (etcdstore.Versioned[BlueprintPrerequisiteAuthority], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{}, false, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, revisionID) != nil {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{}, false, errs.New(
			errs.KindValidationFailed, "Blueprint prerequisite authority read is invalid",
		)
	}
	result, err := repository.store.Get(ctx, blueprintPrerequisiteAuthorityKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{}, false, errs.New(
			errs.KindInternal, "Blueprint prerequisite authority read is incomplete",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{ReadRevision: result.ReadRevision}, false, nil
	}
	authority, err := decodeBlueprintPrerequisiteAuthority(result.Entry.Value)
	if err != nil || authority.EnvironmentID != environmentID || authority.RevisionID != revisionID {
		return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{}, false, errs.New(
			errs.KindInternal, "Blueprint prerequisite authority is corrupt",
		)
	}
	return etcdstore.Versioned[BlueprintPrerequisiteAuthority]{
		Record: authority, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
