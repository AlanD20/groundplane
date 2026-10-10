package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredPrerequisiteAuthorityReader interface {
	GetBlueprintPrerequisiteAuthority(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority], bool, error)
}

func (service *Service) prepareAuthoredPrerequisiteAuthority(
	ctx context.Context,
	environmentID, revisionID string,
	requirements core.BlueprintRequirements,
) (etcd.BlueprintPrerequisiteAuthority, error) {
	var inherited []core.BlueprintRequirementTaskEdge
	var err error
	if len(requirements.Resolved) != 0 {
		inherited, err = environmentBlueprintRequirementTaskEdges(
			ctx, service.repository, revisionID, requirements, requirements.ResolutionRevision,
		)
		if err != nil {
			return etcd.BlueprintPrerequisiteAuthority{}, err
		}
	}
	return etcd.NewBlueprintPrerequisiteAuthority(environmentID, revisionID, requirements, inherited)
}

func (service *Service) loadAuthoredPrerequisiteAuthority(
	ctx context.Context,
	environmentID, revisionID string,
) (etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority], error) {
	reader, ok := service.repository.(authoredPrerequisiteAuthorityReader)
	if !ok {
		return etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority]{}, errs.New(
			errs.KindInternal, "Blueprint prerequisite authority repository is not configured",
		)
	}
	authority, found, err := reader.GetBlueprintPrerequisiteAuthority(ctx, environmentID, revisionID)
	if err != nil {
		return etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority]{}, err
	}
	if !found || authority.Revision <= 0 || authority.ReadRevision < authority.Revision ||
		authority.Record.EnvironmentID != environmentID || authority.Record.RevisionID != revisionID {
		return etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority]{}, errs.New(
			errs.KindStateConflict, "Blueprint prerequisite authority is unavailable",
		)
	}
	return authority, nil
}

func (service *Service) loadAuthoredRuntimeRequirements(
	ctx context.Context,
	desired etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput],
	expectedHeadRevision int64,
) (core.BlueprintRequirements, error) {
	authority, err := service.loadAuthoredPrerequisiteAuthority(
		ctx, desired.Record.EnvironmentID, desired.Record.RevisionID,
	)
	if err != nil {
		return core.BlueprintRequirements{}, err
	}
	if authority.Revision != expectedHeadRevision ||
		!sameAuthoredRequirements(authority.Record.Requirements.Authored, desired.Record.Input.Requires) {
		return core.BlueprintRequirements{}, errs.New(
			errs.KindStateConflict, "Blueprint prerequisite authority changed",
		)
	}
	return authority.Record.Requirements.Clone(), nil
}

func sameBlueprintRequirements(left, right core.BlueprintRequirements) bool {
	if left.ResolutionRevision != right.ResolutionRevision ||
		!sameAuthoredRequirements(left.Authored, right.Authored) ||
		len(left.Resolved) != len(right.Resolved) {
		return false
	}
	for index := range left.Resolved {
		leftRequirement, rightRequirement := left.Resolved[index], right.Resolved[index]
		if leftRequirement.Target != rightRequirement.Target ||
			leftRequirement.Condition != rightRequirement.Condition ||
			!slices.Equal(leftRequirement.Phases, rightRequirement.Phases) {
			return false
		}
	}
	return true
}

func sameAuthoredRequirements(left, right []core.Requirement) bool {
	leftNormalized, leftErr := core.NormalizeRequirements(left)
	rightNormalized, rightErr := core.NormalizeRequirements(right)
	if leftErr != nil || rightErr != nil || len(leftNormalized) != len(rightNormalized) {
		return false
	}
	for index := range leftNormalized {
		if leftNormalized[index].Target != rightNormalized[index].Target ||
			leftNormalized[index].Condition != rightNormalized[index].Condition ||
			!slices.Equal(leftNormalized[index].Phases, rightNormalized[index].Phases) {
			return false
		}
	}
	return true
}
