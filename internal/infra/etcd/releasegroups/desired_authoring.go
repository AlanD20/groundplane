package releasegroups

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DesiredAuthoring projects one prepared Release Group mutation into portable
// desired input using Service names from the immutable projection being revised.
type DesiredAuthoring struct {
	environmentID string
	previousName  string
	candidate     *domain.Group
}

// PrepareDesiredAuthoring pins the current Group identity used by the prepared
// mutation. The prepared mutation's existing CAS remains the publication fence.
func PrepareDesiredAuthoring(
	ctx context.Context,
	store preparationStore,
	prepared ReleaseGroupPreparedMutation,
) (DesiredAuthoring, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ReleaseGroupRecordKey(prepared.groupID)},
	})
	if err != nil {
		return DesiredAuthoring{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return DesiredAuthoring{}, errs.New(
			errs.KindStateConflict,
			"release group desired authoring snapshot is unavailable",
		)
	}

	var previous domain.Group
	if prepared.groupRevision == 0 {
		if read.Values[0] != nil || prepared.taskType != taskjournal.TaskCreate {
			return DesiredAuthoring{}, errs.New(
				errs.KindStateConflict,
				"release group desired authoring creation target changed",
			)
		}
	} else {
		if read.Values[0] == nil || read.Values[0].ModRevision != prepared.groupRevision {
			return DesiredAuthoring{}, errs.New(
				errs.KindStateConflict,
				"release group desired authoring target changed",
			)
		}
		previous, err = DecodeReleaseGroupStored(read.Values[0].Value)
		if err != nil || previous.ID != prepared.groupID || previous.EnvironmentID != prepared.environmentID {
			return DesiredAuthoring{}, errs.New(
				errs.KindInternal,
				"release group desired authoring target is corrupt",
			)
		}
	}

	authoring := DesiredAuthoring{
		environmentID: prepared.environmentID,
		previousName:  previous.Name,
	}
	if prepared.taskType == taskjournal.TaskRemove {
		return authoring, nil
	}
	candidate, err := preparedAuthoringCandidate(prepared)
	if err != nil {
		return DesiredAuthoring{}, err
	}
	authoring.candidate = &candidate
	return authoring, nil
}

// Apply mutates only the targeted Release Group declaration. This preserves
// previously admitted removals even when their settlement Task later fails.
func (authoring DesiredAuthoring) Apply(
	input *core.BlueprintDesiredInput,
	projection *projectionrecord.EnvironmentComposeProjection,
) error {
	if input == nil {
		return errs.New(errs.KindInternal, "release group desired input is missing")
	}
	if projection == nil || projection.EnvironmentID != authoring.environmentID {
		return errs.New(errs.KindInternal, "release group desired projection is invalid")
	}
	if authoring.previousName != "" {
		delete(input.ReleaseGroups, authoring.previousName)
	}
	if authoring.candidate != nil {
		spec, err := authoredSpec(*authoring.candidate, *projection)
		if err != nil {
			return err
		}
		if input.ReleaseGroups == nil {
			input.ReleaseGroups = make(map[string]core.ReleaseGroupSpec)
		}
		input.ReleaseGroups[authoring.candidate.Name] = spec
	}
	if len(input.ReleaseGroups) == 0 {
		input.ReleaseGroups = nil
	}
	return nil
}

func preparedAuthoringCandidate(prepared ReleaseGroupPreparedMutation) (domain.Group, error) {
	key := ReleaseGroupRecordKey(prepared.groupID)
	var candidate *domain.Group
	for _, mutation := range prepared.mutations {
		if mutation.Type != etcdstore.MutationPut || mutation.Key != key {
			continue
		}
		group, err := DecodeReleaseGroupStored(mutation.Value)
		if err != nil || group.ID != prepared.groupID || group.EnvironmentID != prepared.environmentID ||
			candidate != nil {
			return domain.Group{}, errs.New(errs.KindInternal, "release group prepared authoring candidate is invalid")
		}
		candidate = &group
	}
	if candidate == nil {
		return domain.Group{}, errs.New(errs.KindInternal, "release group prepared authoring candidate is absent")
	}
	return *candidate, nil
}

func authoredSpec(
	group domain.Group,
	projection projectionrecord.EnvironmentComposeProjection,
) (core.ReleaseGroupSpec, error) {
	names := make(map[string]string, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if existing, duplicate := names[service.Desired.ID]; duplicate && existing != service.Desired.Name {
			return core.ReleaseGroupSpec{}, errs.New(
				errs.KindInternal,
				"release group desired Service identity is ambiguous",
			)
		}
		names[service.Desired.ID] = service.Desired.Name
	}
	services, err := serviceNames(group.ServiceIDs, names)
	if err != nil {
		return core.ReleaseGroupSpec{}, err
	}
	order, err := serviceNames(group.Order, names)
	if err != nil {
		return core.ReleaseGroupSpec{}, err
	}
	spec := core.ReleaseGroupSpec{
		Services:  services,
		Order:     order,
		Tag:       group.DefaultTag,
		OnFailure: core.OnFailure(group.OnFailure),
	}
	if err := spec.Validate(group.Name); err != nil {
		return core.ReleaseGroupSpec{}, errs.Wrap(errs.KindInternal, err)
	}
	return spec, nil
}

func serviceNames(ids []string, names map[string]string) ([]string, error) {
	result := make([]string, len(ids))
	for index, id := range ids {
		name, found := names[id]
		if !found {
			return nil, errs.New(errs.KindInternal, "release group desired Service identity is missing")
		}
		result[index] = name
	}
	return result, nil
}
