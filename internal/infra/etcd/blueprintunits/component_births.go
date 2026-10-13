package blueprintunits

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// CaptureComponentBirths selects immutable birth authority for persistent
// Environment Component slots. A slot can disappear from an authored head
// without becoming a new resource when it is selected again. This authority
// does not depend on retaining an otherwise retired Blueprint's runtime files.
func (repository *Repository) CaptureComponentBirths(
	ctx context.Context, environmentID string, componentIDs []string, revision int64,
) (map[string]string, []etcdstore.Condition, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision <= 0 {
		return nil, nil, invalidRecord()
	}
	selected := make(map[string]bool, len(componentIDs))
	for _, id := range componentIDs {
		if ids.Validate(ids.KindComponent, id) != nil || selected[id] {
			return nil, nil, invalidRecord()
		}
		selected[id] = true
	}
	snapshot := Snapshot{EnvironmentID: environmentID, ReadRevision: revision}
	if err := repository.loadInitialAbsence(ctx, &snapshot); err != nil {
		return nil, nil, err
	}
	if _, err := seededTargets(snapshot); err != nil {
		return nil, nil, err
	}
	births := make(map[string]string, len(componentIDs))
	var conditions []etcdstore.Condition
	for _, proof := range snapshot.initialAbsence {
		used := false
		for _, target := range proof.Record.Targets {
			if target.Kind == ids.KindComponent && selected[target.ID] {
				births[target.ID] = proof.Record.BirthRevisionID
				used = true
			}
		}
		if used {
			conditions = append(conditions, etcdstore.Condition{
				Key:         initialAbsenceKey(environmentID, proof.Record.BirthRevisionID),
				ModRevision: proof.Revision,
			})
		}
	}
	return births, conditions, nil
}
