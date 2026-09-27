package runners

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type parentFence struct {
	condition keyvalue.Condition
	id        string
	label     string
	notFound  errs.Kind
	deletion  deletions.DeletionTargetKind
}

func (parents RunnerParents) fences() []parentFence {
	fences := []parentFence{{
		condition: keyvalue.Condition{
			Key:         hierarchy.TenantKey(parents.tenant.Record.ID),
			ModRevision: parents.tenant.Revision,
		},
		id: parents.tenant.Record.ID, label: "tenant", notFound: errs.KindTenantNotFound,
		deletion: deletions.DeletionTargetTenant,
	}}
	if parents.project.Record.ID != "" {
		fences = append(fences, parentFence{
			condition: keyvalue.Condition{
				Key:         hierarchy.ProjectKey(parents.project.Record.ID),
				ModRevision: parents.project.Revision,
			},
			id: parents.project.Record.ID, label: "project", notFound: errs.KindProjectNotFound,
			deletion: deletions.DeletionTargetProject,
		})
	}
	if parents.environment.Record.ID != "" {
		fences = append(fences, parentFence{
			condition: keyvalue.Condition{
				Key:         hierarchy.EnvironmentKey(parents.environment.Record.ID),
				ModRevision: parents.environment.Revision,
			},
			id: parents.environment.Record.ID, label: "environment", notFound: errs.KindEnvironmentNotFound,
			deletion: deletions.DeletionTargetEnvironment,
		})
	}
	return fences
}

// ExistenceConditions binds the complete resolved owner chain into Task authority.
func (parents RunnerParents) ExistenceConditions() []keyvalue.Condition {
	fences := parents.fences()
	conditions := make([]keyvalue.Condition, 0, len(fences))
	for _, fence := range fences {
		conditions = append(conditions, fence.condition)
	}
	return conditions
}

// AdmissionConditions also prevents publication while any parent is deleting.
func (parents RunnerParents) AdmissionConditions() []keyvalue.Condition {
	fences := parents.fences()
	conditions := make([]keyvalue.Condition, 0, len(fences)*2)
	for _, fence := range fences {
		conditions = append(conditions, fence.condition,
			keyvalue.Condition{Key: deletions.TombstoneKey(string(fence.deletion), fence.id)},
		)
	}
	return conditions
}

func (parents RunnerParents) ClassifyAdmissionConflict(values []*keyvalue.KeyValue) error {
	fences := parents.fences()
	if len(values) != len(fences)*2 {
		return errs.New(errs.KindInternal, "Runner parent compare evidence is incomplete")
	}
	for index, fence := range fences {
		current, deletion := values[2*index], values[2*index+1]
		if current == nil {
			return errs.New(fence.notFound, fence.label+" was not found")
		}
		if current.Key != fence.condition.Key {
			return errs.New(errs.KindInternal, "Runner parent compare evidence is mismatched")
		}
		if current.ModRevision != fence.condition.ModRevision {
			return recordcodec.StateConflict(fence.label, fence.id)
		}
		if deletion != nil {
			return errs.New(errs.KindResourceInUse, "Runner hierarchy deletion is in progress")
		}
	}
	return nil
}
