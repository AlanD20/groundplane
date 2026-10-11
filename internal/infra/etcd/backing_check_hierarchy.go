package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) captureBackingCheckHierarchy(
	ctx context.Context,
	source *BackingCheckSource,
) error {
	consumer := source.facts.Consumer
	revision := consumer.ReadRevision
	keys := []string{
		hierarchy.EnvironmentKey(consumer.Record.EnvironmentID),
		hierarchy.EnvironmentKey(consumer.Record.BackingEnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[1] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return errs.New(errs.KindStateConflict, "access check Environment is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil {
		return err
	}
	backingEnvironment, err := hierarchy.DecodeEnvironment(read.Values[1].Value)
	if err != nil {
		return err
	}
	if environment.ID != consumer.Record.EnvironmentID ||
		backingEnvironment.ID != consumer.Record.BackingEnvironmentID ||
		backingEnvironment.ProjectID != consumer.Record.BackingProjectID {
		return errs.New(errs.KindInternal, "access check Environment ownership is invalid")
	}
	source.environment = keyvalue.Versioned[hierarchy.EnvironmentRecord]{
		Record:       environment,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: revision,
	}
	projects, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		hierarchy.ProjectKey(
			environment.ProjectID,
		), hierarchy.ProjectKey(backingEnvironment.ProjectID)}, Revision: revision})
	if err != nil {
		return err
	}
	if projects == nil || projects.ReadRevision != revision || len(projects.Values) != 2 || projects.Values[0] == nil ||
		projects.Values[1] == nil {
		if projects != nil {
			keyvalue.ClearValues(projects.Values)
		}
		return errs.New(errs.KindStateConflict, "access check Project is unavailable")
	}
	defer keyvalue.ClearValues(projects.Values)
	project, err := hierarchy.DecodeProject(projects.Values[0].Value)
	if err != nil {
		return err
	}
	backingProject, err := hierarchy.DecodeProject(projects.Values[1].Value)
	if err != nil {
		return err
	}
	if project.ID != environment.ProjectID || project.Kind != hierarchy.ProjectKindTenant ||
		backingProject.ID != backingEnvironment.ProjectID || backingProject.Kind != hierarchy.ProjectKindBacking {
		return errs.New(errs.KindStateConflict, "access check requires a tenant consumer and a shared Backing Service")
	}
	source.project = keyvalue.Versioned[hierarchy.ProjectRecord]{
		Record:       project,
		Revision:     projects.Values[0].ModRevision,
		ReadRevision: revision,
	}
	source.backingProject = keyvalue.Versioned[hierarchy.ProjectRecord]{
		Record:       backingProject,
		Revision:     projects.Values[1].ModRevision,
		ReadRevision: revision,
	}
	source.fence, err = environmentfence.LoadOrdinary(ctx, repository.store, environment.ID, revision)
	if err != nil {
		return err
	}
	source.backingFence, err = environmentfence.LoadOrdinary(ctx, repository.store, backingEnvironment.ID, revision)
	if err != nil {
		return err
	}
	source.conditions, err = environmentfence.AppendConditions(source.conditions, source.fence)
	if err != nil {
		return err
	}
	source.conditions, err = environmentfence.AppendConditions(source.conditions, source.backingFence)
	return err
}
