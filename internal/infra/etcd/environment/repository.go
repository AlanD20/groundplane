// Package environment adapts durable etcd Environment records without
// exposing persistence DTOs to Controller capabilities.
package environment

import (
	"context"

	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ProvisioningState string

const (
	Provisioning ProvisioningState = "provisioning"
	Ready        ProvisioningState = "ready"
	Failed       ProvisioningState = "failed"
)

// Record is the persistence-neutral result of an Environment adapter read.
type Record struct {
	ID, ProjectID, Name, NetworkPool, VolumeDir string
	ProvisioningState                           ProvisioningState
	CreateTaskID                                *string
	DeletionTaskID                              *string
	ZoneSubnets                                 []string
}

type PageRequest struct {
	Limit  int
	Cursor string
}
type Page struct {
	Items      []Record
	NextCursor string
}

type hierarchyRecords interface {
	GetEnvironment(context.Context, string) (etcdinfra.Versioned[etcdinfra.EnvironmentRecord], error)
	ListEnvironments(
		context.Context,
		string,
		etcdinfra.PageRequest,
	) (etcdinfra.Page[etcdinfra.EnvironmentRecord], error)
}
type zoneReservations interface {
	ListZoneSubnetReservationsAtRevision(context.Context, string, int64) ([]string, error)
}

// Repository is the concrete etcd adapter for Environment reads.
type Repository struct {
	hierarchy hierarchyRecords
	zones     zoneReservations
}

func NewRepository(hierarchy hierarchyRecords, zones zoneReservations) *Repository {
	return &Repository{hierarchy: hierarchy, zones: zones}
}

func (repository *Repository) Get(ctx context.Context, id string) (Record, error) {
	if repository == nil || repository.hierarchy == nil || repository.zones == nil {
		return Record{}, errs.New(errs.KindInternal, "Environment repository is not configured")
	}
	stored, err := repository.hierarchy.GetEnvironment(ctx, id)
	if err != nil {
		return Record{}, err
	}
	return repository.project(ctx, stored.Record, stored.ReadRevision)
}

func (repository *Repository) List(ctx context.Context, projectID string, request PageRequest) (Page, error) {
	if repository == nil || repository.hierarchy == nil || repository.zones == nil {
		return Page{}, errs.New(errs.KindInternal, "Environment repository is not configured")
	}
	stored, err := repository.hierarchy.ListEnvironments(
		ctx,
		projectID,
		etcdinfra.PageRequest{Limit: request.Limit, Cursor: request.Cursor},
	)
	if err != nil {
		return Page{}, err
	}
	items := make([]Record, len(stored.Items))
	for index, item := range stored.Items {
		items[index], err = repository.project(ctx, item.Record, stored.Revision)
		if err != nil {
			return Page{}, err
		}
	}
	return Page{Items: items, NextCursor: stored.NextCursor}, nil
}

func (repository *Repository) project(
	ctx context.Context,
	record etcdinfra.EnvironmentRecord,
	revision int64,
) (Record, error) {
	state, err := provisioningState(record.ProvisioningState)
	if err != nil {
		return Record{}, err
	}
	subnets, err := repository.zones.ListZoneSubnetReservationsAtRevision(ctx, record.ID, revision)
	if err != nil {
		return Record{}, err
	}
	var createTaskID *string
	if state != Ready {
		value := record.CreateTaskID
		createTaskID = &value
	}
	var deletionTaskID *string
	if record.DeletionTaskID != "" {
		value := record.DeletionTaskID
		deletionTaskID = &value
	}
	return Record{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name, NetworkPool: record.NetworkPool,
		VolumeDir: record.VolumeDir, ProvisioningState: state, CreateTaskID: createTaskID, ZoneSubnets: subnets,
		DeletionTaskID: deletionTaskID,
	}, nil
}

func provisioningState(state etcdinfra.EnvironmentProvisioningState) (ProvisioningState, error) {
	switch state {
	case etcdinfra.EnvironmentProvisioningProvisioning:
		return Provisioning, nil
	case etcdinfra.EnvironmentProvisioningReady:
		return Ready, nil
	case etcdinfra.EnvironmentProvisioningFailed:
		return Failed, nil
	default:
		return "", errs.New(errs.KindInternal, "stored Environment provisioning state is invalid")
	}
}
