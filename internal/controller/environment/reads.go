// Package environment owns Controller read use cases and projections for
// Environments. Persistence and HTTP representations are adapted at its edges.
package environment

import (
	"context"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	environmentetcd "github.com/AlanD20/groundplane/internal/infra/etcd/environment"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProvisioningState describes the current lifecycle state of an Environment.
type ProvisioningState string

const (
	Provisioning ProvisioningState = "provisioning"
	Ready        ProvisioningState = "ready"
	Failed       ProvisioningState = "failed"
)

// Environment is the Controller read projection for a managed environment.
type Environment struct {
	ID                string
	ProjectID         string
	Name              string
	NetworkPool       string
	VolumeDir         string
	ProvisioningState ProvisioningState
	CreateTaskID      *string
	DeletionTaskID    *string
	NetworkCapacity   NetworkCapacity
}

// NetworkCapacity is the deterministic address-space projection for an
// Environment pool and the Zone subnets reserved inside it.
type NetworkCapacity struct {
	TotalAddresses     int64
	AllocatedAddresses int64
	AvailableAddresses int64
	ZoneCount          int64
}

// ProjectNetworkCapacity derives capacity without assigning host-address
// semantics to either the parent pool or its child subnets.
func ProjectNetworkCapacity(networkPool string, zoneSubnets []string) (NetworkCapacity, error) {
	pool, err := netip.ParsePrefix(networkPool)
	if err != nil || !pool.Addr().Is4() || pool.Masked().String() != networkPool {
		return NetworkCapacity{}, errs.New(errs.KindInternal, "stored Environment network pool is invalid")
	}
	total := int64(1) << uint64(32-pool.Bits())
	var allocated int64
	reserved := make([]netip.Prefix, 0, len(zoneSubnets))
	for _, value := range zoneSubnets {
		subnet, parseErr := netip.ParsePrefix(value)
		if parseErr != nil || !subnet.Addr().Is4() || subnet.Masked().String() != value ||
			!pool.Contains(subnet.Addr()) || subnet.Bits() < pool.Bits() {
			return NetworkCapacity{}, errs.New(errs.KindInternal, "stored Zone subnet reservation is invalid")
		}
		for _, existing := range reserved {
			if existing.Overlaps(subnet) {
				return NetworkCapacity{}, errs.New(
					errs.KindInternal,
					"stored Zone subnet reservations overlap",
				)
			}
		}
		reserved = append(reserved, subnet)
		allocated += int64(1) << uint64(32-subnet.Bits())
		if allocated > total {
			return NetworkCapacity{}, errs.New(
				errs.KindInternal,
				"stored Zone subnet reservations exceed their Environment pool",
			)
		}
	}
	return NetworkCapacity{
		TotalAddresses: total, AllocatedAddresses: allocated,
		AvailableAddresses: total - allocated, ZoneCount: int64(len(zoneSubnets)),
	}, nil
}

// PageRequest carries the limit and opaque cursor for environment listing.
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page contains an environment listing and its opaque continuation cursor.
type Page struct {
	Items      []Environment
	NextCursor string
}

// Repository is the persistence side-effect seam used by Environment reads.
// Its concrete values keep persistence DTOs outside the capability boundary.
type Repository interface {
	// GetEnvironment reads one environment by its stable ID.
	GetEnvironment(context.Context, string) (Environment, error)
	// ListEnvironments reads a page of environments for a project.
	ListEnvironments(context.Context, string, PageRequest) (Page, error)
}

// Reader executes validated environment read use cases against a Repository.
type Reader struct {
	repository Repository
}

// GetInput identifies an environment by its stable ID.
type GetInput struct {
	ID string
}

// ListInput scopes an environment listing to a project and carries pagination.
type ListInput struct {
	ProjectID string
	Limit     int
	Cursor    string
}

// NewReader constructs a Reader backed by repository.
func NewReader(repository Repository) *Reader {
	return &Reader{repository: repository}
}

// NewEtcdReader constructs a Reader from its concrete etcd adapter.
func NewEtcdReader(repository *environmentetcd.Repository) *Reader {
	return NewReader(etcdReads{repository: repository})
}

type etcdReads struct{ repository *environmentetcd.Repository }

func (reads etcdReads) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	record, err := reads.repository.Get(ctx, id)
	if err != nil {
		return Environment{}, err
	}
	return environmentFromEtcd(record)
}

func (reads etcdReads) ListEnvironments(ctx context.Context, projectID string, request PageRequest) (Page, error) {
	stored, err := reads.repository.List(ctx, projectID, environmentetcd.PageRequest{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil {
		return Page{}, err
	}
	items := make([]Environment, len(stored.Items))
	for index, record := range stored.Items {
		items[index], err = environmentFromEtcd(record)
		if err != nil {
			return Page{}, err
		}
	}
	return Page{Items: items, NextCursor: stored.NextCursor}, nil
}

func environmentFromEtcd(record environmentetcd.Record) (Environment, error) {
	capacity, err := ProjectNetworkCapacity(record.NetworkPool, record.ZoneSubnets)
	if err != nil {
		return Environment{}, err
	}
	state := ProvisioningState(record.ProvisioningState)
	if state != Provisioning && state != Ready && state != Failed {
		return Environment{}, errs.New(errs.KindInternal, "stored Environment provisioning state is invalid")
	}
	return Environment{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name, NetworkPool: record.NetworkPool,
		VolumeDir: record.VolumeDir, ProvisioningState: state, CreateTaskID: record.CreateTaskID,
		DeletionTaskID:  record.DeletionTaskID,
		NetworkCapacity: capacity,
	}, nil
}

// Get loads one environment after validating its stable ID.
func (reader *Reader) Get(ctx context.Context, input GetInput) (Environment, error) {
	if reader == nil || reader.repository == nil {
		return Environment{}, errs.New(errs.KindInternal, "environment reader is not configured")
	}
	if err := ids.Validate(ids.KindEnvironment, input.ID); err != nil {
		return Environment{}, errs.New(
			errs.KindValidationFailed,
			"environment get requires a stable environment id",
		)
	}
	return reader.repository.GetEnvironment(ctx, input.ID)
}

// List loads a page of environments after validating its project and pagination inputs.
func (reader *Reader) List(ctx context.Context, input ListInput) (Page, error) {
	if reader == nil || reader.repository == nil {
		return Page{}, errs.New(errs.KindInternal, "environment reader is not configured")
	}
	request, err := listRequest(input)
	if err != nil {
		return Page{}, err
	}
	return reader.repository.ListEnvironments(ctx, input.ProjectID, request)
}

func listRequest(input ListInput) (PageRequest, error) {
	if err := ids.Validate(ids.KindProject, input.ProjectID); err != nil {
		return PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"environment list requires a stable project id",
		)
	}
	if input.Limit < 0 {
		return PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"environment list limit must be a positive integer",
		)
	}
	return PageRequest{Cursor: input.Cursor, Limit: input.Limit}, nil
}
