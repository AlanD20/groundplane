package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AttachCreateScope struct {
	Tenant             etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project            etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment        etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	DesiredHead        etcdstore.Versioned[EnvironmentBlueprintHead]
	ComposeProjection  etcdstore.Versioned[EnvironmentComposeProjection]
	Services           []etcdstore.Versioned[ServiceRecord]
	BackingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService     etcdstore.Versioned[ServiceRecord]
	CredentialOwner    *etcdstore.Versioned[attachrecord.Record]
	Grants             []etcdstore.Versioned[attachrecord.Record]
}

type AttachRepository struct {
	store etcdstore.Store
}

type attachRemovalStore interface {
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewAttachRepository(store etcdstore.Store) (*AttachRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach repository store is required")
	}
	return &AttachRepository{store: store}, nil
}
