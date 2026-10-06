package serviceremovalreferences

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func Validate(ctx context.Context, store referenceStore, current etcdstore.Versioned[servicerecord.ServiceRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) (Guards, error) {
	if err := servicerecord.ValidateServiceVersion(current); err != nil {
		return Guards{}, err
	}
	if projection.Revision <= 0 || projection.ReadRevision < projection.Revision || projection.Record.EnvironmentID != current.Record.EnvironmentID || projectionrecord.ValidateEnvironmentComposeProjection(projection.Record) != nil {
		return Guards{}, errs.New(errs.KindValidationFailed, "Service removal projection is invalid")
	}
	if current.Record.BackingNetworkID != "" || current.Record.Desired.Adapter != "" {
		return Guards{}, errs.New(errs.KindResourceInUse, "Backing Services are removed through their backing lifecycle")
	}
	for _, service := range projection.Record.DesiredServices {
		if service.Desired.ID == current.Record.Desired.ID {
			continue
		}
		if _, referenced := service.Desired.DependsOn[current.Record.Desired.Name]; referenced {
			return Guards{}, errs.New(errs.KindResourceInUse, "Service is referenced by another desired resource")
		}
	}
	for _, component := range projection.Record.Components {
		if slices.Contains(component.Runtime.GeneratedServices, current.Record.Desired.ID) {
			return Guards{}, errs.New(errs.KindResourceInUse, "Component-generated Services are removed through their Component")
		}
	}
	for _, prefix := range []string{"/v1/indexes/attaches/by-service/service/" + current.Record.Desired.ID + "/", "/v1/indexes/attaches/by-backing-service/service/" + current.Record.Desired.ID + "/"} {
		page, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: projection.ReadRevision})
		if err != nil {
			return Guards{}, err
		}
		if page == nil {
			return Guards{}, errs.New(errs.KindInternal, "Service removal reference read is empty")
		}
		if len(page.Values) != 0 {
			return Guards{}, errs.New(errs.KindResourceInUse, "Service is referenced by an Attach")
		}
	}
	selected, found, err := blueprints.ReadCurrentProjection(ctx, store, current.Record.EnvironmentID, projection.ReadRevision)
	if err != nil {
		return Guards{}, err
	}
	if found {
		for _, route := range selected.Record.DesiredRoutes {
			if route.Desired.TargetServiceID == current.Record.Desired.ID {
				return Guards{}, errs.New(errs.KindResourceInUse, "Service is referenced by another desired resource")
			}
		}
	}
	guards, err := Prepare(ctx, store, current.Record, projection)
	if err != nil {
		return Guards{}, err
	}
	_, err = scriptsourceevidence.PrepareServiceScriptAbsence(ctx, store, current.Record.Desired.ID, projection.ReadRevision)
	if err != nil {
		return Guards{}, err
	}
	return guards, nil
}
