package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Until protected removal units exist, reject omissions before claiming a
// revision. Private staging must not be the first point that discovers them.
func (service *Service) rejectUnsupportedBlueprintOmissions(
	ctx context.Context,
	environmentID string,
	parsed blueprintparser.Result,
	currentAttaches []etcdstore.Versioned[attachrecord.Record],
) error {
	currentZones, err := service.listBlueprintZones(ctx, environmentID)
	if err != nil {
		return err
	}
	for _, zone := range currentZones {
		if _, present := parsed.Project.Networks[zone.Record.Desired.Name]; !present {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Zone %s; protected removal is not yet available through Apply",
				zone.Record.Desired.Name,
			)
		}
	}
	for _, attach := range currentAttaches {
		if _, present := parsed.Extensions.Attachments[attach.Record.Name]; !present {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Attach %s; protected removal is not yet available through Apply",
				attach.Record.Name,
			)
		}
	}
	currentRoutes, err := service.listBlueprintRoutes(ctx, environmentID)
	if err != nil {
		return err
	}
	authoredRoutes := make(map[string]struct{}, len(parsed.Extensions.Routes))
	for _, route := range parsed.Extensions.Routes {
		path := route.Path
		if path == "" {
			path = "/"
		}
		authoredRoutes[route.Hostname+"\x00"+path] = struct{}{}
	}
	for _, route := range currentRoutes {
		path := route.Record.Desired.Path
		if path == "" {
			path = "/"
		}
		if _, present := authoredRoutes[route.Record.Desired.Host+"\x00"+path]; !present {
			return errs.New(
				errs.KindResourceInUse,
				"Blueprint omits an existing Route; protected removal is not yet available through Apply",
			)
		}
	}
	return nil
}
