package handlers

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingcatalog"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
	"net/http"
)

type backingAdapterCatalogOutput struct {
	Body apiTypes.BackingAdapterCatalog
}

func (server *Server) registerBackingAdapterCatalog() {
	huma.Register(server.API, huma.Operation{
		OperationID: "backing-service.adapters", Method: http.MethodGet,
		Path: "/backing-service-adapters", Summary: "List supported backing adapter families and versions",
		Tags: []string{"Backing service"},
	}, func(_ context.Context, _ *struct{}) (*backingAdapterCatalogOutput, error) {
		catalog := apiTypes.BackingAdapterCatalog{Items: []apiTypes.BackingAdapter{}}
		for _, adapter := range adapters.All() {
			item := apiTypes.BackingAdapter{Key: adapter.Key(), Label: adapter.Label(), Custom: adapter.Custom(),
				RequiresAuthenticationMode: adapter.SupportsAuthenticationModes(), Versions: []apiTypes.BackingAdapterVersion{}}
			for _, family := range backingcatalog.Families() {
				if family.Key != adapter.Key() {
					continue
				}
				for _, version := range family.Versions {
					item.Versions = append(
						item.Versions,
						apiTypes.BackingAdapterVersion{Version: version.Number, ImagePattern: version.ImagePattern},
					)
				}
			}
			catalog.Items = append(catalog.Items, item)
		}
		return &backingAdapterCatalogOutput{Body: catalog}, nil
	})
}
