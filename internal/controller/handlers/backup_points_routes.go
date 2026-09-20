package handlers

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RecoveryPointReader interface {
	ListRecoveryPoints(context.Context, string, string) (etcdstore.Page[etcd.BackupRecoveryPointRecord], error)
}

type recoveryPointListInput struct {
	Environment string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Cursor      string `query:"cursor" required:"false" maxLength:"2048"`
}

type recoveryPointPageOutput struct {
	Body apiTypes.RecoveryPointPage
}

func (s *Server) registerRecoveryPoints() {
	pointSchema := openAPISchema[apiTypes.RecoveryPoint](s.API.OpenAPI().Components.Schemas, "RecoveryPoint")
	pageSchema := openAPISchema[apiTypes.RecoveryPointPage](s.API.OpenAPI().Components.Schemas, "RecoveryPointPage")
	if pageSchema == nil || pointSchema == nil {
		return
	}
	pageDefinition := s.API.OpenAPI().Components.Schemas.Map()["RecoveryPointPage"]
	if pageDefinition == nil {
		return
	}
	// This exact public page carries fixed-revision evidence only inside its
	// authenticated cursor, so suppress Huma's optional schema-link field.
	pageDefinition.Properties["$schema"] = &huma.Schema{Type: huma.TypeString}
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.points.list",
		Method:      http.MethodGet,
		Path:        "/environments/{id}/recovery-points",
		Summary:     "List verified Recovery Points",
		Tags:        []string{"Backup"},
		Responses: map[string]*huma.Response{
			"200": {
				Description: http.StatusText(http.StatusOK),
				Content: map[string]*huma.MediaType{
					"application/json": {Schema: pageSchema},
				},
			},
		},
	}, s.listRecoveryPoints)
	delete(pageDefinition.Properties, "$schema")
}

func (s *Server) listRecoveryPoints(
	ctx context.Context,
	request *recoveryPointListInput,
) (*recoveryPointPageOutput, error) {
	if s.recoveryPoints == nil {
		return nil, errs.New(errs.KindInternal, "recovery point reader is not configured")
	}
	page, err := s.recoveryPoints.ListRecoveryPoints(ctx, request.Environment, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.RecoveryPointPage{
		Items: make([]apiTypes.RecoveryPoint, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = recoveryPointResponse(item.Record)
	}
	return &recoveryPointPageOutput{Body: response}, nil
}

func recoveryPointResponse(record etcd.BackupRecoveryPointRecord) apiTypes.RecoveryPoint {
	return apiTypes.RecoveryPoint{
		ID:         record.ID,
		SourceID:   record.SourceID,
		SourceKind: apiTypes.BackupSourceKind(record.SourceKind),
		TargetID:   record.TargetID,
		CreatedAt:  record.CreatedAt.UTC().Format(time.RFC3339),
		SizeBytes:  record.SizeBytes,
		Encrypted:  record.Encryption == etcd.BackupRuntimeEncryptionAge,
		KeyEra:     record.KeyEra,
		Status:     apiTypes.RecoveryPointVerified,
	}
}
