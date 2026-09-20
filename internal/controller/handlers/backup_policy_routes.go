package handlers

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"log/slog"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type BackupPolicyReader interface {
	GetBackupPolicy(context.Context, string) (apiTypes.BackupPolicy, error)
}

type BackupPolicyMutator interface {
	SetBackupPolicy(
		context.Context,
		string,
		apiTypes.BackupPolicyReplacementRequest,
		string,
	) (apiTypes.BackupPolicyMutationResult, error)
}

type BackupRunMutator interface {
	RunBackup(context.Context, string, string, ...int64) (idempotencyrecord.IdempotencyResponse, error)
}

type backupPolicyShowInput struct {
	ID string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type backupPolicySetInput struct {
	ID   string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.BackupPolicyReplacementRequest
}

type backupPolicyOutput struct {
	Body apiTypes.BackupPolicy
}

type backupPolicySetOutput struct {
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerBackupPolicies() {
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.policy.show",
		Method:      http.MethodGet,
		Path:        "/environments/{id}/backup-policy",
		Summary:     "Show the effective Backup Policy",
		Tags:        []string{"Backup"},
	}, s.showBackupPolicy)
	huma.Register(s.API, huma.Operation{
		OperationID:   "backup.policy.set",
		Method:        http.MethodPut,
		Path:          "/environments/{id}/backup-policy",
		Summary:       "Replace the Backup Policy",
		Tags:          []string{"Backup"},
		DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{
			"200": {
				Description: http.StatusText(http.StatusOK),
				Content: map[string]*huma.MediaType{
					"application/json": {
						Schema: &huma.Schema{Ref: "#/components/schemas/BackupPolicy"},
					},
				},
			},
		},
	}, s.setBackupPolicy)
	s.setRoutePolicy("PUT /api/v1/environments/{id}/backup-policy", routePolicy{
		body: jsonBody, validateJSON: validateBackupPolicyReplacementJSON,
	})
}

func (s *Server) showBackupPolicy(
	ctx context.Context,
	request *backupPolicyShowInput,
) (*backupPolicyOutput, error) {
	if s.backupPolicies == nil {
		return nil, errs.New(errs.KindInternal, "backup policy reader is not configured")
	}
	policy, err := s.backupPolicies.GetBackupPolicy(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &backupPolicyOutput{Body: policy}, nil
}

func (s *Server) setBackupPolicy(
	ctx context.Context,
	request *backupPolicySetInput,
) (*backupPolicySetOutput, error) {
	if s.backupPolicyMutations == nil {
		return nil, errs.New(errs.KindInternal, "backup policy mutator is not configured")
	}
	result, err := s.backupPolicyMutations.SetBackupPolicy(ctx, request.ID, request.Body, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if !json.Valid(result.Representation) {
		return nil, errs.New(errs.KindInternal, "backup policy response representation is invalid")
	}
	body := result.Representation
	return &backupPolicySetOutput{
		ContentType: "application/json",
		Body: func(ctx huma.Context) {
			ctx.SetStatus(http.StatusOK)
			if _, writeErr := ctx.BodyWriter().Write(body); writeErr != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Backup Policy response",
					slog.Any("error", writeErr),
				)
			}
		},
	}, nil
}

func validateBackupPolicyReplacementJSON(body []byte) error {
	var request apiTypes.BackupPolicyReplacementRequest
	return json.Unmarshal(body, &request)
}
