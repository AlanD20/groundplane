package handlers

import (
	"context"
	"net/http"

	activation "github.com/AlanD20/groundplane/internal/controller/softwareactivation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type SoftwareActivator interface {
	Start(context.Context, string, string) (activation.Accepted, error)
	Get(context.Context, string) (etcdstore.Versioned[activationrecord.Record], error)
}

type softwareApplyInput struct {
	Task string `path:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}
type softwareActivationOutput struct{ Body apiTypes.SoftwareActivation }

func (s *Server) registerSoftwareActivation() {
	huma.Register(s.API, huma.Operation{OperationID: "software.apply", Method: http.MethodPost,
		Path: "/software/preparations/{task}/apply", Summary: "Apply a verified software preparation",
		Tags: []string{"Software"}, DefaultStatus: http.StatusAccepted}, s.applySoftware)
	huma.Register(s.API, huma.Operation{OperationID: "software.activation.show", Method: http.MethodGet,
		Path: "/software/activations/{task}", Summary: "Inspect component activation outcomes",
		Tags: []string{"Software"}}, s.showSoftwareActivation)
}

func (s *Server) applySoftware(ctx context.Context, input *softwareApplyInput) (*softwarePreparationOutput, error) {
	if s.softwareActivator == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software activation is unavailable on this host")
	}
	accepted, err := s.softwareActivator.Start(ctx, input.Task, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &softwarePreparationOutput{Body: apiTypes.TaskAccepted{TaskID: accepted.TaskID}}, nil
}

func (s *Server) showSoftwareActivation(
	ctx context.Context,
	input *softwareShowInput,
) (*softwareActivationOutput, error) {
	if s.softwareActivator == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software activation is unavailable on this host")
	}
	selected, err := s.softwareActivator.Get(ctx, input.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	record, progress := selected.Record, selected.Record.Progress
	return &softwareActivationOutput{Body: apiTypes.SoftwareActivation{
		TaskID: record.TaskID, PreparationTaskID: record.Input.Preparation.TaskID,
		Selection: apiTypes.SoftwareSelection(record.Input.Preparation.Source.Selection), Phase: string(progress.Phase),
		ControllerTaskID: progress.ControllerTaskID, AgentTaskID: progress.AgentTaskID,
		ControllerApplied: progress.ControllerApplied, AgentApplied: progress.AgentApplied,
		ErrorCode: progress.ErrorCode, ErrorDetail: progress.ErrorDetail,
	}}, nil
}
