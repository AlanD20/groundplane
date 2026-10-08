package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	software "github.com/AlanD20/groundplane/internal/controller/softwarepreparation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type SoftwarePreparer interface {
	Start(context.Context, software.Request, string) (software.Accepted, error)
	Get(context.Context, string) (etcdstore.Versioned[preparationrecord.Record], error)
	List(context.Context, etcdstore.PageRequest) (etcdstore.Page[preparationrecord.Record], error)
}

type SoftwareReleaseReader interface {
	ListReleases(context.Context, preparation.Selection) ([]preparation.ReleaseChoice, error)
}

type softwarePreparationInput struct {
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.SoftwarePreparationRequest
}

type softwarePreparationOutput struct{ Body apiTypes.TaskAccepted }
type softwareShowInput struct {
	Task string `path:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}
type softwareShowOutput struct{ Body apiTypes.SoftwarePreparation }
type softwareListInput struct {
	Limit  int    `query:"limit" minimum:"1" maximum:"100" default:"25"`
	Cursor string `query:"cursor" maxLength:"4096"`
}
type softwareListOutput struct {
	Body apiTypes.SoftwarePreparationPage
}
type softwareReleaseInput struct {
	Selection string `query:"selection" required:"true" enum:"controller,agent,both"`
}
type softwareReleaseOutput struct {
	Body apiTypes.SoftwareReleaseCatalog
}

func (s *Server) registerSoftwarePreparation() {
	huma.Register(s.API, huma.Operation{OperationID: "software.prepare", Method: http.MethodPost,
		Path: software.Route, Summary: "Prepare immutable software without activating it",
		Tags: []string{"Software"}, DefaultStatus: http.StatusAccepted}, s.prepareSoftware)
	huma.Register(s.API, huma.Operation{OperationID: "software.preparations", Method: http.MethodGet,
		Path: software.Route, Summary: "List durable software preparations", Tags: []string{"Software"}}, s.listSoftwarePreparations)
	huma.Register(s.API, huma.Operation{OperationID: "software.preparation.show", Method: http.MethodGet,
		Path: software.Route + "/{task}", Summary: "Inspect preparation provenance and verified artifacts", Tags: []string{"Software"}}, s.showSoftwarePreparation)
	huma.Register(s.API, huma.Operation{OperationID: "software.releases", Method: http.MethodGet,
		Path: "/software/releases", Summary: "List recent published component releases", Tags: []string{"Software"}}, s.listSoftwareReleases)
	s.setRoutePolicy("POST /api/v1"+software.Route,
		routePolicy{body: jsonBody, validateJSON: validateSoftwarePreparationJSON})
}

func validateSoftwarePreparationJSON(raw []byte) error {
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	_, err = jcs.Decode[apiTypes.SoftwarePreparationRequest](canonical)
	return err
}

func (s *Server) prepareSoftware(
	ctx context.Context,
	input *softwarePreparationInput,
) (*softwarePreparationOutput, error) {
	if s.softwarePreparer == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software preparation is unavailable on this host")
	}
	accepted, err := s.softwarePreparer.Start(ctx, software.Request{
		Selection: preparation.Selection(
			input.Body.Selection,
		), SourceKind: preparation.SourceKind(input.Body.SourceKind),
		Ref: input.Body.Ref, Platform: preparation.HostPlatform(),
	}, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return &softwarePreparationOutput{Body: apiTypes.TaskAccepted{TaskID: accepted.TaskID}}, nil
}

func (s *Server) showSoftwarePreparation(ctx context.Context, input *softwareShowInput) (*softwareShowOutput, error) {
	if s.softwarePreparer == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software preparation is unavailable on this host")
	}
	record, err := s.softwarePreparer.Get(ctx, input.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &softwareShowOutput{Body: softwarePreparationResponse(record.Record)}, nil
}

func (s *Server) listSoftwarePreparations(ctx context.Context, input *softwareListInput) (*softwareListOutput, error) {
	if s.softwarePreparer == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software preparation is unavailable on this host")
	}
	page, err := s.softwarePreparer.List(
		ctx,
		etcdstore.PageRequest{Limit: input.Limit, Cursor: input.Cursor, Descending: true},
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.SoftwarePreparationPage{
		Items:      make([]apiTypes.SoftwarePreparation, len(page.Items)),
		NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = softwarePreparationResponse(item.Record)
	}
	return &softwareListOutput{Body: response}, nil
}

func (s *Server) listSoftwareReleases(
	ctx context.Context,
	input *softwareReleaseInput,
) (*softwareReleaseOutput, error) {
	if s.softwareReleases == nil {
		return nil, errs.New(errs.KindStrategyNotImplemented, "software release discovery is unavailable on this host")
	}
	choices, err := s.softwareReleases.ListReleases(ctx, preparation.Selection(input.Selection))
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.SoftwareReleaseCatalog{Items: make([]apiTypes.SoftwareReleaseChoice, len(choices))}
	for index, choice := range choices {
		response.Items[index] = apiTypes.SoftwareReleaseChoice{Ref: choice.Ref, Name: choice.Name,
			Selection: apiTypes.SoftwareSelection(
				choice.Selection,
			), PublishedAt: choice.PublishedAt.Format(time.RFC3339)}
	}
	return &softwareReleaseOutput{Body: response}, nil
}

func softwarePreparationResponse(record preparationrecord.Record) apiTypes.SoftwarePreparation {
	source, result := record.Source, record.Progress.Result
	response := apiTypes.SoftwarePreparation{
		TaskID:      record.TaskID,
		Selection:   apiTypes.SoftwareSelection(source.Selection),
		SourceKind:  apiTypes.SoftwareSourceKind(source.SourceKind),
		Ref:         source.OriginalRef,
		Phase:       string(record.Progress.Phase),
		CreatedAt:   record.CreatedAt.Format(time.RFC3339),
		Provenance:  []apiTypes.SoftwareProvenance{},
		Artifacts:   []apiTypes.PreparedSoftwareArtifact{},
		ErrorCode:   record.Progress.ErrorCode,
		ErrorDetail: record.Progress.ErrorDetail,
	}
	if source.SourceKind == preparation.SourceRef {
		response.Provenance = append(
			response.Provenance,
			apiTypes.SoftwareProvenance{Component: "controller", Ref: source.OriginalRef, Commit: source.ResolvedSHA},
		)
		if !source.Selection.IncludesController() {
			response.Provenance[0].Component = "agent"
		} else if source.Selection.IncludesAgent() {
			response.Provenance = append(response.Provenance, apiTypes.SoftwareProvenance{Component: "agent", Ref: source.OriginalRef, Commit: source.ResolvedSHA})
		}
	} else {
		for _, item := range []struct {
			component string
			release   *preparation.ResolvedRelease
		}{{"controller", source.ControllerRelease}, {"agent", source.AgentRelease}} {
			if item.release != nil {
				response.Provenance = append(response.Provenance, apiTypes.SoftwareProvenance{Component: item.component, Ref: item.release.OriginalRef, Commit: item.release.ResolvedSHA})
			}
		}
	}
	for _, output := range []struct {
		component string
		artifact  *preparation.Artifact
	}{{"controller", controllerArtifact(result)}, {"agent", agentArtifact(result)}} {
		if output.artifact != nil {
			response.Artifacts = append(
				response.Artifacts,
				apiTypes.PreparedSoftwareArtifact{Component: output.component,
					Reference: output.artifact.Reference, ManifestDigest: output.artifact.ManifestDigest,
					OS: output.artifact.Platform.OS, Architecture: output.artifact.Platform.Architecture},
			)
		}
	}
	return response
}

func controllerArtifact(result preparation.Result) *preparation.Artifact {
	if result.Controller == nil {
		return nil
	}
	return &result.Controller.Artifact
}

func agentArtifact(result preparation.Result) *preparation.Artifact {
	if result.Agent == nil {
		return nil
	}
	return &result.Agent.Artifact
}
