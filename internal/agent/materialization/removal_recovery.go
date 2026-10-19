package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) StopRemovalRecoveryFiles(
	ctx context.Context,
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
	manifest *agentpb.ServiceRemovalConfigurationManifest,
) error {
	if runtime == nil || runtime.helper == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Removal recovery file helper is unavailable")
	}
	headers, err := executionplan.RemovalConfigurationHeaders(request, manifest)
	if err != nil {
		return err
	}
	for _, header := range headers {
		if err := runtime.helper.StopCaptured(ctx, materializerrunner.OwnershipFromHeader(header)); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *Runtime) RunRemovalRecoveryFile(
	ctx context.Context,
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
	manifest *agentpb.ServiceRemovalConfigurationManifest,
	header entrymaterialization.Header,
	source io.ReadCloser,
	verifyOnly bool,
) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "Removal recovery file source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil || request.GetInspectOnly() && !verifyOnly {
		return errs.New(errs.KindStateConflict, "Removal file write has no recovery grant")
	}
	root, err := executionplan.RemovalConfigurationFileRoot(request, manifest, header)
	if err != nil {
		return err
	}
	return runtime.runFile(ctx, root, Payload{Header: header, Source: source}, verifyOnly)
}
