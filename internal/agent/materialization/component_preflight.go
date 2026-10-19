package materialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ComponentFileValidator runs only the compiled recipe selected by the captured
// native action. Failure precedes delivery to the physical file writer.
type ComponentFileValidator interface {
	ValidateComponentFile(context.Context, componentsdk.ActionEnvelope, string, []byte) error
}

// preflightComponentFile consumes the incoming source on success and failure;
// successful validation transfers a fresh clearing reader to the caller.
func (runtime *Runtime) preflightComponentFile(ctx context.Context, action *agentpb.ComponentApply,
	payload Payload,
) (Payload, error) {
	header := payload.Header
	digest := header.Digest()
	if runtime.componentFiles == nil || action == nil || action.ManagedConfigContent ||
		action.Generation != header.Generation() ||
		header.OutputKind() != entrymaterialization.OutputPlainFile || header.ServiceID() != "" ||
		header.ServiceName() != "" || header.UID() != 0 || header.GID() != 0 ||
		header.Mode() != entrymaterialization.ModeReadOnly || !bytes.Equal(action.ArtifactDigest, digest[:]) {
		return Payload{}, CloseSourceWithError(payload.Source, "agent: Component file preflight authority is invalid")
	}
	envelope, err := componentaction.DecodeComponentAction(action)
	if err != nil {
		return Payload{}, errs.Wrap(errs.KindValidationFailed, errors.Join(err, payload.Source.Close()))
	}
	content, readErr := io.ReadAll(io.LimitReader(payload.Source, int64(entrymaterialization.MaximumContentBytes)+1))
	if err := errors.Join(readErr, payload.Source.Close()); err != nil {
		clear(content)
		return Payload{}, errs.Wrap(errs.KindInternal, err)
	}
	if uint64(len(content)) != header.Length() || sha256.Sum256(content) != digest {
		clear(content)
		return Payload{}, errs.New(
			errs.KindValidationFailed,
			"agent: Component preflight content differs from its captured file",
		)
	}
	if err := runtime.componentFiles.ValidateComponentFile(ctx, envelope, header.Destination(), content); err != nil {
		clear(content)
		return Payload{}, err
	}
	if ctx.Err() != nil || sha256.Sum256(content) != digest {
		clear(content)
		return Payload{}, errs.New(
			errs.KindValidationFailed,
			"agent: Component preflight content or execution authority changed",
		)
	}
	payload.Source = OwnContent(content)
	return payload, nil
}
