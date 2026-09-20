package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
)

func taskStoreStatus(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	slog.Error("controller: Agent task operation failed", slog.Any("error", err))
	kind, ok := errs.KindOf(err)
	if !ok {
		return status.Error(codes.Internal, "agent task operation failed")
	}
	switch kind {
	case errs.KindValidationFailed:
		return status.Error(codes.InvalidArgument, "agent task message is invalid")
	case errs.KindTaskNotFound:
		return status.Error(codes.NotFound, "agent task was not found")
	case errs.KindStateConflict:
		return status.Error(codes.FailedPrecondition, "agent task state does not match")
	case errs.KindStorageUnavailable:
		return status.Error(codes.Unavailable, "agent task storage is unavailable")
	default:
		return status.Error(codes.Internal, "agent task operation failed")
	}
}

func validateAuthenticate(message *agentpb.AgentMessage) (*agentpb.Authenticate, Token, error) {
	if message == nil || message.GetAuthenticate() == nil {
		return nil, Token{}, errs.New(
			errs.KindValidationFailed,
			"Authenticate must be the first Agent message",
		)
	}
	authenticate := message.GetAuthenticate()
	if err := ids.Validate(ids.KindAgent, authenticate.AgentId); err != nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	if len(authenticate.Token) != tokenSize {
		return nil, Token{}, errs.New(
			errs.KindValidationFailed,
			"agent token has an invalid length",
		)
	}

	var token Token
	copy(token[:], authenticate.Token)
	return authenticate, token, nil
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "agent authentication failed")
}
