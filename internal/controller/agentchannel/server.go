package agentchannel

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const tokenSize = 32

// Token is the decoded, fixed-size Agent credential presented to an Authenticator.
type Token [tokenSize]byte

// Authorization is the non-secret result of successful Agent authentication.
type Authorization struct {
	Generation uint64
	Config     *agentpb.AgentConfig
}

// Authenticator authenticates one Agent generation without retaining or
// returning its credential.
type Authenticator interface {
	Authenticate(context.Context, string, Token) (Authorization, error)
}

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth     Authenticator
	sessions *Registry
	now      func() time.Time
}

// New returns an AgentChannel server backed by the supplied authenticator and
// session registry. A nil registry creates an isolated registry.
func New(auth Authenticator, sessions *Registry) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{auth: auth, sessions: sessions, now: time.Now}
}

// Connect authenticates the first message, publishes the authorized config,
// and then owns the live session until disconnect or fencing.
func (s *Server) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	if s.auth == nil {
		return status.Error(codes.Internal, "agent channel is not configured")
	}

	first, err := stream.Recv()
	if err != nil {
		return unauthenticated()
	}
	authenticate, token, err := validateAuthenticate(first)
	if err != nil {
		return unauthenticated()
	}
	defer clear(token[:])
	defer clear(authenticate.Token)

	authorization, authErr := s.auth.Authenticate(stream.Context(), authenticate.AgentId, token)
	if authErr != nil {
		return unauthenticated()
	}
	if authorization.Config == nil {
		return status.Error(codes.Internal, "agent configuration is not available")
	}
	if authorization.Generation == 0 {
		return status.Error(codes.Internal, "agent authorization generation is not available")
	}

	session, err := s.sessions.Open(stream.Context(), authenticate.AgentId, authorization.Generation)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "agent session is not current")
	}
	defer session.Close()

	config := proto.Clone(authorization.Config).(*agentpb.AgentConfig)
	if err := stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ConfigUpdate{
			ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: config},
		},
	}); err != nil {
		return err
	}

	type receiveResult struct {
		message *agentpb.AgentMessage
		err     error
	}
	received := make(chan receiveResult, 1)
	go func() {
		for {
			message, recvErr := stream.Recv()
			select {
			case received <- receiveResult{message: message, err: recvErr}:
			case <-session.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-session.Done():
			return nil
		case result := <-received:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) || errors.Is(result.err, context.Canceled) {
					return nil
				}
				return result.err
			}
			if result.message == nil {
				return status.Error(codes.InvalidArgument, "agent message is required")
			}
			if result.message.GetAuthenticate() != nil {
				return unauthenticated()
			}

			ready := result.message.GetReady()
			if ready == nil {
				return status.Error(codes.Unimplemented, "authenticated agent message is not implemented")
			}
			if err := session.RecordReady(s.now(), ready.Capacity); err != nil {
				if ready.Capacity < 0 {
					return status.Error(codes.InvalidArgument, "agent Ready capacity must be non-negative")
				}
				return status.Error(codes.FailedPrecondition, "agent session is not current")
			}
		}
	}
}

func validateAuthenticate(message *agentpb.AgentMessage) (*agentpb.Authenticate, Token, error) {
	if message == nil || message.GetAuthenticate() == nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "Authenticate must be the first Agent message")
	}
	authenticate := message.GetAuthenticate()
	if err := ids.Validate(ids.KindAgent, authenticate.AgentId); err != nil {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	if len(authenticate.Token) != tokenSize {
		return nil, Token{}, errs.New(errs.KindValidationFailed, "agent token has an invalid length")
	}

	var token Token
	copy(token[:], authenticate.Token)
	return authenticate, token, nil
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "agent authentication failed")
}
