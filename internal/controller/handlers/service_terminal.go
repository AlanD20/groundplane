package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceTerminalOpenInput struct {
	ID             string                         `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string                         `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ContainerTerminalOpen `required:"true"`
}

func (s *Server) openServiceTerminal(
	ctx context.Context,
	input *serviceTerminalOpenInput,
) (*hostTerminalOpenOutput, error) {
	if s.serviceTerminals == nil {
		return nil, errs.New(errs.KindRequestUnavailable, "container terminal is unavailable")
	}
	owner, ok := ctx.Value(terminalOwnerContextKey{}).(string)
	if !ok {
		return nil, errs.New(errs.KindInternal, "terminal owner context is unavailable")
	}
	secure, ok := ctx.Value(terminalSecureContextKey{}).(bool)
	if !ok {
		return nil, errs.New(errs.KindInternal, "terminal transport context is unavailable")
	}
	session, owner, err := s.serviceTerminals.Open(ctx, owner, input.IdempotencyKey, input.ID, input.Body)
	if err != nil {
		return nil, err
	}
	return &hostTerminalOpenOutput{SetCookie: (&http.Cookie{
		Name: terminalOwnerCookie, Value: owner, Path: "/api/v1", HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode,
	}).String(), CacheControl: "no-store", Body: session}, nil
}
