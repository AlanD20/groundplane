package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/hostterminal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/coder/websocket"
	"github.com/danielgtaylor/huma/v2"
	"github.com/oklog/ulid/v2"
)

const terminalOwnerCookie = "gp_terminal_owner"

type terminalOwnerContextKey struct{}

type terminalSecureContextKey struct{}

type hostTerminalOpenInput struct {
	IdempotencyKey string                    `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.HostTerminalOpen `required:"true"`
}

type hostTerminalOpenOutput struct {
	SetCookie    string `header:"Set-Cookie"`
	CacheControl string `header:"Cache-Control"`
	Body         apiTypes.HostTerminalSession
}

type terminalClientMessage struct {
	Type    string `json:"type"`
	Data    string `json:"data,omitempty"`
	Columns int    `json:"columns,omitempty"`
	Rows    int    `json:"rows,omitempty"`
}

type terminalServerMessage struct {
	Type    string                        `json:"type"`
	Session *apiTypes.HostTerminalSession `json:"session,omitempty"`
}

func (s *Server) registerHostTerminal() {
	huma.Register(s.API, huma.Operation{
		OperationID: "host.terminal", Method: http.MethodPost, Path: "/host/terminal",
		Summary: "Open or resume the owning browser session's interactive root host terminal",
		Tags:    []string{"Host"}, DefaultStatus: http.StatusCreated,
		Middlewares: huma.Middlewares{s.ownHostTerminalRequest},
	}, s.openHostTerminal)
	s.setRoutePolicy("POST /api/v1/host/terminal", routePolicy{body: jsonBody})
	s.upgradeRoute("GET /api/v1/host/terminal/{id}/stream", s.streamHostTerminal)
}

func (s *Server) ownHostTerminalRequest(ctx huma.Context, next func(huma.Context)) {
	if !sameOriginRequest(ctx.Header("Origin"), ctx.Header("Sec-Fetch-Site"), ctx.Host(), ctx.TLS() != nil) {
		_ = huma.WriteErr(s.API, ctx, http.StatusForbidden, "host terminal requests must be same-origin")
		return
	}
	owner := terminalOwner(ctx.Header("Cookie"))
	ctx = huma.WithValue(ctx, terminalOwnerContextKey{}, owner)
	next(huma.WithValue(ctx, terminalSecureContextKey{}, ctx.TLS() != nil))
}

func (s *Server) openHostTerminal(
	ctx context.Context,
	input *hostTerminalOpenInput,
) (*hostTerminalOpenOutput, error) {
	if s.hostTerminals == nil {
		return nil, errs.New(errs.KindRequestUnavailable, "host terminal service is unavailable")
	}
	owner, ok := ctx.Value(terminalOwnerContextKey{}).(string)
	if !ok {
		return nil, errs.New(errs.KindInternal, "host terminal owner context is unavailable")
	}
	secure, ok := ctx.Value(terminalSecureContextKey{}).(bool)
	if !ok {
		return nil, errs.New(errs.KindInternal, "host terminal transport context is unavailable")
	}
	session, owner, err := s.hostTerminals.Open(
		ctx, owner, input.IdempotencyKey,
		uint16(input.Body.Columns), uint16(input.Body.Rows),
	)
	if err != nil {
		return nil, err
	}
	return &hostTerminalOpenOutput{
		SetCookie: (&http.Cookie{
			Name: terminalOwnerCookie, Value: owner, Path: "/api/v1/host/terminal",
			HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode,
		}).String(),
		CacheControl: "no-store",
		Body:         session,
	}, nil
}

func (s *Server) streamHostTerminal(w http.ResponseWriter, r *http.Request) {
	if s.hostTerminals == nil {
		s.writeProblem(w, errs.New(errs.KindRequestUnavailable, "host terminal service is unavailable"))
		return
	}
	if !sameOriginHTTP(r) {
		s.writeRequestProblem(w, http.StatusForbidden, "host terminal requests must be same-origin")
		return
	}
	owner := terminalOwner(r.Header.Get("Cookie"))
	id := r.PathValue("id")
	if ids.Validate(ids.KindTerminal, id) != nil {
		s.writeProblem(w, errs.New(errs.KindRequestNotFound, "host terminal session was not found"))
		return
	}
	after, err := terminalAfter(r.URL.Query())
	if err != nil {
		s.writeProblem(w, errs.New(errs.KindMalformedRequest, "host terminal cursor is invalid"))
		return
	}
	attachment, err := s.hostTerminals.Attach(owner, id, after)
	if err != nil {
		if errs.IsNotFound(err) {
			closeUnavailableHostTerminal(w, r)
			return
		}
		var terminalErr *errs.Error
		if !errors.As(err, &terminalErr) {
			terminalErr = errs.Wrap(errs.KindInternal, err)
		}
		s.writeProblem(w, terminalErr)
		return
	}
	defer attachment.Detach()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(40 << 10)

	streamCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	readResult := make(chan error, 1)
	go func() { readResult <- s.readHostTerminalClient(streamCtx, conn, owner, id) }()

	if attachment.Truncated {
		writeCtx, stop := context.WithTimeout(streamCtx, 5*time.Second)
		err := writeTerminalJSON(writeCtx, conn, terminalServerMessage{Type: "output_truncated"})
		stop()
		if err != nil {
			return
		}
	}
	for _, event := range attachment.Replay {
		writeCtx, stop := context.WithTimeout(streamCtx, 5*time.Second)
		err := writeTerminalEvent(writeCtx, conn, event)
		stop()
		if err != nil {
			return
		}
	}
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-streamCtx.Done():
			return
		case <-readResult:
			return
		case event, ok := <-attachment.Events:
			if !ok {
				return
			}
			writeCtx, stop := context.WithTimeout(streamCtx, 5*time.Second)
			err := writeTerminalEvent(writeCtx, conn, event)
			stop()
			if err != nil || event.Kind == hostterminal.EventTerminated {
				return
			}
		case <-ping.C:
			pingCtx, stop := context.WithTimeout(streamCtx, 5*time.Second)
			err := conn.Ping(pingCtx)
			stop()
			if err != nil {
				return
			}
		}
	}
}

// A browser cannot read an HTTP failure from a WebSocket handshake. Send a
// permanent close on the continuation itself so reconnect cannot loop after
// restart, expiration or loss of ownership. This never opens a shell.
func closeUnavailableHostTerminal(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	// The close handshake is bounded by the WebSocket transport; a disconnected
	// peer cannot receive it and does not require another recovery action.
	_ = conn.Close(websocket.StatusPolicyViolation, "host terminal session is no longer available")
}

func (s *Server) readHostTerminalClient(
	ctx context.Context,
	conn *websocket.Conn,
	owner string,
	id string,
) error {
	for {
		kind, body, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageText {
			return errors.New("host terminal control frame must be text")
		}
		var message terminalClientMessage
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&message); err != nil {
			return err
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return errors.New("host terminal control frame must contain one JSON document")
		}
		switch message.Type {
		case "input":
			if message.Columns != 0 || message.Rows != 0 {
				return errors.New("host terminal input frame is invalid")
			}
			if _, err := s.hostTerminals.Input(owner, id, []byte(message.Data)); err != nil {
				return err
			}
		case "resize":
			if message.Data != "" {
				return errors.New("host terminal resize frame is invalid")
			}
			if message.Columns < 20 || message.Columns > 500 || message.Rows < 5 || message.Rows > 300 {
				return errors.New("host terminal size is invalid")
			}
			if err := s.hostTerminals.Resize(owner, id, uint16(message.Columns), uint16(message.Rows)); err != nil {
				return err
			}
		case "terminate":
			if message.Data != "" || message.Columns != 0 || message.Rows != 0 {
				return errors.New("host terminal termination frame is invalid")
			}
			if err := s.hostTerminals.Terminate(owner, id, "operator"); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		default:
			return errors.New("host terminal control type is invalid")
		}
	}
}

func writeTerminalEvent(ctx context.Context, conn *websocket.Conn, event hostterminal.Event) error {
	switch event.Kind {
	case hostterminal.EventOutput:
		frame := make([]byte, 8+len(event.Data))
		binary.BigEndian.PutUint64(frame, event.Sequence)
		copy(frame[8:], event.Data)
		return conn.Write(ctx, websocket.MessageBinary, frame)
	case hostterminal.EventActivity:
		return writeTerminalJSON(ctx, conn, terminalServerMessage{Type: "activity", Session: &event.Session})
	case hostterminal.EventTerminated:
		return writeTerminalJSON(ctx, conn, terminalServerMessage{Type: "terminated", Session: &event.Session})
	default:
		return errors.New("host terminal event is invalid")
	}
}

func writeTerminalJSON(ctx context.Context, conn *websocket.Conn, value terminalServerMessage) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, body)
}

func terminalAfter(query url.Values) (uint64, error) {
	if len(query) == 0 {
		return 0, nil
	}
	values, ok := query["after"]
	if !ok || len(query) != 1 || len(values) != 1 || values[0] == "" {
		return 0, errors.New("invalid cursor")
	}
	return strconv.ParseUint(values[0], 10, 64)
}

func terminalOwner(cookieHeader string) string {
	cookies, err := http.ParseCookie(cookieHeader)
	if err != nil {
		return ""
	}
	owner := ""
	for _, cookie := range cookies {
		if cookie.Name != terminalOwnerCookie {
			continue
		}
		if owner != "" || len(cookie.Value) != 26 || cookie.Value != strings.ToUpper(cookie.Value) {
			return ""
		}
		if _, err := ulid.ParseStrict(cookie.Value); err != nil {
			return ""
		}
		owner = cookie.Value
	}
	return owner
}

func sameOriginHTTP(r *http.Request) bool {
	if len(r.Header.Values("Origin")) > 1 || len(r.Header.Values("Sec-Fetch-Site")) > 1 {
		return false
	}
	return sameOriginRequest(r.Header.Get("Origin"), r.Header.Get("Sec-Fetch-Site"), r.Host, r.TLS != nil)
}

func sameOriginRequest(origin, fetchSite, host string, tls bool) bool {
	if fetchSite != "" && fetchSite != "same-origin" {
		return false
	}
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.User != nil || parsed.Host == "" || parsed.Path != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return false
	}
	expectedScheme := "http"
	if tls {
		expectedScheme = "https"
	}
	return parsed.Scheme == expectedScheme && strings.EqualFold(parsed.Host, host)
}
