package apiclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/coder/websocket"
)

func (c *Client) OpenHostTerminal(
	ctx context.Context,
	columns int,
	rows int,
) (apiTypes.HostTerminalSession, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.HostTerminalSession{}, err
	}
	const path = "/api/v1/host/terminal"
	key := ids.NewULID()
	var response *generated.HostTerminalResponse
	for attempt := 0; attempt < 2; attempt++ {
		response, err = client.HostTerminalWithResponse(
			ctx,
			&generated.HostTerminalParams{IdempotencyKey: key},
			generated.HostTerminalJSONRequestBody{Columns: int64(columns), Rows: int64(rows)},
		)
		if err == nil {
			break
		}
		if ctx.Err() != nil || attempt == 1 {
			return apiTypes.HostTerminalSession{}, generatedCallError(ctx, http.MethodPost, path, err)
		}
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.HostTerminalSession{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.HostTerminalSession{}
		if err := decodeSingleJSON(http.MethodPost, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.HostTerminalSession{}, err
		}
	}
	reason := ""
	if parsed.Reason != nil {
		reason = *parsed.Reason
	}
	return apiTypes.HostTerminalSession{
		ID: parsed.Id, Host: parsed.Host, User: parsed.User, State: string(parsed.State), Reason: reason,
		InputInactivitySeconds: int(parsed.InputInactivitySeconds), ExpiresAt: parsed.ExpiresAt,
		StreamPath: parsed.StreamPath, Resumed: parsed.Resumed,
	}, nil
}

func (c *Client) DialHostTerminal(
	ctx context.Context,
	session apiTypes.HostTerminalSession,
	after uint64,
) (*websocket.Conn, error) {
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("apiclient: bad terminal base url: %w", err))
	}
	switch base.Scheme {
	case "http":
		base.Scheme = "ws"
	case "https":
		base.Scheme = "wss"
	default:
		return nil, errs.New(
			errs.KindValidationFailed,
			"apiclient: terminal requires an http or https Controller address",
		)
	}
	if !strings.HasPrefix(session.StreamPath, "/api/v1/host/terminal/") {
		return nil, errs.New(errs.KindInternal, "apiclient: terminal stream path is invalid")
	}
	base.Path = session.StreamPath
	base.RawPath = ""
	base.RawQuery = url.Values{"after": []string{strconv.FormatUint(after, 10)}}.Encode()
	conn, response, err := websocket.Dial(ctx, base.String(), &websocket.DialOptions{HTTPClient: c.StreamingHTTP})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestFailed, fmt.Errorf("apiclient: open terminal stream: %w", err))
	}
	conn.SetReadLimit(40 << 10)
	return conn, nil
}
