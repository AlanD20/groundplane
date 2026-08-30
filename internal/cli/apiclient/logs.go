package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StreamLogs consumes the transient, non-resumable log representation exposed
// by the Controller. It deliberately does not retry: every reconnect creates
// a new fixed-source tail as required by ADR 0059.
func (c *Client) StreamLogs(
	ctx context.Context,
	path string,
	query map[string]string,
	onEvent func(apiTypes.LogEvent) error,
) error {
	if onEvent == nil {
		return errs.New(errs.KindInternal, "apiclient: log event callback is required")
	}
	return c.Stream(ctx, path, query, func(data string) error {
		decoder := json.NewDecoder(strings.NewReader(data))
		decoder.DisallowUnknownFields()
		var event apiTypes.LogEvent
		if err := decoder.Decode(&event); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("apiclient: decode log event: %w", err))
		}
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return errs.New(errs.KindInternal, "apiclient: log event contains multiple JSON documents")
			}
			return errs.Wrap(errs.KindInternal, fmt.Errorf("apiclient: decode trailing log event: %w", err))
		}
		if err := validateLogEvent(event); err != nil {
			return err
		}
		return onEvent(event)
	})
}

func validateLogEvent(event apiTypes.LogEvent) error {
	if event.Sequence == 0 || ids.Validate(ids.KindService, event.ServiceID) != nil ||
		ids.Validate(ids.KindDeployment, event.ReleaseID) != nil || event.ServiceName == "" ||
		event.ContainerID == "" || event.ContainerName == "" || event.Timestamp.IsZero() ||
		!utf8.ValidString(event.ServiceName) || !utf8.ValidString(event.ContainerID) ||
		!utf8.ValidString(event.ContainerName) || !utf8.ValidString(event.Line) ||
		(event.Slot != "blue" && event.Slot != "green" && event.Slot != "singleton") ||
		(event.Stream != "stdout" && event.Stream != "stderr") {
		return errs.New(errs.KindInternal, "apiclient: log event data is invalid")
	}
	return nil
}
