package discordwebhook

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ALERT-04: actual HTTP outcomes govern retryability; rejected credentials and
// redirects cannot become silent successes or leak a credential into an error.
func TestDeliveryRequiresConfirmationAndHonorsRateLimit(t *testing.T) {
	const webhook = "https://discord.com/api/webhooks/123456789/abcdefghijklmnopqrstuv"
	for _, test := range []struct {
		status    int
		body      string
		retry     bool
		permanent bool
	}{
		{200, `{"id":"1234"}`, false, false}, {200, `{}`, true, false},
		{429, `private body`, true, false}, {403, `private body`, true, true}, {302, `private body`, true, true},
	} {
		t.Run(http.StatusText(test.status)+test.body, func(t *testing.T) {
			client := NewClient()
			client.http.Transport = alertTransport(func(request *http.Request) (*http.Response, error) {
				if request.URL.Query().Get("wait") != "true" {
					t.Fatal("delivery did not request confirmation")
				}
				return &http.Response{
					StatusCode: test.status,
					Header:     http.Header{"Retry-After": []string{"45"}},
					Body:       io.NopCloser(strings.NewReader(test.body)),
					Request:    request,
				}, nil
			})
			result, err := client.Send(context.Background(), webhook, "Task failed")
			if (err != nil) != test.retry || result.Permanent != test.permanent {
				t.Fatal("wrong delivery classification", result, err)
			}
			if test.status == 429 && result.RetryAfter != 45*time.Second {
				t.Fatal("rate-limit delay was ignored")
			}
			if err != nil && (strings.Contains(err.Error(), "private body") || strings.Contains(err.Error(), webhook)) {
				t.Fatal("private transport data escaped")
			}
		})
	}
}

type alertTransport func(*http.Request) (*http.Response, error)

func (transport alertTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
