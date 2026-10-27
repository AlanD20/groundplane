// Package discordwebhook owns bounded Discord webhook delivery, never settings.
package discordwebhook

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type Client struct{ http *http.Client }
type Result struct {
	Permanent  bool
	RetryAfter time.Duration
}

func NewClient() *Client {
	return &Client{
		http: &http.Client{
			Timeout:       5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}
func ValidateURL(raw string) error {
	value, err := url.Parse(raw)
	if err != nil || value.Scheme != "https" || value.Host != "discord.com" || value.User != nil ||
		value.RawQuery != "" ||
		value.Fragment != "" ||
		value.RawPath != "" {
		return errs.New(errs.KindValidationFailed, "Use an HTTPS discord.com webhook URL without query parameters")
	}
	parts := strings.Split(strings.TrimPrefix(value.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "api" || parts[1] != "webhooks" || len(parts[2]) < 1 || len(parts[3]) < 16 ||
		len(raw) > 2048 {
		return errs.New(errs.KindValidationFailed, "Discord webhook URL must contain its webhook id and token")
	}
	for _, c := range parts[2] {
		if !unicode.IsDigit(c) || c > 127 {
			return errs.New(errs.KindValidationFailed, "Discord webhook id is invalid")
		}
	}
	for _, c := range parts[3] {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return errs.New(errs.KindValidationFailed, "Discord webhook token is invalid")
		}
	}
	return nil
}
func (client *Client) Send(ctx context.Context, webhook, content string) (Result, error) {
	if utf8.RuneCountInString(content) > 2000 {
		return Result{
				Permanent: true,
			}, errs.New(
				errs.KindValidationFailed,
				"Discord notification exceeds its message limit",
			)
	}
	if err := ValidateURL(webhook); err != nil {
		return Result{Permanent: true}, err
	}
	payload := struct {
		Content         string `json:"content"`
		AllowedMentions struct {
			Parse []string `json:"parse"`
		} `json:"allowed_mentions"`
	}{Content: content}
	payload.AllowedMentions.Parse = []string{}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Result{Permanent: true}, errs.New(errs.KindInternal, "Discord notification could not be encoded")
	}
	defer clear(raw)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook+"?wait=true", bytes.NewReader(raw))
	if err != nil {
		return Result{Permanent: true}, errs.New(errs.KindRequestFailed, "Discord notification request is invalid")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Groundplane task alerts")
	response, err := client.http.Do(request)
	if err != nil {
		return Result{}, errs.New(errs.KindRequestFailed, "Discord notification could not be delivered")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusTooManyRequests {
		seconds, _ := strconv.ParseFloat(response.Header.Get("Retry-After"), 64)
		delay := time.Duration(seconds * float64(time.Second))
		if delay < time.Second {
			delay = 30 * time.Second
		}
		if delay > time.Hour {
			delay = time.Hour
		}
		return Result{RetryAfter: delay}, errs.New(errs.KindRequestFailed, "Discord rate limited this notification")
	}
	if response.StatusCode != http.StatusOK {
		return Result{
				Permanent: response.StatusCode >= 300 && response.StatusCode < 500,
			}, errs.New(
				errs.KindRequestFailed,
				"Discord rejected this notification",
			)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	defer clear(body)
	var message struct {
		ID string `json:"id"`
	}
	if err != nil || len(body) > 65536 || json.Unmarshal(body, &message) != nil || message.ID == "" {
		return Result{}, errs.New(errs.KindRequestFailed, "Discord did not confirm this notification")
	}
	return Result{}, nil
}
