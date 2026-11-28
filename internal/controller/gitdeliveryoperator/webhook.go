package gitdeliveryoperator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	GitHubWebhookPrefix = "/webhooks/github/"
	maxWebhookPayload   = 2 << 20
	maxWebhookRequests  = 16
	webhookTimeout      = 8 * time.Second
)

// GitHubWebhook is mounted on the dedicated webhook listener. It handles only
// the canonical provider route and has no access to the human API mux.
func (operator *Operator) GitHubWebhook() http.Handler {
	active := make(chan struct{}, maxWebhookRequests)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		scope, connectionID, found := webhookPath(request)
		if !found {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		select {
		case active <- struct{}{}:
			defer func() { <-active }()
		default:
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), webhookTimeout)
		defer cancel()
		writer.WriteHeader(operator.receiveWebhook(ctx, writer, request, scope, connectionID))
	})
}

func webhookPath(request *http.Request) (githubconnection.Scope, string, bool) {
	var scope githubconnection.Scope
	if !strings.HasPrefix(request.URL.Path, GitHubWebhookPrefix) || request.URL.RawPath != "" ||
		request.URL.RawQuery != "" || request.URL.ForceQuery || request.URL.Opaque != "" {
		return scope, "", false
	}
	fields := strings.Split(strings.TrimPrefix(request.URL.Path, GitHubWebhookPrefix), "/")
	if len(fields) != 3 {
		return scope, "", false
	}
	switch fields[0] {
	case "tenant":
		scope.Kind = ids.KindTenant
	case "project":
		scope.Kind = ids.KindProject
	case "environment":
		scope.Kind = ids.KindEnvironment
	default:
		return scope, "", false
	}
	scope.ID = fields[1]
	return scope, fields[2], ids.Validate(scope.Kind, scope.ID) == nil &&
		ids.Validate(githubconnection.IDKind, fields[2]) == nil
}

func (operator *Operator) receiveWebhook(ctx context.Context, writer http.ResponseWriter, request *http.Request,
	scope githubconnection.Scope, connectionID string,
) int {
	signature := request.Header.Values("X-Hub-Signature-256")
	if len(signature) != 1 {
		return http.StatusUnauthorized
	}
	digest, valid := webhookDigest(signature[0])
	if !valid {
		return http.StatusUnauthorized
	}
	media, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return http.StatusUnsupportedMediaType
	}
	if request.ContentLength > maxWebhookPayload {
		return http.StatusRequestEntityTooLarge
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(writer).SetReadDeadline(deadline)
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxWebhookPayload))
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			return http.StatusRequestEntityTooLarge
		}
		return http.StatusBadRequest
	}
	defer clear(body)
	current, err := operator.connections.Get(ctx, scope, connectionID)
	if err != nil {
		return webhookFailure(err)
	}
	if current.Record.RevokedAt != nil || current.Record.Generation > math.MaxInt64 {
		return http.StatusUnauthorized
	}
	err = operator.connections.WithCredentials(ctx, scope, connectionID, current.Record.Generation,
		func(_ githubconnection.Connection, credentials githubconnection.Credentials) error {
			if !verifyWebhookSignature(credentials.WebhookSecret, body, digest) {
				return errs.New(errs.KindRequestForbidden, "invalid GitHub webhook signature")
			}
			return nil
		})
	if err != nil {
		return webhookFailure(err)
	}
	events := request.Header.Values("X-GitHub-Event")
	if len(events) != 1 {
		return http.StatusBadRequest
	}
	if events[0] != "push" {
		return http.StatusNoContent
	}
	status, push := decodeWebhookPush(body, current.Record)
	if status != http.StatusAccepted {
		return status
	}
	guards, err := operator.connections.ActiveFence(ctx, scope, connectionID, current.Record.Generation)
	if err != nil {
		return webhookFailure(err)
	}
	if err := operator.delivery.QueuePush(ctx, push, guards); err != nil {
		return webhookFailure(err)
	}
	return http.StatusAccepted
}

func decodeWebhookPush(body []byte, connection githubconnection.Connection) (int, gitdelivery.Push) {
	var event struct {
		Ref        string `json:"ref"`
		After      string `json:"after"`
		Deleted    bool   `json:"deleted"`
		Repository struct {
			ID int64 `json:"id"`
		} `json:"repository"`
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		return http.StatusBadRequest, gitdelivery.Push{}
	}
	if event.Installation.ID != connection.InstallationID || event.Repository.ID <= 0 {
		return http.StatusUnauthorized, gitdelivery.Push{}
	}
	branch, isBranch := strings.CutPrefix(event.Ref, "refs/heads/")
	if !isBranch || event.Deleted || event.After == strings.Repeat("0", 40) {
		return http.StatusNoContent, gitdelivery.Push{}
	}
	return http.StatusAccepted, gitdelivery.Push{
		ConnectionID: connection.ID, ConnectionScopeKind: connection.Scope.Kind,
		ConnectionScopeID: connection.Scope.ID, ConnectionGeneration: int64(connection.Generation),
		InstallationID: connection.InstallationID, RepositoryID: event.Repository.ID,
		Branch: branch, Commit: event.After,
	}
}

func webhookDigest(value string) ([sha256.Size]byte, bool) {
	var digest [sha256.Size]byte
	value, found := strings.CutPrefix(value, "sha256=")
	if !found || len(value) != hex.EncodedLen(len(digest)) {
		return digest, false
	}
	_, err := hex.Decode(digest[:], []byte(value))
	return digest, err == nil
}

func verifyWebhookSignature(secret, body []byte, digest [sha256.Size]byte) bool {
	if len(secret) == 0 {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(digest[:], mac.Sum(nil))
}

func webhookFailure(err error) int {
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindRequestForbidden, errs.KindRequestNotFound, errs.KindScopeUnauthorized, errs.KindStateConflict,
		errs.KindTenantNotFound, errs.KindProjectNotFound, errs.KindEnvironmentNotFound, errs.KindResourceInUse:
		return http.StatusUnauthorized
	case errs.KindValidationFailed, errs.KindMalformedRequest:
		return http.StatusBadRequest
	default:
		return http.StatusServiceUnavailable
	}
}
