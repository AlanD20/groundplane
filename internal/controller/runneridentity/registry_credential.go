package runneridentity

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RegistryCredential struct {
	Username   string    `json:"username"`
	Password   string    `json:"password"`
	Repository string    `json:"repository"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type RegistryCredentialExchange interface {
	IssueRegistryCredential(context.Context, Principal, string, string) (RegistryCredential, error)
}

// WithRegistryCredentialExchange opts the scoped Runner listener into one
// narrowly bounded build credential route. The operator listener never
// receives this handler, and the default Runner routes remain closed.
func (handler *Handler) WithRegistryCredentialExchange(exchange RegistryCredentialExchange) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		id, matches := strings.CutPrefix(request.URL.Path, "/api/v1/runner-jobs/")
		if matches {
			id, matches = strings.CutSuffix(id, "/registry-credential")
		}
		if !matches {
			handler.ServeHTTP(writer, request)
			return
		}
		handler.serveRegistryCredential(writer, request, exchange, id)
	})
}

func (handler *Handler) serveRegistryCredential(
	writer http.ResponseWriter,
	request *http.Request,
	exchange RegistryCredentialExchange,
	id string,
) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	url := request.URL
	if exchange == nil || request.Method != http.MethodPost || url.IsAbs() || url.RawQuery != "" ||
		url.ForceQuery || url.Fragment != "" || url.EscapedPath() != url.Path ||
		ids.Validate(ids.KindOperation, id) != nil {
		handler.writeProblem(writer, errs.New(errs.KindRequestNotFound, "runner route not found"))
		return
	}
	principal, err := handler.authenticate(request)
	if err != nil {
		handler.writeProblem(writer, err)
		return
	}
	if principal.Credential.RequestID != "" {
		handler.writeProblem(writer, forbidden())
		return
	}
	proof, err := registryCredentialProof(request)
	if err != nil {
		handler.writeProblem(writer, err)
		return
	}
	credential, err := exchange.IssueRegistryCredential(request.Context(), principal, id, proof)
	if err != nil {
		handler.writeProblem(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(writer).Encode(credential); err != nil {
		handler.logger.Debug("write Runner registry credential response failed")
	}
}

func registryCredentialProof(request *http.Request) (string, error) {
	if request.ContentLength > maxRequestBody {
		return "", errs.New(errs.KindRequestTooLarge, "runner request exceeds the size limit")
	}
	media, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || request.Body == nil {
		return "", errs.New(errs.KindRequestUnsupportedMediaType, "runner request requires a JSON body")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBody+1))
	defer clear(body)
	if err != nil {
		return "", errs.New(errs.KindMalformedRequest, "runner request body could not be read")
	}
	if int64(len(body)) > maxRequestBody {
		return "", errs.New(errs.KindRequestTooLarge, "runner request exceeds the size limit")
	}
	canonical, err := jcs.Canonicalize(body)
	defer clear(canonical)
	if err != nil {
		return "", errs.New(errs.KindMalformedRequest, "build identity proof is invalid")
	}
	value, err := jcs.Decode[struct {
		Proof string `json:"proof"`
	}](canonical)
	if err != nil || value.Proof == "" {
		return "", errs.New(errs.KindMalformedRequest, "build identity proof is invalid")
	}
	return value.Proof, nil
}
