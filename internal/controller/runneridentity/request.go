package runneridentity

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maxRequestBody int64 = 16 << 10

func matchRequest(request *http.Request) (Request, bool) {
	url := request.URL
	if url.IsAbs() || url.RawQuery != "" || url.ForceQuery || url.Fragment != "" || url.EscapedPath() != url.Path {
		return Request{}, false
	}
	path, matched := strings.CutPrefix(url.Path, "/api/v1/")
	if !matched {
		return Request{}, false
	}
	group, remainder, matched := strings.Cut(path, "/")
	if !matched {
		return Request{}, false
	}
	target, suffix, trailing := strings.Cut(remainder, "/")
	var kind ids.Kind
	var operation runneraccess.Operation
	switch {
	case request.Method == http.MethodGet && group == "services" && !trailing:
		kind, operation = ids.KindService, runneraccess.ReadService
	case request.Method == http.MethodGet && group == "tasks" && !trailing:
		kind, operation = ids.KindTask, runneraccess.ReadTask
	case request.Method == http.MethodPost && group == "services" && suffix == "deploy":
		kind, operation = ids.KindService, runneraccess.DeployService
	case request.Method == http.MethodPost && group == "services" && suffix == "images/fetch":
		kind, operation = ids.KindService, runneraccess.FetchServiceImage
	case request.Method == http.MethodPost && group == "runner-jobs" && suffix == "result":
		kind, operation = ids.KindOperation, runneraccess.ReportBuild
	default:
		return Request{}, false
	}
	if ids.Validate(kind, target) != nil {
		return Request{}, false
	}
	return Request{Operation: operation, ResourceID: target}, true
}

func decodeRequest(request *http.Request, input *Request) error {
	emptyBody := request.ContentLength == 0 && len(request.TransferEncoding) == 0 &&
		(request.Body == nil || request.Body == http.NoBody)
	if request.Method == http.MethodGet {
		if !emptyBody {
			return errs.New(errs.KindMalformedRequest, "runner read must not include a request body")
		}
		return nil
	}
	key, valid := idempotencyKey(request.Header)
	if !valid {
		return errs.New(errs.KindMalformedRequest, "runner mutation requires a valid idempotency key")
	}
	input.IdempotencyKey = key
	if request.ContentLength > maxRequestBody {
		return errs.New(errs.KindRequestTooLarge, "runner request exceeds the size limit")
	}

	if input.Operation == runneraccess.DeployService && emptyBody {
		input.Deploy = &api.DeployRequest{}
		return nil
	}
	if request.Body == nil || request.Body == http.NoBody {
		return errs.New(errs.KindMalformedRequest, "runner request requires a JSON body")
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errs.New(errs.KindRequestUnsupportedMediaType, "runner request requires a JSON body")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBody+1))
	if err != nil {
		return errs.New(errs.KindMalformedRequest, "runner request body could not be read")
	}
	if int64(len(body)) > maxRequestBody {
		return errs.New(errs.KindRequestTooLarge, "runner request exceeds the size limit")
	}
	canonical, err := jcs.Canonicalize(body)
	if err != nil || !bytes.HasPrefix(canonical, []byte("{")) {
		return errs.New(errs.KindMalformedRequest, "runner request body must be a JSON object")
	}
	return decodeAction(input, canonical)
}

func decodeAction(input *Request, canonical []byte) error {
	switch input.Operation {
	case runneraccess.DeployService:
		body, err := jcs.Decode[api.DeployRequest](canonical)
		if err != nil || len(body.Image) > 512 || body.Image != "" && body.Tag != "" || !validTaskIDs(body.After) {
			return errs.New(errs.KindMalformedRequest, "runner deploy body is invalid")
		}
		input.Deploy = &body
	case runneraccess.FetchServiceImage:
		body, err := jcs.Decode[api.ImageFetchRequest](canonical)
		if err != nil || len(body.Image) > 512 || !strings.HasPrefix(body.Image, imagefetch.RegistryAuthority+"/") ||
			!imageref.IsDigestPinned(body.Image) || !validTaskIDs(body.After) {
			return errs.New(
				errs.KindMalformedRequest,
				"runner image fetch requires a managed registry digest and valid prerequisites",
			)
		}
		input.Fetch = &body
	case runneraccess.ReportBuild:
		body, err := jcs.Decode[BuildResult](canonical)
		if err != nil || body.RunID <= 0 || body.RunAttempt <= 0 || !validCommit(body.SourceCommit) ||
			!workloadimage.LocalIDValid(body.ImageDigest) {
			return errs.New(errs.KindMalformedRequest, "runner build result is invalid")
		}
		input.Build = &body
	default:
		return forbidden()
	}
	return nil
}

func validTaskIDs(values []string) bool {
	if len(values) > 16 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate || ids.Validate(ids.KindTask, value) != nil {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validCommit(commit string) bool {
	if len(commit) != 40 || strings.ToLower(commit) != commit {
		return false
	}
	_, err := hex.DecodeString(commit)
	return err == nil
}

func idempotencyKey(headers http.Header) (string, bool) {
	values := headers.Values("Idempotency-Key")
	if len(values) != 1 || len(values[0]) < 16 || len(values[0]) > 128 {
		return "", false
	}
	for _, char := range values[0] {
		if 'a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' || '0' <= char && char <= '9' ||
			char == '.' || char == '_' || char == ':' || char == '-' {
			continue
		}
		return "", false
	}
	return values[0], true
}

// namespaceKey prevents a Runner from replaying another Runner's or operator's
// idempotency receipt by supplying its key. Rotation preserves the namespace.
func namespaceKey(principal Principal, key string) string {
	digest := sha256.Sum256(
		[]byte(principal.Credential.RunnerID + "\x00" + principal.Credential.RequestID + "\x00" + key),
	)
	return "runner:" + hex.EncodeToString(digest[:])
}
