package runneridentity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	tokenPrefix = "gpr1_"
	tokenBytes  = 32
	tokenLength = 43
)

// Reader resolves an opaque credential digest at a fresh registry snapshot.
// The lookup must exclude revoked grants and Runners which no longer own the
// credential's runtime epoch. The binding revision fences later admission.
type Reader interface {
	ReadRunnerCredential(context.Context, [sha256.Size]byte) (runnercredential.Binding, error)
}

// Principal is the server-resolved credential binding. Digest is a storage
// lookup identifier for the admission fence, not a usable bearer credential.
type Principal struct {
	Credential runnercredential.Binding
	Digest     [sha256.Size]byte
}

// Mint creates an opaque credential. Deliver the bearer only to its intended
// Runner or job. Persist the digest together with its authorization binding.
func Mint() (bearer string, digest [sha256.Size]byte, err error) {
	var raw [tokenBytes]byte
	defer clear(raw[:])
	if _, err := rand.Read(raw[:]); err != nil {
		return "", digest, errs.Wrap(errs.KindInternal, err)
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(raw[:]), sha256.Sum256(raw[:]), nil
}

func credentialDigest(headers http.Header) ([sha256.Size]byte, bool) {
	var zero [sha256.Size]byte
	values := headers.Values("Authorization")
	if len(values) != 1 {
		return zero, false
	}
	scheme, bearer, separated := strings.Cut(values[0], " ")
	if !separated || !strings.EqualFold(scheme, "Bearer") {
		return zero, false
	}
	encoded, hasPrefix := strings.CutPrefix(bearer, tokenPrefix)
	if !hasPrefix || len(encoded) != tokenLength {
		return zero, false
	}
	var raw [tokenBytes]byte
	defer clear(raw[:])
	size, err := base64.RawURLEncoding.Strict().Decode(raw[:], []byte(encoded))
	if err != nil || size != len(raw) || base64.RawURLEncoding.EncodeToString(raw[:]) != encoded {
		return zero, false
	}
	return sha256.Sum256(raw[:]), true
}

func (handler *Handler) authenticate(request *http.Request) (Principal, error) {
	digest, valid := credentialDigest(request.Header)
	if !valid {
		return Principal{}, forbidden()
	}
	credential, err := handler.reader.ReadRunnerCredential(request.Context(), digest)
	if err != nil {
		return Principal{}, err
	}
	if ids.Validate(ids.KindRunner, credential.RunnerID) != nil || credential.Revision <= 0 ||
		credential.RunnerEpoch == 0 || !credential.ActiveAt(time.Now()) ||
		credential.RequestID != "" && ids.Validate(ids.KindOperation, credential.RequestID) != nil {
		return Principal{}, forbidden()
	}
	return Principal{Credential: credential, Digest: digest}, nil
}
