package app

import (
	"encoding/base64"
	"io"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	attachPasswordEntropyBytes  = 32
	attachIdentitySuffixBytes   = 6
	maximumAttachIdentityBytes  = 63
	maximumAttachServiceNameLen = maximumAttachIdentityBytes - 1 - attachIdentitySuffixBytes
)

type attachNameLabels struct {
	tenant      string
	project     string
	environment string
	service     string
}

func attachProvisionIdentity(attachID string, serviceName string) (string, error) {
	if err := ids.Validate(ids.KindAttach, attachID); err != nil {
		return "", errs.New(errs.KindValidationFailed, "Attach identity requires a canonical Attach id")
	}
	if len(serviceName) == 0 || len(serviceName) > maximumAttachServiceNameLen ||
		!validAttachIdentityAtom(serviceName) {
		return "", errs.Newf(
			errs.KindValidationFailed,
			"credential-backed Attach service name must be an ASCII identity atom of 1-%d bytes",
			maximumAttachServiceNameLen,
		)
	}
	body := strings.TrimPrefix(attachID, string(ids.KindAttach)+"_")
	identity := serviceName + "_" + strings.ToLower(body[10:10+attachIdentitySuffixBytes])
	if len(identity) > maximumAttachIdentityBytes {
		return "", errs.New(errs.KindInternal, "generated Attach identity exceeds its adapter bound")
	}
	return identity, nil
}

func generateAttachPassword(random io.Reader) ([]byte, error) {
	if random == nil {
		return nil, errs.New(errs.KindInternal, "Attach password entropy source is required")
	}
	raw := make([]byte, attachPasswordEntropyBytes)
	defer clear(raw)
	if _, err := io.ReadFull(random, raw); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	password := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(password, raw)
	return password, nil
}

func suggestAttachName(labels attachNameLabels, existing map[string]struct{}) (string, error) {
	parts := []string{labels.tenant, labels.project, labels.environment, labels.service}
	for index := range parts {
		normalized, err := normalizeAttachNameLabel(parts[index])
		if err != nil {
			return "", err
		}
		parts[index] = normalized
	}
	base := strings.Join(parts, "-")
	for ordinal := 1; ordinal <= len(existing)+1; ordinal++ {
		candidate := attachNameCandidate(base, ordinal)
		if _, occupied := existing[candidate]; occupied {
			continue
		}
		if err := etcd.ValidateAttachName(candidate); err != nil {
			return "", err
		}
		return candidate, nil
	}
	return "", errs.New(errs.KindInternal, "Attach name selection exhausted its finite candidate set")
}

func attachNameCandidate(base string, ordinal int) string {
	if ordinal == 1 {
		return base
	}
	suffix := "-" + strconv.Itoa(ordinal)
	limit := etcd.MaximumAttachNameBytes - len(suffix)
	trimmed := strings.TrimRight(base[:min(len(base), limit)], "-")
	return trimmed + suffix
}

func normalizeAttachNameLabel(value string) (string, error) {
	if value == "" {
		return "", errs.New(errs.KindValidationFailed, "Attach name labels must be nonempty")
	}
	var normalized strings.Builder
	normalized.Grow(len(value))
	separator := false
	for _, character := range []byte(value) {
		switch {
		case character >= 'A' && character <= 'Z':
			normalized.WriteByte(character + ('a' - 'A'))
			separator = false
		case character >= 'a' && character <= 'z' || character >= '0' && character <= '9':
			normalized.WriteByte(character)
			separator = false
		case character == '-' || character == '_' || character == '.':
			if normalized.Len() > 0 && !separator {
				normalized.WriteByte('-')
				separator = true
			}
		default:
			return "", errs.New(
				errs.KindValidationFailed,
				"Attach name labels must contain only ASCII letters, digits, hyphens, underscores, or dots",
			)
		}
	}
	result := strings.TrimRight(normalized.String(), "-")
	if result == "" {
		return "", errs.New(errs.KindValidationFailed, "Attach name labels must contain an ASCII letter or digit")
	}
	return result, nil
}

func validAttachIdentityAtom(value string) bool {
	for _, character := range []byte(value) {
		if character == '_' || character == '-' || character == '.' ||
			character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}
