package dnsresolverobserver

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/coredns/caddy/caddyfile"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const reportedConfigMarker = "plugin/reload: Running configuration SHA512 = "

func effectiveConfigSHA512(path string, artifact []byte) ([sha512.Size]byte, error) {
	if path == "" || len(artifact) == 0 || len(artifact) > maximumArtifactBytes {
		return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver effective configuration is invalid")
	}
	blocks, err := caddyfile.Parse(path, bytes.NewReader(artifact), nil)
	if err != nil {
		return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver effective configuration is invalid")
	}
	parsed, err := json.Marshal(blocks)
	if err != nil {
		return [sha512.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(parsed)
	return sha512.Sum512(parsed), nil
}

func latestReportedConfigSHA512(logs []byte) ([sha512.Size]byte, error) {
	if len(logs) == 0 || len(logs) > maximumLogBytes {
		return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver runtime logs are invalid")
	}
	result := [sha512.Size]byte{}
	found := false
	for _, line := range strings.Split(string(logs), "\n") {
		count := strings.Count(line, reportedConfigMarker)
		if count == 0 {
			continue
		}
		if count != 1 {
			return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver configuration log is ambiguous")
		}
		value := strings.TrimSpace(line[strings.Index(line, reportedConfigMarker)+len(reportedConfigMarker):])
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha512.Size || hex.EncodeToString(decoded) != value {
			return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver configuration log is invalid")
		}
		copy(result[:], decoded)
		clear(decoded)
		found = true
	}
	if !found {
		return [sha512.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver configuration log is missing")
	}
	return result, nil
}
