package dnsresolverobserver

import (
	"bytes"
	"crypto/sha512"
	"encoding/json"

	"github.com/coredns/caddy/caddyfile"

	"github.com/AlanD20/groundplane/pkg/errs"
)

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
