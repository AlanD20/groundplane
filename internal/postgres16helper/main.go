//go:build linux

package postgres16helper

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

// These measurements are linked into the helper by the managed-image build.
// Neither the request, its environment, nor a writable manifest selects them.
var (
	releaseGateSHA256      string
	releasePGDumpSHA256    string
	releasePGRestoreSHA256 string
	releasePSQLSHA256      string
)

// Main runs only the managed-image build. The Agent authenticates that image
// and its helper before Exec; the helper independently pins the gate and clients
// to the measurements compiled into those authenticated executable bytes.
func Main(ctx context.Context, arguments []string) (postgres16protocol.ExitCode, error) {
	if err := ctx.Err(); err != nil {
		return postgres16protocol.ExitDeadlineExceeded, err
	}
	gate, gateOK := releaseDigest(releaseGateSHA256)
	dump, dumpOK := releaseDigest(releasePGDumpSHA256)
	restore, restoreOK := releaseDigest(releasePGRestoreSHA256)
	psql, psqlOK := releaseDigest(releasePSQLSHA256)
	if !gateOK || !dumpOK || !restoreOK || !psqlOK {
		return postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	helper, identity, err := inspectReleaseFile(postgres16protocol.HelperPath)
	if err != nil {
		return postgres16protocol.ExitEnvironmentInvalid, err
	}
	if err := helper.Close(); err != nil {
		return postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	seccomp, err := GateSeccompSHA256()
	if err != nil {
		return postgres16protocol.ExitEnvironmentInvalid, err
	}
	runtime, err := NewRuntime(postgres16protocol.ConfinementReleaseAuthority{
		HelperSHA256: identity.SHA256, GateSHA256: gate, PGDumpSHA256: dump,
		PGRestoreSHA256: restore, PSQLSHA256: psql,
		LaunchProfileSHA256: postgres16protocol.ManagedLaunchProfileSHA256(), GateSeccompSHA256: seccomp,
	})
	if err != nil {
		return postgres16protocol.ExitEnvironmentInvalid, err
	}
	return runtime.Run(ctx, arguments)
}

func releaseDigest(value string) (postgres16protocol.Digest, bool) {
	var digest postgres16protocol.Digest
	if len(value) != hex.EncodedLen(len(digest)) || strings.ToLower(value) != value {
		return digest, false
	}
	n, err := hex.Decode(digest[:], []byte(value))
	return digest, err == nil && n == len(digest) && digest != (postgres16protocol.Digest{})
}
