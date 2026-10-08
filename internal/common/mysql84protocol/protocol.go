// Package mysql84protocol owns the closed MySQL 8.4 logical-backup contract.
// It accepts no operator command, SQL, path, or environment input.
package mysql84protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	AdapterContractVersion uint32 = 1
	ServerMajor            uint32 = 8
	ServerMinor            uint32 = 4
	ArtifactFormat                = "mysql-logical-v1"
	MaximumVersionBytes           = 128
	DiagnosticLimitBytes   uint64 = 64 * 1024
)

type Digest [sha256.Size]byte
type Nonce [sha256.Size]byte

type Operation uint8

const (
	OperationProbeMySQL Operation = iota + 1
	OperationProbeMySQLDump
	OperationServerVersion
	OperationToolVersion
	OperationAssertZeroConnections
	OperationDump
	OperationRestoreApply
	OperationPostRestoreVerify
	OperationRecoverDump
	OperationRecoverEvidence
	OperationRecoveryInventory
	OperationRetire
	OperationRestoreToolVersion
)

type Request struct {
	Operation        Operation
	Nonce            Nonce
	DeadlineUnixNano uint64
	Database         string
	Role             string
	MaximumBytes     uint64
	SourceSize       uint64
	SourceSHA256     Digest
}

type StreamEvidence struct {
	Bytes  uint64
	SHA256 Digest
	EOF    bool
}

type StartEvidence struct {
	Request     Request
	ContainerID string
	ExecID      string
}

var (
	generatedIdentity = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	serverVersion     = regexp.MustCompile(`^8\.4\.[0-9]+(?:[-+._a-zA-Z0-9]*)?$`)
	toolVersion       = regexp.MustCompile(
		`^(?:mysql|mysqldump) +Ver 8\.4\.[0-9]+(?:[-+._a-zA-Z0-9]*)?(?: +[^\r\n]+)?$`,
	)
)

func ValidGeneratedIdentity(value string) bool {
	return generatedIdentity.MatchString(value)
}

func ValidateObservedServerVersion(value string) error {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > MaximumVersionBytes || !serverVersion.MatchString(value) {
		return errs.New(errs.KindValidationFailed, "MySQL source server version is not supported")
	}
	return nil
}

func ValidateObservedToolVersion(tool, value string) error {
	if (tool != "mysql" && tool != "mysqldump") || len(value) > MaximumVersionBytes ||
		!strings.HasPrefix(value, tool+" ") || !toolVersion.MatchString(value) {
		return errs.New(errs.KindValidationFailed, "MySQL tool version is not supported")
	}
	return nil
}

func (request Request) Validate() error {
	if request.Nonce == (Nonce{}) || request.DeadlineUnixNano == 0 {
		return invalidRequest()
	}
	switch request.Operation {
	case OperationProbeMySQL,
		OperationProbeMySQLDump,
		OperationToolVersion,
		OperationRestoreToolVersion,
		OperationRecoveryInventory:
		if request.Database != "" || request.Role != "" || request.MaximumBytes != 0 ||
			request.SourceSize != 0 || request.SourceSHA256 != (Digest{}) {
			return invalidRequest()
		}
	case OperationServerVersion, OperationAssertZeroConnections, OperationPostRestoreVerify:
		if !ValidGeneratedIdentity(request.Database) || request.Role != "" || request.MaximumBytes != 0 ||
			request.SourceSize != 0 || request.SourceSHA256 != (Digest{}) {
			return invalidRequest()
		}
	case OperationDump:
		if !ValidGeneratedIdentity(request.Database) || !ValidGeneratedIdentity(request.Role) ||
			(request.MaximumBytes != backupformat.MaxStoredBytes && request.MaximumBytes != backupformat.MaxAgeSourceBytes) ||
			request.SourceSize != 0 || request.SourceSHA256 != (Digest{}) {
			return invalidRequest()
		}
	case OperationRestoreApply:
		if !ValidGeneratedIdentity(request.Database) || !ValidGeneratedIdentity(request.Role) ||
			request.MaximumBytes != 0 || request.SourceSize == 0 || request.SourceSize > backupformat.MaxStoredBytes ||
			request.SourceSHA256 == (Digest{}) {
			return invalidRequest()
		}
	case OperationRecoverDump:
		if request.Database != "" || request.Role != "" || request.MaximumBytes == 0 ||
			request.MaximumBytes > backupformat.MaxStoredBytes || request.SourceSize != 0 ||
			request.SourceSHA256 != (Digest{}) {
			return invalidRequest()
		}
	case OperationRecoverEvidence, OperationRetire:
		if request.Database != "" || request.Role != "" || request.MaximumBytes != 0 ||
			request.SourceSize != 0 || request.SourceSHA256 != (Digest{}) {
			return invalidRequest()
		}
	default:
		return invalidRequest()
	}
	return nil
}

func (request Request) DockerExecCommand() ([]string, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	nonce := hex.EncodeToString(request.Nonce[:])
	deadline := strconv.FormatUint(request.DeadlineUnixNano, 10)
	args := []string{closedScript, operationName(request.Operation), nonce, deadline}
	switch request.Operation {
	case OperationServerVersion, OperationAssertZeroConnections, OperationPostRestoreVerify:
		args = append(args, request.Database)
	case OperationDump:
		args = append(args, request.Database, request.Role, strconv.FormatUint(request.MaximumBytes, 10))
	case OperationRestoreApply:
		args = append(args, request.Database, request.Role, strconv.FormatUint(request.SourceSize, 10),
			hex.EncodeToString(request.SourceSHA256[:]))
	case OperationRecoverDump:
		args = append(args, strconv.FormatUint(request.MaximumBytes, 10))
	}
	return append([]string{"/bin/sh", "-ceu", closedScript, "groundplane-mysql84"}, args[1:]...), nil
}

func operationName(operation Operation) string {
	switch operation {
	case OperationProbeMySQL:
		return "probe-mysql"
	case OperationProbeMySQLDump:
		return "probe-mysqldump"
	case OperationServerVersion:
		return "server-version"
	case OperationToolVersion:
		return "tool-version"
	case OperationRestoreToolVersion:
		return "restore-tool-version"
	case OperationAssertZeroConnections:
		return "zero-connections"
	case OperationDump:
		return "dump"
	case OperationRestoreApply:
		return "restore-apply"
	case OperationPostRestoreVerify:
		return "post-restore-verify"
	case OperationRecoverDump:
		return "recover-dump"
	case OperationRecoverEvidence:
		return "recover-evidence"
	case OperationRecoveryInventory:
		return "recovery-inventory"
	case OperationRetire:
		return "retire"
	default:
		return ""
	}
}

func invalidRequest() error {
	return errs.New(errs.KindValidationFailed, "managed MySQL execution request is invalid")
}

// The command surface is fixed here. Values are passed as positional argv only
// after the Go validator has reduced them to generated identifiers, decimal
// integers, or lowercase hexadecimal digests.
const closedScript = `
op="$1"; nonce="$2"; deadline="$3"; shift 3
root=/tmp/.groundplane-mysql84
state="$root/$nonce"
umask 077
mkdir -p "$root"
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
case "$op" in
  probe-mysql) command -v mysql >/dev/null ;;
  probe-mysqldump) command -v mysqldump >/dev/null ;;
  server-version) exec mysql --no-defaults --protocol=socket --user=root --batch --skip-column-names --database="$1" --execute='SELECT VERSION()' ;;
  tool-version) exec mysqldump --version ;;
  restore-tool-version) exec mysql --version ;;
  zero-connections)
    db="$1"
    count=$(mysql --no-defaults --protocol=socket --user=root --batch --skip-column-names --database=mysql --execute="SELECT COUNT(*) FROM information_schema.processlist WHERE DB = '$db' AND ID <> CONNECTION_ID()")
    test "$count" = 0
    ;;
  dump)
    db="$1"; role="$2"; maximum="$3"
    test ! -e "$state"
    mkdir "$state"
    printf '%s\n' dump >"$state/operation"
    : >"$state/active"; trap 'rm -f "$state/active"' EXIT
    mysqldump --no-defaults --protocol=socket --user=root --single-transaction --quick --skip-lock-tables --set-gtid-purged=OFF --no-tablespaces --routines --events --triggers --hex-blob --databases "$db" >"$state/source"
    size=$(wc -c <"$state/source"); test "$size" -gt 0; test "$size" -le "$maximum"
    sha=$(sha256sum "$state/source"); sha=${sha%% *}
    printf '%s\n%s\n' "$size" "$sha" >"$state/evidence"
    cat "$state/source"
    ;;
  restore-apply)
    db="$1"; role="$2"; expected_size="$3"; expected_sha="$4"
    test ! -e "$state"
    mkdir "$state"
    printf '%s\n' restore-apply >"$state/operation"
    : >"$state/active"; trap 'rm -f "$state/active"' EXIT
    cat >"$state/source"
    size=$(wc -c <"$state/source"); test "$size" = "$expected_size"
    sha=$(sha256sum "$state/source"); sha=${sha%% *}; test "$sha" = "$expected_sha"
    mysql --no-defaults --protocol=socket --user=root --database=mysql --execute="DROP DATABASE IF EXISTS $db; CREATE DATABASE $db;"
    mysql --no-defaults --protocol=socket --user=root --database="$db" <"$state/source"
    mysql --no-defaults --protocol=socket --user=root --batch --skip-column-names --database=mysql --execute="SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '$db'" | grep -Fx "$db" >/dev/null
    printf '%s\n%s\n' "$size" "$sha" >"$state/evidence"
    ;;
  post-restore-verify)
    db="$1"
    mysqlcheck --no-defaults --protocol=socket --user=root --silent --check --databases "$db" >/dev/null
    exec mysql --no-defaults --protocol=socket --user=root --batch --skip-column-names --database=mysql --execute="SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '$db'"
    ;;
  recover-dump)
    maximum="$1"; test -f "$state/source"; test -f "$state/evidence"
    size=$(wc -c <"$state/source"); test "$size" -gt 0; test "$size" -le "$maximum"
    sha=$(sha256sum "$state/source"); sha=${sha%% *}
    test "$(sed -n '1p' "$state/evidence")" = "$size"
    test "$(sed -n '2p' "$state/evidence")" = "$sha"
    exec cat "$state/source"
    ;;
  recover-evidence) test -f "$state/evidence"; exec cat "$state/evidence" ;;
  recovery-inventory)
    for candidate in "$root"/*; do
      test -d "$candidate" || continue
      name=${candidate##*/}
      case "$name" in *[!0-9a-f]*|'') exit 65 ;; esac
      test "${#name}" = 64 || exit 65
      operation=$(cat "$candidate/operation")
      case "$operation" in dump|restore-apply) ;; *) exit 65 ;; esac
      if test -f "$candidate/active"; then active=1; else active=0; fi
      if test -f "$candidate/evidence"; then evidence=1; else evidence=0; fi
      printf '%s %s %s %s\n' "$name" "$operation" "$active" "$evidence"
    done
    ;;
  retire) rm -rf -- "$state" ;;
  *) exit 64 ;;
esac
`
