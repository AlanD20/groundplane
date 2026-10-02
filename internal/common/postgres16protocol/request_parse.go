package postgres16protocol

import (
	"encoding/hex"
	"strconv"

	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

func parseRunArguments(arguments []string) (Request, error) {
	if len(arguments) < 5 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper run header is invalid")
	}
	nonce, err := ParseNonce(arguments[2])
	if err != nil {
		return Request{}, err
	}
	deadline, err := parsePositiveUint(arguments[3])
	if err != nil {
		return Request{}, err
	}
	operation, err := parseRunOperation(arguments[4])
	if err != nil {
		return Request{}, err
	}
	request := Request{Operation: operation, Nonce: nonce, DeadlineUnixNano: deadline}
	suffix := arguments[5:]
	switch operation {
	case OperationProbePGDump, OperationProbePGRestore, OperationProbePSQL:
		if len(suffix) != 1 || suffix[0] != strconv.FormatUint(uint64(PostgreSQLMajor), 10) {
			return Request{}, invalid("postgres helper probe version is invalid")
		}
	case OperationServerMajor:
		if len(suffix) != 2 || suffix[0] != strconv.FormatUint(uint64(PostgreSQLMajor), 10) {
			return Request{}, invalid("postgres helper server-major suffix is invalid")
		}
		request.Database = suffix[1]
	case OperationDump:
		if len(suffix) != 2 {
			return Request{}, invalid("postgres helper dump suffix is invalid")
		}
		request.Database, request.Role = suffix[0], suffix[1]
	case OperationRestoreList:
		if len(suffix) != 2 {
			return Request{}, invalid("postgres helper restore-list suffix is invalid")
		}
		request.SourceSize, err = parseCanonicalUint(suffix[0])
		if err == nil {
			request.SourceSHA256, err = ParseDigest(suffix[1])
		}
	case OperationTerminateDBConnections, OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		if len(suffix) != 1 {
			return Request{}, invalid("postgres helper database suffix is invalid")
		}
		request.Database = suffix[0]
	case OperationRestoreApply:
		if len(suffix) != 4 {
			return Request{}, invalid("postgres helper restore-apply suffix is invalid")
		}
		request.SourceSize, err = parseCanonicalUint(suffix[0])
		if err == nil {
			request.SourceSHA256, err = ParseDigest(suffix[1])
		}
		request.Database, request.Role = suffix[2], suffix[3]
	default:
		return Request{}, invalid("postgres helper run operation is invalid")
	}
	if err != nil {
		return Request{}, err
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}

func parseStopArguments(arguments []string) (Request, error) {
	if len(arguments) != 4 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper stop suffix is invalid")
	}
	nonce, err := ParseNonce(arguments[2])
	if err != nil {
		return Request{}, err
	}
	deadline, err := parsePositiveUint(arguments[3])
	if err != nil {
		return Request{}, err
	}
	request := Request{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: deadline}
	return request, request.Validate()
}

func parseRecoveryInventoryArguments(arguments []string) (Request, error) {
	if len(arguments) != 4 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper recovery inventory arguments are invalid")
	}
	nonce, err := ParseNonce(arguments[2])
	if err != nil {
		return Request{}, err
	}
	deadline, err := parsePositiveUint(arguments[3])
	if err != nil {
		return Request{}, err
	}
	request := Request{Operation: OperationRecoveryInventory, Nonce: nonce, DeadlineUnixNano: deadline}
	return request, request.Validate()
}

func parseRunOperation(value string) (Operation, error) {
	for operation := OperationProbePGDump; operation <= OperationPostRestoreVerify; operation++ {
		if operation.String() == value {
			return operation, nil
		}
	}
	return 0, invalid("postgres helper run operation is invalid")
}

func parsePositiveUint(value string) (uint64, error) {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return 0, invalid("postgres helper integer is not canonical")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, invalid("postgres helper integer is invalid")
	}
	return parsed, nil
}

func parseCanonicalUint(value string) (uint64, error) {
	if value == "0" {
		return 0, nil
	}
	return parsePositiveUint(value)
}

func parseLowerHex(value string, size int) ([]byte, error) {
	if len(value) != size*2 {
		return nil, invalid("postgres helper hexadecimal value has invalid length")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return nil, invalid("postgres helper hexadecimal value is not canonical")
	}
	return decoded, nil
}

func invalid(message string) error {
	return grounderrs.New(grounderrs.KindValidationFailed, message)
}
