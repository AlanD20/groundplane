package operationmanifest

import (
	"fmt"
	"sort"
	"strings"
)

func validMethod(method string) bool {
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
		return true
	default:
		return false
	}
}

func validSuccessStatus(method string, status int) bool {
	switch method {
	case "GET":
		return status == 200
	case "POST":
		return status == 200 || status == 201 || status == 202
	case "PUT":
		return status == 200 || status == 202
	case "PATCH":
		return status == 200
	case "DELETE":
		return status == 202 || status == 204
	default:
		return false
	}
}

func validOperationIdentity(value string) bool {
	noun, verb, found := strings.Cut(value, ".")
	return found && !strings.Contains(verb, ".") && validLiteralSegment(noun) &&
		validLiteralSegment(verb)
}

func acceptedExemptionClass(leaf CLILeaf) (ExemptionClass, bool) {
	switch cliKey(leaf) {
	case "controller serve", "agent-run run":
		return ExemptionLocalProcess, true
	case "controller key show", "controller etcd show":
		return ExemptionLocalDiagnostic, true
	case "version", "completion bash", "completion zsh", "completion fish":
		return ExemptionLocalTooling, true
	default:
		return "", false
	}
}

func validPath(path string) bool {
	if len(path) < 2 || path[0] != '/' || path[len(path)-1] == '/' || strings.Contains(path, "//") {
		return false
	}
	for index := range len(path) {
		char := path[index]
		if char < 0x20 || char == 0x7f || char == '\\' || char == '%' || char == '?' ||
			char == '#' {
			return false
		}
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if !validLiteralSegment(segment) && !validBindingSegment(segment) {
			return false
		}
	}
	return true
}

func validLiteralSegment(segment string) bool {
	if segment == "" || !isLowerLetter(segment[0]) {
		return false
	}
	previousHyphen := false
	for index := range len(segment) {
		char := segment[index]
		switch {
		case isLowerLetter(char) || isDigit(char):
			previousHyphen = false
		case char == '-' && index > 0 && index < len(segment)-1 && !previousHyphen:
			previousHyphen = true
		default:
			return false
		}
	}
	return true
}

func validBindingSegment(segment string) bool {
	if len(segment) < 3 || segment[0] != '{' || segment[len(segment)-1] != '}' {
		return false
	}
	name := segment[1 : len(segment)-1]
	if name == "" || !isLowerLetter(name[0]) {
		return false
	}
	previousUnderscore := false
	for index := range len(name) {
		char := name[index]
		switch {
		case isLowerLetter(char) || isDigit(char):
			previousUnderscore = false
		case char == '_' && index > 0 && index < len(name)-1 && !previousUnderscore:
			previousUnderscore = true
		default:
			return false
		}
	}
	return true
}

func isLowerLetter(char byte) bool {
	return char >= 'a' && char <= 'z'
}

func isDigit(char byte) bool {
	return char >= '0' && char <= '9'
}

func cliKey(leaf CLILeaf) string {
	return strings.Join(leaf.Path, " ")
}

func apiKey(operation APIContract) string {
	return operation.Method + " " + operation.Path
}

func payloadKey(payload Payload) string {
	if payload.Kind == PayloadNone {
		return string(PayloadNone)
	}
	return string(payload.Kind) + ":" + payload.Schema
}

func errorKey(contract ErrorContract) string {
	return fmt.Sprintf("%s/%d", quoted(string(contract.Code)), contract.Status)
}

func sameErrors(left, right []ErrorContract) bool {
	if len(left) != len(right) {
		return false
	}
	leftCounts := make(map[string]int, len(left))
	for _, contract := range left {
		leftCounts[errorKey(contract)]++
	}
	for _, contract := range right {
		key := errorKey(contract)
		leftCounts[key]--
		if leftCounts[key] < 0 {
			return false
		}
	}
	for _, count := range leftCounts {
		if count != 0 {
			return false
		}
	}
	return true
}

func formatErrors(contracts []ErrorContract) string {
	values := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		values = append(values, errorKey(contract))
	}
	sort.Strings(values)
	return "[" + strings.Join(values, ", ") + "]"
}

func quoted(value string) string {
	return fmt.Sprintf("%q", value)
}
