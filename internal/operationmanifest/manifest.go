// Package operationmanifest defines the executable parity contract shared by
// Console, CLI, and human API contract tests.
//
// This package is only the validation foundation. It is not a complete parity
// proof until the concrete product inventory and independent Console, Cobra,
// and OpenAPI captures are wired into CI.
package operationmanifest

import (
	"fmt"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Manifest is the complete set of parity operations and closed exemptions.
type Manifest struct {
	Entries []Entry
}

// Entry is a closed union. Exactly one branch must be set.
type Entry struct {
	ID        string
	Operation *Operation
	Exemption *Exemption
}

// Operation binds exactly one identity on each operator-facing surface.
type Operation struct {
	Console ConsoleAction
	CLI     CLILeaf
	API     APIContract
}

type ConsoleAction struct {
	ID string
}

// CLILeaf contains canonical command names and excludes the executable name
// and aliases.
type CLILeaf struct {
	Path []string
}

// APIContract carries the generation-relevant shape beyond method and path.
type APIContract struct {
	OperationID   string
	Method        string
	Path          string
	Request       Payload
	Response      Payload
	SuccessStatus int
	Errors        []ErrorContract
}

// PayloadKind makes an absent payload explicit instead of overloading an empty
// schema name. The catalog is the set used by the MVP human API.
type PayloadKind string

const (
	PayloadNone        PayloadKind = "none"
	PayloadJSON        PayloadKind = "json"
	PayloadMultipart   PayloadKind = "multipart"
	PayloadEventStream PayloadKind = "event_stream"
)

type Payload struct {
	Kind   PayloadKind
	Schema string
}

// ErrorContract is one accepted public code/status tuple. A code may have
// multiple statuses only when the closed errs catalog declares each tuple.
type ErrorContract struct {
	Code   errs.Code
	Status int
}

// ExemptionClass is the closed ADR 0006 exception vocabulary.
type ExemptionClass string

const (
	ExemptionLocalProcess    ExemptionClass = "local_process"
	ExemptionLocalDiagnostic ExemptionClass = "local_diagnostic"
	ExemptionLocalTooling    ExemptionClass = "local_tooling"
)

// Exemption records why one canonical CLI leaf has no Console or API peer.
type Exemption struct {
	CLI    CLILeaf
	Class  ExemptionClass
	Reason string
}

// Validate proves that every declared surface is captured exactly once, every
// captured surface is declared, and the captured API contract matches deeply.
func Validate(manifest Manifest, capture Capture) error {
	validator := newValidator(capture)
	validator.validateManifest(manifest)
	validator.validateCapture()
	if len(validator.issues) == 0 {
		return nil
	}

	sort.Strings(validator.issues)
	return errs.New(
		errs.KindInternal,
		"invalid operation manifest: "+strings.Join(validator.issues, "; "),
	)
}

type validator struct {
	issues []string

	entryIDs        map[string]int
	expectedConsole map[string]int
	expectedCLI     map[string]int
	expectedAPI     map[string][]APIContract
	expectedAPIIDs  map[string]int

	capturedConsole map[string]int
	capturedCLI     map[string]int
	capturedAPI     map[string][]APIContract
	capturedAPIIDs  map[string]int
}

func newValidator(capture Capture) *validator {
	value := &validator{
		entryIDs:        make(map[string]int),
		expectedConsole: make(map[string]int),
		expectedCLI:     make(map[string]int),
		expectedAPI:     make(map[string][]APIContract),
		expectedAPIIDs:  make(map[string]int),
		capturedConsole: make(map[string]int),
		capturedCLI:     make(map[string]int),
		capturedAPI:     make(map[string][]APIContract),
		capturedAPIIDs:  make(map[string]int),
	}
	if len(capture.Console) == 0 && len(capture.CLI) == 0 && len(capture.API) == 0 {
		value.add("capture must not be empty")
	}
	for _, action := range capture.Console {
		value.capturedConsole[action.ID]++
		value.validateOperationIdentity("captured console action id", action.ID)
	}
	for _, leaf := range capture.CLI {
		value.capturedCLI[cliKey(leaf)]++
		value.validateCLI("captured cli leaf", leaf)
	}
	for _, operation := range capture.API {
		key := apiKey(operation)
		value.capturedAPI[key] = append(value.capturedAPI[key], operation)
		value.capturedAPIIDs[operation.OperationID]++
		value.validateAPI("captured api endpoint "+quoted(key), operation)
	}
	return value
}

func (v *validator) validateManifest(manifest Manifest) {
	if len(manifest.Entries) == 0 {
		v.add("manifest must not be empty")
	}
	for index := range manifest.Entries {
		entry := manifest.Entries[index]
		v.entryIDs[entry.ID]++
		v.validateIdentifier(fmt.Sprintf("entry %d id", index), entry.ID)

		if (entry.Operation == nil) == (entry.Exemption == nil) {
			v.add("entry %s must define exactly one of operation or exemption", quoted(entry.ID))
			continue
		}
		if entry.Operation != nil {
			v.validateOperation(entry.ID, *entry.Operation)
			continue
		}
		v.validateExemption(entry.ID, *entry.Exemption)
	}

	for id, count := range v.entryIDs {
		if count > 1 {
			v.add("entry id %s is declared %d times", quoted(id), count)
		}
	}
	for id, count := range v.expectedConsole {
		if count > 1 {
			v.add("console action %s is mapped by %d entries", quoted(id), count)
		}
	}
	for key, count := range v.expectedCLI {
		if count > 1 {
			v.add("cli leaf %s is mapped by %d entries", quoted(key), count)
		}
	}
	for key, operations := range v.expectedAPI {
		if len(operations) > 1 {
			v.add("api endpoint %s is mapped by %d entries", quoted(key), len(operations))
		}
	}
	for id, count := range v.expectedAPIIDs {
		if count > 1 {
			v.add("api operation id %s is mapped by %d entries", quoted(id), count)
		}
	}
}

func (v *validator) validateOperation(entryID string, operation Operation) {
	v.validateOperationIdentity("entry "+quoted(entryID)+" console action id", operation.Console.ID)
	v.validateCLI("entry "+quoted(entryID)+" cli leaf", operation.CLI)
	v.validateAPI("entry "+quoted(entryID)+" api endpoint", operation.API)
	if operation.Console.ID != operation.API.OperationID {
		v.add(
			"entry %s console action id %s must equal api operation id %s",
			quoted(entryID),
			quoted(operation.Console.ID),
			quoted(operation.API.OperationID),
		)
	}

	v.expectedConsole[operation.Console.ID]++
	v.expectedCLI[cliKey(operation.CLI)]++
	key := apiKey(operation.API)
	v.expectedAPI[key] = append(v.expectedAPI[key], operation.API)
	v.expectedAPIIDs[operation.API.OperationID]++
}

func (v *validator) validateExemption(entryID string, exemption Exemption) {
	v.validateCLI("entry "+quoted(entryID)+" exemption cli leaf", exemption.CLI)
	acceptedClass, ok := acceptedExemptionClass(exemption.CLI)
	if !ok {
		v.add(
			"entry %s exemption cli leaf %s is not accepted by ADR 0006",
			quoted(entryID),
			quoted(cliKey(exemption.CLI)),
		)
	} else if exemption.Class != acceptedClass {
		v.add(
			"entry %s exemption cli leaf %s requires class %s, got %s",
			quoted(entryID),
			quoted(cliKey(exemption.CLI)),
			quoted(string(acceptedClass)),
			quoted(string(exemption.Class)),
		)
	}
	if strings.TrimSpace(exemption.Reason) == "" {
		v.add("entry %s exemption reason must not be empty", quoted(entryID))
	}
	v.expectedCLI[cliKey(exemption.CLI)]++
}

func (v *validator) validateCapture() {
	v.compareCounts("console action", v.expectedConsole, v.capturedConsole)
	v.compareCounts("cli leaf", v.expectedCLI, v.capturedCLI)
	for id, count := range v.capturedAPIIDs {
		if count > 1 {
			v.add("captured api operation id %s occurs %d times", quoted(id), count)
		}
	}

	for key, captured := range v.capturedAPI {
		if len(captured) > 1 {
			v.add("captured api endpoint %s occurs %d times", quoted(key), len(captured))
		}
		if len(v.expectedAPI[key]) == 0 {
			v.add("captured api endpoint %s is not in the manifest", quoted(key))
		}
	}
	for key, expected := range v.expectedAPI {
		captured := v.capturedAPI[key]
		if len(captured) == 0 {
			v.add("manifest api endpoint %s was not captured", quoted(key))
			continue
		}
		if len(expected) == 1 && len(captured) == 1 {
			v.compareAPI(key, expected[0], captured[0])
		}
	}
}

func (v *validator) compareCounts(surface string, expected, captured map[string]int) {
	for key, count := range captured {
		if count > 1 {
			v.add("captured %s %s occurs %d times", surface, quoted(key), count)
		}
		if expected[key] == 0 {
			v.add("captured %s %s is not in the manifest", surface, quoted(key))
		}
	}
	for key := range expected {
		if captured[key] == 0 {
			v.add("manifest %s %s was not captured", surface, quoted(key))
		}
	}
}

func (v *validator) compareAPI(key string, expected, captured APIContract) {
	if captured.OperationID != expected.OperationID {
		v.add(
			"api endpoint %s operation id = %s, want %s",
			quoted(key),
			quoted(captured.OperationID),
			quoted(expected.OperationID),
		)
	}
	if captured.Request != expected.Request {
		v.add(
			"api endpoint %s request payload = %s, want %s",
			quoted(key),
			quoted(payloadKey(captured.Request)),
			quoted(payloadKey(expected.Request)),
		)
	}
	if captured.Response != expected.Response {
		v.add(
			"api endpoint %s response payload = %s, want %s",
			quoted(key),
			quoted(payloadKey(captured.Response)),
			quoted(payloadKey(expected.Response)),
		)
	}
	if captured.SuccessStatus != expected.SuccessStatus {
		v.add(
			"api endpoint %s success status = %d, want %d",
			quoted(key),
			captured.SuccessStatus,
			expected.SuccessStatus,
		)
	}
	if !sameErrors(captured.Errors, expected.Errors) {
		v.add(
			"api endpoint %s stable errors = %s, want %s",
			quoted(key),
			formatErrors(captured.Errors),
			formatErrors(expected.Errors),
		)
	}
}

func (v *validator) validateAPI(label string, operation APIContract) {
	v.validateOperationIdentity(label+" operation id", operation.OperationID)
	if !validMethod(operation.Method) {
		v.add("%s method %s is not in the human API method set", label, quoted(operation.Method))
	}
	if !validPath(operation.Path) {
		v.add(
			"%s path %s is not a canonical human API route template",
			label,
			quoted(operation.Path),
		)
	}
	if !validSuccessStatus(operation.Method, operation.SuccessStatus) {
		v.add(
			"%s success status %d is not valid for method %s",
			label,
			operation.SuccessStatus,
			quoted(operation.Method),
		)
	}
	v.validatePayload(label+" request", operation.Request)
	v.validatePayload(label+" response", operation.Response)
	if operation.Request.Kind == PayloadEventStream {
		v.add("%s request payload kind event_stream is response-only", label)
	}
	if operation.Response.Kind == PayloadMultipart {
		v.add("%s response payload kind multipart is request-only", label)
	}
	v.validateSuccessResponse(label, operation)
	v.validateErrors(label, operation.Errors)
}

func (v *validator) validateSuccessResponse(label string, operation APIContract) {
	switch operation.SuccessStatus {
	case 200:
		if operation.Response.Kind == PayloadNone {
			v.add("%s success status 200 requires a response representation", label)
		}
	case 201:
		if operation.Response.Kind != PayloadJSON {
			v.add("%s success status 201 requires a json resource response", label)
		}
	case 202:
		want := Payload{Kind: PayloadJSON, Schema: "TaskAccepted"}
		if operation.Response != want {
			v.add(
				"%s success status 202 requires response payload %s",
				label,
				quoted(payloadKey(want)),
			)
		}
	case 204, 205:
		if operation.Response.Kind != PayloadNone {
			v.add(
				"%s success status %d requires response payload kind none",
				label,
				operation.SuccessStatus,
			)
		}
	}
}

func (v *validator) validatePayload(label string, payload Payload) {
	switch payload.Kind {
	case PayloadNone:
		if payload.Schema != "" {
			v.add("%s payload kind none must not declare schema %s", label, quoted(payload.Schema))
		}
	case PayloadJSON, PayloadMultipart, PayloadEventStream:
		v.validateIdentifier(label+" payload schema", payload.Schema)
	default:
		v.add("%s payload kind %s is not supported", label, quoted(string(payload.Kind)))
	}
}

func (v *validator) validateErrors(label string, contracts []ErrorContract) {
	if len(contracts) == 0 {
		v.add("%s must declare at least one stable error", label)
		return
	}
	counts := make(map[string]int, len(contracts))
	for _, contract := range contracts {
		key := errorKey(contract)
		counts[key]++
		_, ok := errs.FromProblem(errs.Problem{
			Type:   errs.ProblemType,
			Status: contract.Status,
			Code:   contract.Code,
		})
		if !ok {
			v.add(
				"%s error %s/%d is not in the stable error catalog",
				label,
				quoted(string(contract.Code)),
				contract.Status,
			)
		}
	}
	for key, count := range counts {
		if count > 1 {
			v.add("%s error %s is declared %d times", label, key, count)
		}
	}
}

func (v *validator) validateCLI(label string, leaf CLILeaf) {
	if len(leaf.Path) == 0 {
		v.add("%s path must not be empty", label)
		return
	}
	for index, segment := range leaf.Path {
		v.validateIdentifier(fmt.Sprintf("%s segment %d", label, index), segment)
	}
}

func (v *validator) validateIdentifier(label, value string) {
	if value == "" || value != strings.TrimSpace(value) || strings.ContainsAny(value, " \t\r\n") {
		v.add("%s %s must be non-empty and contain no whitespace", label, quoted(value))
	}
}

func (v *validator) validateOperationIdentity(label, value string) {
	if !validOperationIdentity(value) {
		v.add("%s %s must be canonical noun.verb", label, quoted(value))
	}
}

func (v *validator) add(format string, args ...any) {
	v.issues = append(v.issues, fmt.Sprintf(format, args...))
}

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
