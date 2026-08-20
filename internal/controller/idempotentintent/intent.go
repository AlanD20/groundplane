// Package idempotentintent canonicalizes validated human-API mutations into
// opaque, sensitive digests. It never reads HTTP transport state and does not
// own durable idempotency markers or replay behavior.
package idempotentintent

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const domain = "groundplane-http-intent"

// Version identifies the immutable canonical intent encoding.
type Version uint8

const Version1 Version = 1

// ScopeKind is the closed durable-owner vocabulary for human mutations.
type ScopeKind string

const (
	ScopePlatform    ScopeKind = "platform"
	ScopeTenant      ScopeKind = "tenant"
	ScopeProject     ScopeKind = "project"
	ScopeEnvironment ScopeKind = "environment"
)

// ContentKind replaces intent-affecting Content-Type text after transport
// validation.
type ContentKind string

const (
	ContentNone              ContentKind = "none"
	ContentJSON              ContentKind = "json"
	ContentBlueprintBundleV1 ContentKind = "blueprint-bundle-v1"
)

// Scope identifies the durable owner after slug resolution.
type Scope struct {
	Kind ScopeKind
	ID   string
}

// PathBinding is one decoded, validated route binding in route order.
type PathBinding struct {
	Name  string
	Value string
}

// Field is one member of a typed canonical object.
type Field struct {
	Name  string
	Value Value
}

type valueKind uint8

const (
	valueInvalid valueKind = 0x00
	valueNull    valueKind = 0x01
	valueFalse   valueKind = 0x02
	valueTrue    valueKind = 0x03
	valueInteger valueKind = 0x04
	valueString  valueKind = 0x05
	valueList    valueKind = 0x06
	valueObject  valueKind = 0x07
	valueSHA256  valueKind = 0x08
)

// Value is the closed canonical value tree accepted after route validation.
// Its fields are private so callers cannot invent another scalar kind.
type Value struct {
	kind    valueKind
	integer string
	text    string
	items   []Value
	fields  []Field
	digest  [sha256.Size]byte
}

func Null() Value { return Value{kind: valueNull} }

func Bool(value bool) Value {
	if value {
		return Value{kind: valueTrue}
	}
	return Value{kind: valueFalse}
}

func Integer(value int64) Value {
	return Value{kind: valueInteger, integer: strconv.FormatInt(value, 10)}
}

func UnsignedInteger(value uint64) Value {
	return Value{kind: valueInteger, integer: strconv.FormatUint(value, 10)}
}

func String(value string) Value { return Value{kind: valueString, text: value} }

func List(items ...Value) Value {
	return Value{kind: valueList, items: append([]Value(nil), items...)}
}

func Object(fields ...Field) Value {
	return Value{kind: valueObject, fields: append([]Field(nil), fields...)}
}

// SHA256Value is reserved for verified Blueprint file digests. JSON bodies
// cannot contain it.
func SHA256Value(value [sha256.Size]byte) Value {
	return Value{kind: valueSHA256, digest: value}
}

type bodyKind uint8

const (
	bodyInvalid bodyKind = iota
	bodyNone
	bodyJSON
	bodyBlueprint
)

// Body is the closed union of accepted mutation body kinds.
type Body struct {
	kind      bodyKind
	json      Value
	blueprint BlueprintManifestV1
}

func NoBody() Body { return Body{kind: bodyNone} }

func JSONBody(value Value) Body { return Body{kind: bodyJSON, json: value} }

func BlueprintBody(manifest BlueprintManifestV1) Body {
	return Body{kind: bodyBlueprint, blueprint: manifest}
}

// BlueprintManifestV1 contains only the verified, boundary-independent
// closed-bundle manifest. It contains no raw multipart framing or file bytes.
type BlueprintManifestV1 struct {
	FormatVersion  uint8
	RootPath       string
	ComposeSources []string
	Interpolation  []Interpolation
	Files          []BlueprintFile
}

type Interpolation struct {
	Name  string
	Value string
}

type BlueprintFile struct {
	Path   string
	Part   string
	Size   uint64
	SHA256 [sha256.Size]byte
}

// CanonicalIntentV1 is constructed only from decoded and validated route
// values. Query must be an object whose route defaults have already applied.
type CanonicalIntentV1 struct {
	Method string
	Route  string
	Scope  Scope
	Path   []PathBinding
	Query  Value
	Body   Body
}

// Digest owns one sensitive digest. It has no raw accessor and may be consumed
// exactly once by Protect or Compare.
type Digest struct {
	state *digestState
}

type digestState struct {
	mu       sync.Mutex
	value    [sha256.Size]byte
	consumed bool
}

// Format deliberately prevents fmt from exposing the private digest bytes,
// including when a caller copies or dereferences the handle.
func (Digest) Format(state fmt.State, _ rune) {
	// fmt.State.Write reports only the underlying formatter failure, which the
	// Format interface cannot return to its caller.
	_, _ = state.Write([]byte("[sensitive idempotent intent digest]"))
}

// MarshalJSON prevents accidental durable or public serialization.
func (Digest) MarshalJSON() ([]byte, error) {
	return nil, errs.New(errs.KindInternal, "idempotent intent digest cannot be marshaled")
}

// MarshalText prevents accidental key, log, or text serialization.
func (Digest) MarshalText() ([]byte, error) {
	return nil, errs.New(errs.KindInternal, "idempotent intent digest cannot be marshaled")
}

// Destroy idempotently clears an abandoned or no-longer-needed digest. All
// copies of the handle share the same private state.
func (digest *Digest) Destroy() {
	if digest == nil || digest.state == nil {
		return
	}
	digest.state.mu.Lock()
	defer digest.state.mu.Unlock()
	clear(digest.state.value[:])
	digest.state.consumed = true
}

// Canonicalize returns the immutable encoding version and one caller-owned,
// one-use sensitive digest.
func Canonicalize(ctx context.Context, intent CanonicalIntentV1) (Version, *Digest, error) {
	if ctx == nil {
		return 0, nil, internalError("context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if err := validateIntent(intent); err != nil {
		return 0, nil, err
	}

	encoder := canonicalEncoder{}
	defer encoder.clear()
	encoder.writeRawString(domain)
	encoder.writeRawBytes([]byte{0, byte(Version1)})
	encoder.writeString(intent.Method)
	encoder.writeString(intent.Route)
	encoder.writeString(string(intent.Scope.Kind))
	encoder.writeString(intent.Scope.ID)
	encoder.writeListHeader(len(intent.Path))
	for _, binding := range intent.Path {
		encoder.writeObjectHeader(2)
		encoder.writeString("name")
		encoder.writeString(binding.Name)
		encoder.writeString("value")
		encoder.writeString(binding.Value)
	}
	encoder.writeValue(intent.Query)

	switch intent.Body.kind {
	case bodyNone:
		encoder.writeString(string(ContentNone))
		encoder.writeNull()
	case bodyJSON:
		encoder.writeString(string(ContentJSON))
		encoder.writeValue(intent.Body.json)
	case bodyBlueprint:
		encoder.writeString(string(ContentBlueprintBundleV1))
		encoder.writeValue(blueprintValue(intent.Body.blueprint))
	default:
		return 0, nil, internalError("body kind is invalid")
	}

	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	sum := sha256.Sum256(encoder.buffer)
	defer clear(sum[:])
	digest := &Digest{state: &digestState{value: sum}}
	return Version1, digest, nil
}

func validateIntent(intent CanonicalIntentV1) error {
	switch intent.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return internalError("method is not a registered mutation method")
	}
	bindings, valid := routeBindings(intent.Route)
	if !valid {
		return internalError("route template is invalid")
	}
	if err := validateScope(intent.Scope); err != nil {
		return err
	}
	if len(intent.Path) != len(bindings) {
		return internalError("path bindings do not match the route template")
	}
	for index, binding := range intent.Path {
		if !validName(binding.Name) || !validText(binding.Value) || binding.Value == "" {
			return internalError("path binding is invalid")
		}
		if binding.Name != bindings[index] {
			return internalError("path bindings do not match the route template")
		}
	}
	if intent.Query.kind != valueObject {
		return internalError("query must be a canonical object")
	}
	if err := validateValue(intent.Query, false); err != nil {
		return err
	}
	switch intent.Body.kind {
	case bodyNone:
	case bodyJSON:
		return validateValue(intent.Body.json, false)
	case bodyBlueprint:
		return validateBlueprint(intent.Body.blueprint)
	default:
		return internalError("body kind is invalid")
	}
	return nil
}

func validateScope(scope Scope) error {
	if scope.Kind == ScopePlatform {
		if scope.ID != "" {
			return internalError("platform scope must not have an id")
		}
		return nil
	}
	var kind ids.Kind
	switch scope.Kind {
	case ScopeTenant:
		kind = ids.KindTenant
	case ScopeProject:
		kind = ids.KindProject
	case ScopeEnvironment:
		kind = ids.KindEnvironment
	default:
		return internalError("scope kind is invalid")
	}
	if err := ids.Validate(kind, scope.ID); err != nil {
		return internalError("scope id is invalid")
	}
	return nil
}

func validateBlueprint(manifest BlueprintManifestV1) error {
	if manifest.FormatVersion != 1 || !validText(manifest.RootPath) || manifest.RootPath == "" {
		return internalError("Blueprint manifest identity is invalid")
	}
	if len(manifest.ComposeSources) == 0 || manifest.ComposeSources[0] != manifest.RootPath {
		return internalError("Blueprint Compose sources are invalid")
	}
	filePaths := make(map[string]struct{}, len(manifest.Files))
	previousPath := ""
	for index, file := range manifest.Files {
		if !validText(file.Path) || file.Path == "" || (index > 0 && file.Path <= previousPath) {
			return internalError("Blueprint file order is invalid")
		}
		if file.Part != fmt.Sprintf("file-%06d", index+1) {
			return internalError("Blueprint file part identity is invalid")
		}
		filePaths[file.Path] = struct{}{}
		previousPath = file.Path
	}
	if len(filePaths) == 0 {
		return internalError("Blueprint files are required")
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if _, exists := filePaths[source]; !exists {
			return internalError("Blueprint Compose source is undeclared")
		}
		if _, exists := seenSources[source]; exists {
			return internalError("Blueprint Compose source is duplicated")
		}
		seenSources[source] = struct{}{}
	}
	seenVariables := make(map[string]struct{}, len(manifest.Interpolation))
	for _, variable := range manifest.Interpolation {
		if !validInterpolationName(variable.Name) || !validText(variable.Value) {
			return internalError("Blueprint interpolation is invalid")
		}
		if _, exists := seenVariables[variable.Name]; exists {
			return internalError("Blueprint interpolation is duplicated")
		}
		seenVariables[variable.Name] = struct{}{}
	}
	return nil
}

func validateValue(value Value, allowSHA256 bool) error {
	switch value.kind {
	case valueNull, valueFalse, valueTrue:
		return nil
	case valueInteger:
		if value.integer == "" {
			return internalError("integer value is invalid")
		}
		return nil
	case valueString:
		if !validText(value.text) {
			return internalError("string value is invalid")
		}
		return nil
	case valueList:
		for _, item := range value.items {
			if err := validateValue(item, allowSHA256); err != nil {
				return err
			}
		}
		return nil
	case valueObject:
		seen := make(map[string]struct{}, len(value.fields))
		for _, field := range value.fields {
			if !validText(field.Name) || field.Name == "" {
				return internalError("object member name is invalid")
			}
			if _, exists := seen[field.Name]; exists {
				return internalError("object member is duplicated")
			}
			seen[field.Name] = struct{}{}
			if err := validateValue(field.Value, allowSHA256); err != nil {
				return err
			}
		}
		return nil
	case valueSHA256:
		if allowSHA256 {
			return nil
		}
		return internalError("SHA-256 value is not allowed here")
	default:
		return internalError("canonical value kind is invalid")
	}
}

func blueprintValue(manifest BlueprintManifestV1) Value {
	variables := append([]Interpolation(nil), manifest.Interpolation...)
	sort.Slice(variables, func(left, right int) bool { return variables[left].Name < variables[right].Name })
	variableFields := make([]Field, 0, len(variables))
	for _, variable := range variables {
		variableFields = append(variableFields, Field{Name: variable.Name, Value: String(variable.Value)})
	}
	sources := make([]Value, 0, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		sources = append(sources, String(source))
	}
	files := make([]Value, 0, len(manifest.Files))
	for _, file := range manifest.Files {
		files = append(files, Object(
			Field{Name: "path", Value: String(file.Path)},
			Field{Name: "part", Value: String(file.Part)},
			Field{Name: "size", Value: UnsignedInteger(file.Size)},
			Field{Name: "sha256", Value: SHA256Value(file.SHA256)},
		))
	}
	return Object(
		Field{Name: "format_version", Value: UnsignedInteger(uint64(manifest.FormatVersion))},
		Field{Name: "root_path", Value: String(manifest.RootPath)},
		Field{Name: "compose_sources", Value: List(sources...)},
		Field{Name: "interpolation", Value: Object(variableFields...)},
		Field{Name: "files", Value: List(files...)},
	)
}

func routeBindings(value string) ([]string, bool) {
	if value == "" || value == "/" || !strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") ||
		strings.Contains(value, "//") || strings.ContainsAny(value, "\\?#%") {
		return nil, false
	}
	bindings := make([]string, 0)
	seenBindings := make(map[string]struct{})
	for _, segment := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if strings.HasPrefix(segment, "{") || strings.HasSuffix(segment, "}") {
			if len(segment) < 3 || segment[0] != '{' || segment[len(segment)-1] != '}' ||
				!validName(segment[1:len(segment)-1]) {
				return nil, false
			}
			name := segment[1 : len(segment)-1]
			if _, exists := seenBindings[name]; exists {
				return nil, false
			}
			seenBindings[name] = struct{}{}
			bindings = append(bindings, name)
			continue
		}
		for index, char := range segment {
			if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (char == '-' && index > 0) {
				continue
			}
			return nil, false
		}
		if strings.HasSuffix(segment, "-") {
			return nil, false
		}
	}
	return bindings, true
}

func validName(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' && index > 0 || char == '_' && index > 0 {
			continue
		}
		return false
	}
	return true
}

func validInterpolationName(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char == '_' ||
			char >= '0' && char <= '9' && index > 0 {
			continue
		}
		return false
	}
	return true
}

func validText(value string) bool { return utf8.ValidString(value) }

func internalError(message string) error {
	return errs.New(errs.KindInternal, "idempotent intent: "+message)
}

type canonicalEncoder struct {
	buffer []byte
}

func (encoder *canonicalEncoder) clear() {
	clear(encoder.buffer)
	encoder.buffer = nil
}

func (encoder *canonicalEncoder) writeRawBytes(value []byte) {
	encoder.ensure(len(value))
	encoder.buffer = append(encoder.buffer, value...)
}

func (encoder *canonicalEncoder) writeRawString(value string) {
	encoder.ensure(len(value))
	encoder.buffer = append(encoder.buffer, value...)
}

func (encoder *canonicalEncoder) writeLength(value uint64) {
	encoder.ensure(8)
	start := len(encoder.buffer)
	encoder.buffer = encoder.buffer[:start+8]
	binary.BigEndian.PutUint64(encoder.buffer[start:], value)
}

func (encoder *canonicalEncoder) writeScalar(tag valueKind, value []byte) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(tag))
	encoder.writeLength(uint64(len(value)))
	encoder.writeRawBytes(value)
}

func (encoder *canonicalEncoder) writeNull() { encoder.writeScalar(valueNull, nil) }

func (encoder *canonicalEncoder) writeString(value string) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueString))
	encoder.writeLength(uint64(len(value)))
	encoder.writeRawString(value)
}

func (encoder *canonicalEncoder) writeListHeader(count int) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueList))
	encoder.writeLength(uint64(count))
}

func (encoder *canonicalEncoder) writeObjectHeader(count int) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueObject))
	encoder.writeLength(uint64(count))
}

func (encoder *canonicalEncoder) writeValue(value Value) {
	switch value.kind {
	case valueNull:
		encoder.writeNull()
	case valueFalse, valueTrue:
		encoder.writeScalar(value.kind, nil)
	case valueInteger:
		encoder.ensure(1)
		encoder.buffer = append(encoder.buffer, byte(valueInteger))
		encoder.writeLength(uint64(len(value.integer)))
		encoder.writeRawString(value.integer)
	case valueString:
		encoder.writeString(value.text)
	case valueList:
		encoder.writeListHeader(len(value.items))
		for _, item := range value.items {
			encoder.writeValue(item)
		}
	case valueObject:
		fields := append([]Field(nil), value.fields...)
		sort.Slice(fields, func(left, right int) bool { return fields[left].Name < fields[right].Name })
		encoder.writeObjectHeader(len(fields))
		for _, field := range fields {
			encoder.writeString(field.Name)
			encoder.writeValue(field.Value)
		}
	case valueSHA256:
		encoder.writeScalar(valueSHA256, value.digest[:])
	}
}

func (encoder *canonicalEncoder) ensure(additional int) {
	if additional <= cap(encoder.buffer)-len(encoder.buffer) {
		return
	}
	required := len(encoder.buffer) + additional
	capacity := cap(encoder.buffer) * 2
	if capacity < required {
		capacity = required
	}
	if capacity < 64 {
		capacity = 64
	}
	replacement := make([]byte, len(encoder.buffer), capacity)
	copy(replacement, encoder.buffer)
	clear(encoder.buffer)
	encoder.buffer = replacement
}

func (digest *Digest) consume() ([sha256.Size]byte, error) {
	if digest == nil || digest.state == nil {
		return [sha256.Size]byte{}, internalError("digest is required")
	}
	digest.state.mu.Lock()
	defer digest.state.mu.Unlock()
	if digest.state.consumed {
		return [sha256.Size]byte{}, internalError("digest was already consumed")
	}
	value := digest.state.value
	clear(digest.state.value[:])
	digest.state.consumed = true
	return value, nil
}
