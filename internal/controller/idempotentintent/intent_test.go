package idempotentintent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: object layout and validated query spelling must not change the
// identity of an otherwise equal operator intent.
func TestCanonicalizeNormalizesObjectOrder(t *testing.T) {
	t.Parallel()

	left := canonicalTestIntent()
	left.Query = Object(
		Field{Name: "limit", Value: Integer(50)},
		Field{Name: "cursor", Value: Null()},
	)
	left.Body = JSONBody(Object(
		Field{Name: "enabled", Value: Bool(true)},
		Field{Name: "name", Value: String("api")},
	))
	right := canonicalTestIntent()
	right.Query = Object(
		Field{Name: "cursor", Value: Null()},
		Field{Name: "limit", Value: Integer(50)},
	)
	right.Body = JSONBody(Object(
		Field{Name: "name", Value: String("api")},
		Field{Name: "enabled", Value: Bool(true)},
	))

	leftDigest := canonicalTestDigest(t, left)
	rightDigest := canonicalTestDigest(t, right)
	if leftDigest.state.value != rightDigest.state.value {
		t.Fatal("equivalent typed intents produced different digests")
	}
}

// Rationale: route, durable owner, actual path, absent/null, and content kind
// are separate domains even when the mutation payload is otherwise equal.
func TestCanonicalizeSeparatesIntentDimensions(t *testing.T) {
	t.Parallel()

	base := canonicalTestIntent()
	baseDigest := canonicalTestDigest(t, base).state.value
	tests := []struct {
		name   string
		mutate func(*CanonicalIntentV1)
	}{
		{name: "route", mutate: func(value *CanonicalIntentV1) { value.Route = "/services/{id}/stop" }},
		{name: "owner", mutate: func(value *CanonicalIntentV1) {
			value.Scope.ID = ids.NewAt(ids.KindEnvironment, testTime(2), 2)
		}},
		{
			name:   "path",
			mutate: func(value *CanonicalIntentV1) { value.Path[0].Value = ids.NewAt(ids.KindService, testTime(3), 3) },
		},
		{name: "absent versus null", mutate: func(value *CanonicalIntentV1) {
			value.Query = Object(Field{Name: "cursor", Value: Null()})
		}},
		{name: "none versus json null", mutate: func(value *CanonicalIntentV1) { value.Body = JSONBody(Null()) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := canonicalTestIntent()
			test.mutate(&changed)
			if got := canonicalTestDigest(t, changed).state.value; got == baseDigest {
				t.Fatal("different intent dimension produced the same digest")
			}
		})
	}
}

// Rationale: multipart transport framing is excluded, while every verified
// Blueprint manifest decision remains committed by the intent digest.
func TestCanonicalizeBlueprintUsesOnlyVerifiedManifest(t *testing.T) {
	t.Parallel()

	fileDigest := sha256.Sum256([]byte("services: {}\n"))
	base := canonicalTestIntent()
	base.Body = BlueprintBody(BlueprintManifestV1{
		FormatVersion:  1,
		RootPath:       "blueprint.yaml",
		ComposeSources: []string{"blueprint.yaml"},
		Interpolation: []Interpolation{
			{Name: "TAG", Value: "latest"},
			{Name: "PORT", Value: "8080"},
		},
		Files: []BlueprintFile{{
			Path: "blueprint.yaml", Part: "file-000001", Size: 13, SHA256: fileDigest,
		}},
	})
	reorderedVariables := base
	reorderedVariables.Body = BlueprintBody(base.Body.blueprint)
	reorderedVariables.Body.blueprint.Interpolation = []Interpolation{
		{Name: "PORT", Value: "8080"},
		{Name: "TAG", Value: "latest"},
	}
	if canonicalTestDigest(t, base).state.value != canonicalTestDigest(t, reorderedVariables).state.value {
		t.Fatal("interpolation map order changed the Blueprint digest")
	}

	changed := base
	changed.Body = BlueprintBody(base.Body.blueprint)
	changed.Body.blueprint.ComposeSources = append([]string(nil), base.Body.blueprint.ComposeSources...)
	changed.Body.blueprint.Interpolation = append([]Interpolation(nil), base.Body.blueprint.Interpolation...)
	changed.Body.blueprint.Files = append([]BlueprintFile(nil), base.Body.blueprint.Files...)
	changed.Body.blueprint.Interpolation[0].Value = "stable"
	if canonicalTestDigest(t, base).state.value == canonicalTestDigest(t, changed).state.value {
		t.Fatal("changed Blueprint interpolation produced the same digest")
	}
}

// Rationale: the version-1 Blueprint vector freezes the dedicated 0x08 file
// digest tag in addition to the ordinary JSON scalar/container tags.
func TestCanonicalVersion1BlueprintGoldenVector(t *testing.T) {
	t.Parallel()

	fileDigest := sha256.Sum256([]byte("services: {}\n"))
	intent := canonicalTestIntent()
	intent.Body = BlueprintBody(BlueprintManifestV1{
		FormatVersion:  1,
		RootPath:       "blueprint.yaml",
		ComposeSources: []string{"blueprint.yaml"},
		Files: []BlueprintFile{{
			Path: "blueprint.yaml", Part: "file-000001", Size: 13, SHA256: fileDigest,
		}},
	})
	digest := canonicalTestDigest(t, intent)
	got := hex.EncodeToString(digest.state.value[:])
	const want = "af16e5a9a68505fd314d18b28dc7931e3ae3d7cfc522ce9df4986a7ea0e3bdd1"
	if got != want {
		t.Fatalf("version 1 Blueprint digest = %s, want %s", got, want)
	}
}

// Rationale: the sensitive digest must fail closed rather than acquiring a
// raw JSON, text, or fmt representation through Go defaults.
func TestDigestCannotBeSerializedOrFormatted(t *testing.T) {
	t.Parallel()

	digest := canonicalTestDigest(t, canonicalTestIntent())
	raw := fmt.Sprintf("%x", digest.state.value)
	if rendered := fmt.Sprintf("%v", digest); rendered != "[sensitive idempotent intent digest]" {
		t.Fatalf("formatted digest = %q", rendered)
	}
	copy := *digest
	if rendered := fmt.Sprintf("%x", copy); rendered != "[sensitive idempotent intent digest]" {
		t.Fatalf("formatted digest copy = %q", rendered)
	}
	if bytes, err := json.Marshal(digest); err == nil || strings.Contains(string(bytes), raw) {
		t.Fatalf("json.Marshal() = %q, %v; want safe failure", bytes, err)
	}
	if bytes, err := digest.MarshalText(); err == nil || strings.Contains(string(bytes), raw) {
		t.Fatalf("MarshalText() = %q, %v; want safe failure", bytes, err)
	}
}

// Rationale: explicit tag assignments and one fixed digest make version 1
// immutable across harmless source reordering and future scalar additions.
func TestCanonicalVersion1GoldenVector(t *testing.T) {
	t.Parallel()

	intent := canonicalTestIntent()
	intent.Query = Object(
		Field{Name: "false", Value: Bool(false)},
		Field{Name: "integer", Value: Integer(-7)},
		Field{Name: "list", Value: List(String("x"), Null())},
		Field{Name: "true", Value: Bool(true)},
	)
	intent.Body = JSONBody(Object(Field{Name: "empty", Value: String("")}))
	digest := canonicalTestDigest(t, intent)
	got := hex.EncodeToString(digest.state.value[:])
	const want = "c36952fa6338f83a0f6d8221270ff673d954def0801c18596e06ca4fcd08ac7c"
	if got != want {
		t.Fatalf("version 1 digest = %s, want %s", got, want)
	}
}

// Rationale: route bindings are a complete ordered projection of registered
// placeholders; omissions or reorderings could otherwise collapse intents.
func TestCanonicalizeRequiresExactRouteBindings(t *testing.T) {
	t.Parallel()

	for _, path := range [][]PathBinding{
		nil,
		{{Name: "service_id", Value: ids.NewAt(ids.KindService, testTime(1), 2)}},
		{{Name: "id", Value: ids.NewAt(ids.KindService, testTime(1), 2)}, {Name: "id", Value: "extra"}},
	} {
		intent := canonicalTestIntent()
		intent.Path = path
		_, _, err := Canonicalize(context.Background(), intent)
		if !hasKind(err, errs.KindInternal) {
			t.Fatalf("Canonicalize(path=%+v) error = %v, want internal", path, err)
		}
	}
}

// Rationale: the encoder owns one mutable staging buffer and must erase it
// after success or abandonment because it can contain secret field values.
func TestCanonicalEncoderClearErasesOwnedBuffer(t *testing.T) {
	t.Parallel()

	encoder := canonicalEncoder{}
	encoder.writeString("sensitive-value")
	owned := encoder.buffer
	encoder.clear()
	for index, value := range owned {
		if value != 0 {
			t.Fatalf("owned buffer byte %d = %d, want zero", index, value)
		}
	}
}

// Rationale: encoder growth must wipe retired backing arrays immediately;
// waiting for garbage collection would retain secret-bearing prefixes.
func TestCanonicalEncoderGrowthErasesRetiredBuffer(t *testing.T) {
	t.Parallel()

	encoder := canonicalEncoder{}
	encoder.writeString("sensitive-value")
	retired := encoder.buffer
	encoder.writeString(strings.Repeat("x", 256))
	for index, value := range retired {
		if value != 0 {
			t.Fatalf("retired buffer byte %d = %d, want zero", index, value)
		}
	}
	encoder.clear()
}

// Rationale: invalid typed values indicate a programming error, never a
// caller-validation response or a panic inside canonicalization.
func TestCanonicalizeRejectsProgrammingInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*CanonicalIntentV1)
	}{
		{name: "safe method", mutate: func(value *CanonicalIntentV1) { value.Method = "GET" }},
		{name: "raw route", mutate: func(value *CanonicalIntentV1) { value.Route = "/services//start?x=1" }},
		{
			name:   "platform id",
			mutate: func(value *CanonicalIntentV1) { value.Scope = Scope{Kind: ScopePlatform, ID: "platform"} },
		},
		{name: "query is not object", mutate: func(value *CanonicalIntentV1) { value.Query = Null() }},
		{name: "duplicate member", mutate: func(value *CanonicalIntentV1) {
			value.Query = Object(Field{Name: "x", Value: Null()}, Field{Name: "x", Value: Null()})
		}},
		{name: "binary json", mutate: func(value *CanonicalIntentV1) {
			value.Body = JSONBody(SHA256Value(sha256.Sum256([]byte("secret"))))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			intent := canonicalTestIntent()
			test.mutate(&intent)
			_, _, err := Canonicalize(context.Background(), intent)
			kind, ok := errs.KindOf(err)
			if !ok || kind != errs.KindInternal {
				t.Fatalf("error kind = %v, %v, want %v, true; err=%v", kind, ok, errs.KindInternal, err)
			}
		})
	}
}

func canonicalTestIntent() CanonicalIntentV1 {
	return CanonicalIntentV1{
		Method: "POST",
		Route:  "/services/{id}/start",
		Scope: Scope{
			Kind: ScopeEnvironment,
			ID:   ids.NewAt(ids.KindEnvironment, testTime(1), 1),
		},
		Path: []PathBinding{{
			Name:  "id",
			Value: ids.NewAt(ids.KindService, testTime(1), 2),
		}},
		Query: Object(),
		Body:  NoBody(),
	}
}

func canonicalTestDigest(t *testing.T, intent CanonicalIntentV1) *Digest {
	t.Helper()
	version, digest, err := Canonicalize(context.Background(), intent)
	if err != nil {
		t.Fatalf("Canonicalize() error = %v", err)
	}
	if version != Version1 {
		t.Fatalf("Canonicalize() version = %d, want %d", version, Version1)
	}
	return digest
}
