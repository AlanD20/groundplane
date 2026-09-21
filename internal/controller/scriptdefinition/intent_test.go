package scriptdefinition

import (
	"context"
	"testing"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: changing order must not replay a different saved mutation; explicit
// zero in a patch remains distinct from omission, while creation defaults agree.
func TestScriptOrderIsPartOfProtectedMutationIntent(t *testing.T) {
	zero, ten := uint16(0), uint16(10)
	base := apiTypes.ScriptCreate{Slug: "migrate", Body: "echo migrate", When: "pre-deploy"}
	changed := base
	changed.Order = ten
	for _, test := range []struct {
		name        string
		left, right idempotentintent.Value
		match       bool
	}{
		{"create order", CreateIntentBody(base), CreateIntentBody(changed), false},
		{"edit explicit zero", EditIntentBody(apiTypes.ScriptEdit{}), EditIntentBody(apiTypes.ScriptEdit{Order: &zero}), false},
		{"edit changed order", EditIntentBody(apiTypes.ScriptEdit{Order: &zero}), EditIntentBody(apiTypes.ScriptEdit{Order: &ten}), false},
		{"same intent", CreateIntentBody(base), CreateIntentBody(base), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			protector, err := secretvalue.NewProtector(intentCipher{}, intentCipher{})
			if err != nil {
				t.Fatal(err)
			}
			left := protectIntent(t, protector, test.left)
			right := protectIntent(t, protector, test.right)
			defer left.Clear()
			defer right.Clear()
			match, err := idempotentintent.CompareProtected(context.Background(), protector, left, right)
			if err != nil || match != test.match {
				t.Fatalf("protected intent match = %v, %v; want %v", match, err, test.match)
			}
		})
	}
}

func protectIntent(t *testing.T, protector *secretvalue.Protector, body idempotentintent.Value) secretvalue.Envelope {
	t.Helper()
	version, digest, err := idempotentintent.Canonicalize(context.Background(), idempotentintent.CanonicalIntentV1{
		Method: "POST", Route: "/scripts", Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment,
			ID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer digest.Destroy()
	protected, err := idempotentintent.Protect(context.Background(), protector, version, digest)
	if err != nil {
		t.Fatal(err)
	}
	return protected
}

// The crypto port is irrelevant to intent equality; retain its buffer-ownership contract.
type intentCipher struct{}

func (intentCipher) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (intentCipher) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}
