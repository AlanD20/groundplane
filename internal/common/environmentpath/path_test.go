package environmentpath

import "testing"

const (
	testTenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestDeriveUsesOnlyConfiguredRootAndStableOwnership(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		scope Scope
		want  string
	}{
		{
			name: "tenant",
			scope: Scope{ProjectKind: ProjectKindTenant, TenantID: testTenantID,
				ProjectID: testProjectID, EnvironmentID: testEnvironmentID},
			want: "/srv/groundplane-volumes/" + testTenantID + "/" + testProjectID + "/" + testEnvironmentID,
		},
		{
			name: "backing",
			scope: Scope{ProjectKind: ProjectKindBacking,
				ProjectID: testProjectID, EnvironmentID: testEnvironmentID},
			want: "/srv/groundplane-volumes/platform/" + testProjectID + "/" + testEnvironmentID,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := Derive("/srv/groundplane-volumes", test.scope)
			if err != nil || got != test.want {
				t.Fatalf("Derive() = %q, %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestDeriveRejectsEveryInvalidOwnershipShape(t *testing.T) {
	t.Parallel()
	valid := Scope{
		ProjectKind: ProjectKindTenant, TenantID: testTenantID,
		ProjectID: testProjectID, EnvironmentID: testEnvironmentID,
	}
	for _, test := range []struct {
		name   string
		root   string
		mutate func(*Scope)
	}{
		{name: "relative root", root: "var/lib/groundplane/vol"},
		{name: "root filesystem", root: "/"},
		{name: "dirty root", root: "/var/lib/../lib/groundplane/vol"},
		{name: "trailing separator", root: "/var/lib/groundplane/vol/"},
		{name: "invalid project kind", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.ProjectKind = "other" }},
		{name: "wrong tenant kind", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.TenantID = testProjectID }},
		{name: "wrong project kind", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.ProjectID = testEnvironmentID }},
		{name: "wrong environment kind", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.EnvironmentID = testProjectID }},
		{name: "backing with tenant", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.ProjectKind = ProjectKindBacking }},
		{name: "tenant without tenant", root: DefaultVolumeRoot, mutate: func(scope *Scope) { scope.TenantID = "" }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scope := valid
			if test.mutate != nil {
				test.mutate(&scope)
			}
			if _, err := Derive(test.root, scope); err == nil {
				t.Fatal("Derive() accepted invalid root or ownership")
			}
		})
	}
}

func TestParseAuthorizesOnlyExactConfiguredRootPaths(t *testing.T) {
	t.Parallel()
	want := Scope{
		ProjectKind: ProjectKindTenant, TenantID: testTenantID,
		ProjectID: testProjectID, EnvironmentID: testEnvironmentID,
	}
	value, err := Derive(DefaultVolumeRoot, want)
	if err != nil {
		t.Fatalf("Derive() error = %v", err)
	}
	got, err := Parse(DefaultVolumeRoot, value)
	if err != nil || got != want {
		t.Fatalf("Parse() = %#v, %v, want %#v", got, err, want)
	}
	for _, invalid := range []string{
		"/srv/groundplane-volumes/" + testTenantID + "/" + testProjectID + "/" + testEnvironmentID,
		DefaultVolumeRoot + "/tenant-slug/project-slug/" + testEnvironmentID,
		DefaultVolumeRoot + "/" + testTenantID + "/" + testProjectID + "/other",
		DefaultVolumeRoot + "/platform/" + testProjectID + "/" + testEnvironmentID + "/extra",
	} {
		if _, err := Parse(DefaultVolumeRoot, invalid); err == nil {
			t.Fatalf("Parse(%q) accepted invalid path", invalid)
		}
	}
}
