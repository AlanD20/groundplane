package blueprintparser

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"gopkg.in/yaml.v3"
)

// Rationale: direct resource mutation artifacts may contain each closed
// generated extension and both Compose metadata encodings. Authoring removes
// only those fields, preserving human metadata, managed paths and opaque refs.
func TestMarshalAuthoringDocumentOmitsGeneratedResourceMetadata(t *testing.T) {
	for _, section := range []string{"services", "networks", "volumes", "configs", "secrets"} {
		for _, sequence := range []bool{false, true} {
			name := section + "/mapping"
			labels := "      com.groundplane.managed: 'true'\n      owner: operator\n"
			if sequence {
				name = section + "/sequence"
				labels = "      - com.groundplane.managed=true\n      - owner=operator\n"
			}
			t.Run(name, func(t *testing.T) {
				input := AuthoringDocument{
					Envelope: core.Envelope{Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
						Metadata: core.EnvelopeMetadata{
							Tenant:      "tenant",
							Project:     "project",
							Environment: "production",
						}},
					NetworkPool: "10.40.0.0/16",
					Compose: []byte(section + ":\n  resource:\n" +
						"    x-gp-resource: {id: generated}\n    x-gp-execution: generated\n    x-gp-managed: true\n" +
						"    labels:\n" + labels + "    annotations:\n" + labels +
						"    file: config/retained.yaml\n    x-custom: {source: opaque-reference}\n" +
						"    environment: {com.groundplane.keep: literal-data}\n"),
				}
				encoded, err := MarshalAuthoringDocument(input)
				if err != nil {
					t.Fatal(err)
				}
				for _, forbidden := range []string{"x-gp-resource", "x-gp-execution", "x-gp-managed", "com.groundplane.managed"} {
					if strings.Contains(string(encoded), forbidden) {
						t.Fatalf("retained generated %s", forbidden)
					}
				}
				var document yaml.Node
				if err := yaml.Unmarshal(encoded, &document); err != nil {
					t.Fatal(err)
				}
				for _, retained := range []string{"operator", "config/retained.yaml", "opaque-reference", "com.groundplane.keep", "literal-data"} {
					if !strings.Contains(string(encoded), retained) {
						t.Fatalf("lost authored %s", retained)
					}
				}
			})
		}
	}
}
