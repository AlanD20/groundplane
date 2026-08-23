package core

import (
	"strings"
	"testing"
)

func TestParseConnectorDocumentRequiresExplicitPathStyle(t *testing.T) {
	// Rationale: false is a valid addressing choice, while omission would make
	// an SDK default an undeclared desired-state input.
	explicitFalse := []byte(`schema: 1
kind: connector
metadata:
  tenant: sample
  project: application
  environment: production
  name: backups
connector:
  kind: s3-compatible
  endpoint: https://objects.example.test
  bucket: groundplane-backups
  region: auto
  path_style: false
  credentials:
    access_key:
      secret_ref: S3_ACCESS_KEY
    secret_key:
      secret_ref: S3_SECRET_KEY
`)
	document, err := ParseConnectorDocument(explicitFalse)
	if err != nil {
		t.Fatalf("ParseConnectorDocument(explicit false) error: %v", err)
	}
	if document.Connector.PathStyle == nil || *document.Connector.PathStyle {
		t.Fatalf("ParseConnectorDocument(explicit false) path_style = %#v", document.Connector.PathStyle)
	}

	missing := []byte(strings.ReplaceAll(string(explicitFalse), "  path_style: false\n", ""))
	if _, err := ParseConnectorDocument(missing); err == nil || !strings.Contains(err.Error(), "path_style") {
		t.Fatalf("ParseConnectorDocument(missing path_style) error = %v", err)
	}
}
