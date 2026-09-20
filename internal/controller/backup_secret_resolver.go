package controller

import (
	"bytes"
	"context"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupSecretEvidenceReader interface {
	ResolveBackupSecretEvidence(
		context.Context,
		backupsecret.Request,
	) (etcd.BackupSecretResolutionEvidence, error)
}

// BackupSecretResolver converts one fixed-revision durable evidence snapshot
// into the two transient S3 credential slots accepted by the Agent. It never
// resolves or decrypts an age identity.
type BackupSecretResolver struct {
	reader    backupSecretEvidenceReader
	protector *secretvalue.Protector
}

func NewBackupSecretResolver(
	reader backupSecretEvidenceReader,
	protector *secretvalue.Protector,
) (*BackupSecretResolver, error) {
	if reader == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolver dependencies are required")
	}
	return &BackupSecretResolver{reader: reader, protector: protector}, nil
}

// ResolveBackupSecretSlots is the composition seam for an authenticated Agent
// dispatch. The durable reader fences Task, claim, assignment, Agent
// generation, sealed plan, and selected step at one MVCC revision.
func (resolver *BackupSecretResolver) ResolveBackupSecretSlots(
	ctx context.Context,
	request backupsecret.Request,
) (map[agentpb.BackupSecretSlotPurpose][]byte, error) {
	if resolver == nil || resolver.reader == nil || resolver.protector == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolver is not configured")
	}
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolver context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	evidence, err := resolver.reader.ResolveBackupSecretEvidence(ctx, request)
	defer evidence.Clear()
	if err != nil {
		return nil, err
	}

	slots := make(map[agentpb.BackupSecretSlotPurpose][]byte, 2)
	ok := false
	defer func() {
		if !ok {
			clearBackupSecretSlotMap(slots)
		}
	}()

	expected, err := expectedBackupCredentialSources(evidence.Connector)
	if err != nil {
		return nil, err
	}
	if evidence.HasCredentials {
		direct, openErr := resolver.openDirectCredentials(ctx, &evidence.Credentials, expected)
		if openErr != nil {
			return nil, openErr
		}
		for name, value := range direct {
			purpose, purposeErr := backupCredentialPurpose(name)
			if purposeErr != nil || expected[name] != core.ConnectorCredentialDirect {
				clearBackupSecretMap(direct)
				return nil, errs.New(errs.KindInternal, "backup direct credential evidence is inconsistent")
			}
			slots[purpose] = value
			direct[name] = nil
			delete(direct, name)
		}
		clearBackupSecretMap(direct)
	} else {
		for _, kind := range expected {
			if kind == core.ConnectorCredentialDirect {
				return nil, errs.New(errs.KindStateConflict, "backup direct credential evidence is missing")
			}
		}
	}

	seen := make(map[core.ConnectorCredentialName]bool, len(evidence.SecretValues))
	for index := range evidence.SecretValues {
		value := &evidence.SecretValues[index]
		if value.Name != core.ConnectorCredentialAccessKey && value.Name != core.ConnectorCredentialSecretKey {
			return nil, errs.New(errs.KindInternal, "backup Secret credential name is invalid")
		}
		credential, found := evidence.Connector.Connector.Credentials[value.Name]
		if !found || seen[value.Name] || expected[value.Name] != core.ConnectorCredentialSecretRef ||
			credential.SecretRef != value.Reference {
			return nil, errs.New(errs.KindInternal, "backup Secret credential evidence is inconsistent")
		}
		seen[value.Name] = true
		plaintext, openErr := resolver.openSecretValue(ctx, &value.Value)
		if openErr != nil {
			clear(plaintext)
			return nil, openErr
		}
		if len(plaintext) == 0 || len(plaintext) > int(executionplan.MaximumBackupSecretCredentialBytes) {
			clear(plaintext)
			return nil, errs.New(errs.KindValidationFailed, "backup Secret credential value is invalid")
		}
		purpose, purposeErr := backupCredentialPurpose(value.Name)
		if purposeErr != nil {
			clear(plaintext)
			return nil, purposeErr
		}
		if _, exists := slots[purpose]; exists {
			clear(plaintext)
			return nil, errs.New(errs.KindInternal, "backup credential slot is duplicated")
		}
		slots[purpose] = plaintext
	}
	for name, kind := range expected {
		if kind == core.ConnectorCredentialSecretRef && !seen[name] {
			return nil, errs.New(errs.KindStateConflict, "backup Secret credential evidence is missing")
		}
	}
	if len(slots) != 2 {
		return nil, errs.New(errs.KindInternal, "backup credential slots are incomplete")
	}
	ok = true
	return slots, nil
}

func expectedBackupCredentialSources(
	record etcd.ConnectorRecord,
) (map[core.ConnectorCredentialName]core.ConnectorCredentialKind, error) {
	if len(record.Connector.Credentials) != 2 {
		return nil, errs.New(errs.KindInternal, "backup Connector credential metadata is invalid")
	}
	expected := make(map[core.ConnectorCredentialName]core.ConnectorCredentialKind, 2)
	for _, name := range []core.ConnectorCredentialName{
		core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey,
	} {
		credential, found := record.Connector.Credentials[name]
		if !found || (credential.Kind != core.ConnectorCredentialDirect &&
			credential.Kind != core.ConnectorCredentialSecretRef) {
			return nil, errs.New(errs.KindInternal, "backup Connector credential metadata is invalid")
		}
		expected[name] = credential.Kind
	}
	return expected, nil
}

func backupCredentialPurpose(name core.ConnectorCredentialName) (agentpb.BackupSecretSlotPurpose, error) {
	switch name {
	case core.ConnectorCredentialAccessKey:
		return agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, nil
	case core.ConnectorCredentialSecretKey:
		return agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, nil
	default:
		return 0, errs.New(errs.KindInternal, "backup credential name is invalid")
	}
}

func (resolver *BackupSecretResolver) openDirectCredentials(
	ctx context.Context,
	value *etcd.ConnectorEncryptedCredentials,
	expected map[core.ConnectorCredentialName]core.ConnectorCredentialKind,
) (map[core.ConnectorCredentialName][]byte, error) {
	metadata := secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(value.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(value.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(value.DigestAlgorithm),
			Value:     value.CiphertextSHA256,
		},
	}
	ciphertext := value.Ciphertext
	value.Ciphertext = nil
	envelope, err := secretvalue.RestoreOwned(metadata, ciphertext)
	if err != nil {
		return nil, err
	}
	defer envelope.Clear()
	result := make(map[core.ConnectorCredentialName][]byte)
	err = resolver.protector.OpenOwned(ctx, &envelope, func(plaintext []byte) error {
		decoded, decodeErr := decodeDirectCredentialJSON(plaintext, expected)
		if decodeErr != nil {
			return decodeErr
		}
		result = decoded
		return nil
	})
	if err != nil {
		clearBackupSecretMap(result)
		return nil, err
	}
	return result, nil
}

func (resolver *BackupSecretResolver) openSecretValue(
	ctx context.Context,
	value *secretrecord.EncryptedValue,
) ([]byte, error) {
	metadata := secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(value.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(value.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(value.DigestAlgorithm),
			Value:     value.CiphertextSHA256,
		},
	}
	ciphertext := value.Ciphertext
	value.Ciphertext = nil
	envelope, err := secretvalue.RestoreOwned(metadata, ciphertext)
	if err != nil {
		return nil, err
	}
	defer envelope.Clear()
	var result []byte
	err = resolver.protector.OpenOwned(ctx, &envelope, func(plaintext []byte) error {
		result = append([]byte(nil), plaintext...)
		return nil
	})
	if err != nil {
		clear(result)
		return nil, err
	}
	return result, nil
}

func decodeDirectCredentialJSON(
	value []byte,
	expected map[core.ConnectorCredentialName]core.ConnectorCredentialKind,
) (map[core.ConnectorCredentialName][]byte, error) {
	if len(value) == 0 || len(value) > int(executionplan.MaximumBackupSecretCredentialBytes) {
		return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
	}
	parser := directCredentialJSONParser{value: value}
	parser.skipWhitespace()
	if !parser.consume('{') {
		return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
	}
	decoded := make(map[core.ConnectorCredentialName][]byte, len(expected))
	for {
		parser.skipWhitespace()
		if parser.consume('}') {
			break
		}
		keyBytes, keyErr := parser.stringBytes()
		if keyErr != nil {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
		}
		key := core.ConnectorCredentialName("")
		switch {
		case bytes.Equal(keyBytes, []byte(backupsecret.CredentialAccessKey)):
			key = backupsecret.CredentialAccessKey
		case bytes.Equal(keyBytes, []byte(backupsecret.CredentialSecretKey)):
			key = backupsecret.CredentialSecretKey
		}
		clear(keyBytes)
		if key == "" {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON contains an unexpected field")
		}
		if _, duplicate := decoded[key]; duplicate {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
		}
		kind, known := expected[key]
		if !known || kind != core.ConnectorCredentialDirect {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON contains an unexpected field")
		}
		parser.skipWhitespace()
		if !parser.consume(':') {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
		}
		parser.skipWhitespace()
		text, decodeErr := parser.stringBytes()
		if decodeErr != nil || len(text) == 0 ||
			len(text) > int(executionplan.MaximumBackupSecretCredentialBytes) {
			clear(text)
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON value is invalid")
		}
		decoded[key] = text
		parser.skipWhitespace()
		if parser.consume('}') {
			break
		}
		if !parser.consume(',') {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
		}
		parser.skipWhitespace()
		if parser.offset >= len(parser.value) || parser.value[parser.offset] == '}' {
			clearBackupSecretMap(decoded)
			return nil, errs.New(errs.KindInternal, "backup direct credential JSON is invalid")
		}
	}
	parser.skipWhitespace()
	if parser.offset != len(value) || len(decoded) != len(expectedDirectCredentialNames(expected)) {
		clearBackupSecretMap(decoded)
		return nil, errs.New(errs.KindInternal, "backup direct credential JSON is incomplete")
	}
	return decoded, nil
}

// directCredentialJSONParser reads directly from the clearable plaintext
// slice. It deliberately avoids encoding/json's retained decoder buffer and
// avoids converting secret values through immutable Go strings.
type directCredentialJSONParser struct {
	value  []byte
	offset int
}

func (parser *directCredentialJSONParser) skipWhitespace() {
	for parser.offset < len(parser.value) {
		switch parser.value[parser.offset] {
		case ' ', '\t', '\n', '\r':
			parser.offset++
		default:
			return
		}
	}
}

func (parser *directCredentialJSONParser) consume(expected byte) bool {
	if parser.offset >= len(parser.value) || parser.value[parser.offset] != expected {
		return false
	}
	parser.offset++
	return true
}

func (parser *directCredentialJSONParser) stringBytes() ([]byte, error) {
	if !parser.consume('"') {
		return nil, errs.New(errs.KindInternal, "backup direct credential JSON string is invalid")
	}
	decoded := make([]byte, 0, len(parser.value)-parser.offset)
	fail := func() ([]byte, error) {
		clear(decoded)
		return nil, errs.New(errs.KindInternal, "backup direct credential JSON string is invalid")
	}
	for parser.offset < len(parser.value) {
		current := parser.value[parser.offset]
		parser.offset++
		switch current {
		case '"':
			if !utf8.Valid(decoded) {
				return fail()
			}
			return decoded, nil
		case '\\':
			if parser.offset >= len(parser.value) {
				return fail()
			}
			escape := parser.value[parser.offset]
			parser.offset++
			switch escape {
			case '"', '\\', '/':
				decoded = append(decoded, escape)
			case 'b':
				decoded = append(decoded, '\b')
			case 'f':
				decoded = append(decoded, '\f')
			case 'n':
				decoded = append(decoded, '\n')
			case 'r':
				decoded = append(decoded, '\r')
			case 't':
				decoded = append(decoded, '\t')
			case 'u':
				runeValue, ok := parser.unicodeEscape()
				if !ok {
					return fail()
				}
				decoded = utf8.AppendRune(decoded, runeValue)
			default:
				return fail()
			}
		default:
			if current < 0x20 {
				return fail()
			}
			decoded = append(decoded, current)
		}
	}
	return fail()
}

func (parser *directCredentialJSONParser) unicodeEscape() (rune, bool) {
	first, ok := parser.hexQuad()
	if !ok || first >= 0xDC00 && first <= 0xDFFF {
		return 0, false
	}
	if first < 0xD800 || first > 0xDBFF {
		return rune(first), true
	}
	if parser.offset+2 > len(parser.value) || parser.value[parser.offset] != '\\' ||
		parser.value[parser.offset+1] != 'u' {
		return 0, false
	}
	parser.offset += 2
	second, ok := parser.hexQuad()
	if !ok || second < 0xDC00 || second > 0xDFFF {
		return 0, false
	}
	decoded := utf16.DecodeRune(rune(first), rune(second))
	return decoded, decoded != utf8.RuneError
}

func (parser *directCredentialJSONParser) hexQuad() (uint16, bool) {
	if parser.offset+4 > len(parser.value) {
		return 0, false
	}
	var result uint16
	for range 4 {
		current := parser.value[parser.offset]
		parser.offset++
		result <<= 4
		switch {
		case current >= '0' && current <= '9':
			result |= uint16(current - '0')
		case current >= 'a' && current <= 'f':
			result |= uint16(current-'a') + 10
		case current >= 'A' && current <= 'F':
			result |= uint16(current-'A') + 10
		default:
			return 0, false
		}
	}
	return result, true
}

func expectedDirectCredentialNames(
	expected map[core.ConnectorCredentialName]core.ConnectorCredentialKind,
) []string {
	names := make([]string, 0, 2)
	for name, kind := range expected {
		if kind == core.ConnectorCredentialDirect {
			names = append(names, string(name))
		}
	}
	return names
}

func clearBackupSecretMap(values map[core.ConnectorCredentialName][]byte) {
	for key, value := range values {
		clear(value)
		delete(values, key)
	}
}

func clearBackupSecretSlotMap(values map[agentpb.BackupSecretSlotPurpose][]byte) {
	for purpose, value := range values {
		clear(value)
		delete(values, purpose)
	}
}
