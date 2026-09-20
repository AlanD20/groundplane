package blueprintparser

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/compose-spec/compose-go/v2/types"
	"sort"
	"strings"
)

func validateGroundplaneExtensionNames(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if strings.HasPrefix(key, "x-gp-") {
				if _, known := knownGroundplaneExtensions[key]; !known {
					return validationError("blueprint has an unknown Groundplane extension")
				}
				if _, generated := generatedGroundplaneExtensions[key]; generated {
					return validationError("blueprint contains Controller-generated metadata")
				}
			}
			if err := validateGroundplaneExtensionNames(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := validateGroundplaneExtensionNames(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func hasGroundplaneExtension(value any, expected string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == expected || hasGroundplaneExtension(child, expected) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if hasGroundplaneExtension(child, expected) {
				return true
			}
		}
	}
	return false
}

func hasDocumentGroundplaneField(model map[string]any) bool {
	for name := range rootGroundplaneFields {
		if _, exists := model[name]; exists {
			return true
		}
	}
	return false
}

func mappingFingerprint(mapping types.Mapping) string {
	keys := make([]string, 0, len(mapping))
	for key := range mapping {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%d:%s%d:%s", len(key), key, len(mapping[key]), mapping[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func stringMap(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func anyList(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case nil:
		return nil, false
	default:
		return []any{typed}, true
	}
}

func stringList(value any) ([]string, bool) {
	items, ok := anyList(value)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, false
		}
		result = append(result, text)
	}
	return result, true
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func isEmptyCollection(value any) bool {
	switch typed := value.(type) {
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	case nil:
		return true
	default:
		return false
	}
}
