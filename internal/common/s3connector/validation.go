// Package s3connector owns the authored S3-compatible Connector authority
// validation shared by durable CRUD and execution adapter construction.
package s3connector

import (
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	EndpointMaxBytes = 2048
	PrefixMaxBytes   = 1024
	RegionMaxBytes   = 64
)

func ValidEndpoint(value string) bool {
	parsed, err := url.ParseRequestURI(value)
	return utf8.ValidString(value) && len(value) <= EndpointMaxBytes && err == nil && parsed != nil &&
		(parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && parsed.User == nil &&
		parsed.RawQuery == "" && parsed.Fragment == "" && parsed.Opaque == "" && parsed.Path == "" &&
		parsed.RawPath == ""
}

func ValidBucket(value string) bool {
	if len(value) < 3 || len(value) > 63 || ipv4Shape(value) ||
		hasAnyPrefix(value, "xn--", "sthree-", "amzn-s3-demo-") ||
		hasAnySuffix(value, "-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3") {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || !lowerAlphaNumeric(label[0]) || !lowerAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for index := range len(label) {
			character := label[index]
			if !lowerAlphaNumeric(character) && character != '-' {
				return false
			}
		}
	}
	return true
}

func ValidPrefix(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) || len(value) > PrefixMaxBytes || strings.ContainsRune(value, '\x00') ||
		strings.ContainsRune(value, '\\') || strings.HasPrefix(value, "/") || !strings.HasSuffix(value, "/") {
		return false
	}
	for _, component := range strings.Split(strings.TrimSuffix(value, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

func ValidRegion(value string) bool {
	if len(value) == 0 || len(value) > RegionMaxBytes {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func hasAnyPrefix(value string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func hasAnySuffix(value string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}

func ipv4Shape(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 3 {
			return false
		}
		for index := range len(part) {
			if part[index] < '0' || part[index] > '9' {
				return false
			}
		}
	}
	return true
}

func lowerAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}
