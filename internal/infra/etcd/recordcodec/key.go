package recordcodec

import "encoding/base64"

// EncodeKeySegment preserves a dynamic label as one canonical storage-key segment.
func EncodeKeySegment(value string) string {
	return "~" + base64.RawURLEncoding.EncodeToString([]byte(value))
}
