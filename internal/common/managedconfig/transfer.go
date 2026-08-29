package managedconfig

const (
	MediaTypeTextUTF8    = "text/plain; charset=utf-8"
	MaximumArtifactBytes = 96 * 1024
	MaximumChunkBytes    = 32 * 1024
)

func ValidMediaType(value string) bool {
	return value == MediaTypeTextUTF8
}
