package common

import (
	"strconv"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// RecoveryPointTable owns the table-only formatting for the typed verified
// Recovery Point collection. JSON and YAML continue to render the API page.
func RecoveryPointTable(points []apiTypes.RecoveryPoint) ([]string, [][]string) {
	headers := []string{
		"ID", "SOURCE_ID", "SOURCE_KIND", "TARGET_ID", "CREATED_AT",
		"SIZE_BYTES", "ENCRYPTED", "KEY_ERA", "STATUS",
	}
	rows := make([][]string, len(points))
	for index, point := range points {
		keyEra := ""
		if point.Encrypted {
			keyEra = strconv.Itoa(point.KeyEra)
		}
		rows[index] = []string{
			point.ID,
			point.SourceID,
			string(point.SourceKind),
			point.TargetID,
			point.CreatedAt,
			strconv.FormatInt(point.SizeBytes, 10),
			strconv.FormatBool(point.Encrypted),
			keyEra,
			string(point.Status),
		}
	}
	return headers, rows
}
