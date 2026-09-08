package api

// Ordered membership is authored state: the first router Zone is primary.
// Validation must reject duplicates rather than sorting or deduplicating input.
func validComponentZoneIDs(zoneIDs []string) bool {
	if len(zoneIDs) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(zoneIDs))
	for _, zoneID := range zoneIDs {
		if !validStableID(zoneID, "net_") {
			return false
		}
		if _, duplicate := seen[zoneID]; duplicate {
			return false
		}
		seen[zoneID] = struct{}{}
	}
	return true
}
