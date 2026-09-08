package component

// ValidRouterAlias accepts an optional single lowercase DNS label.
func ValidRouterAlias(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
			return false
		}
	}
	return true
}
