package taskmaterialization

// EqualRecords compares immutable source selection as well as output metadata.
// Pointer identity does not identify a retained file or Secret generation.
func EqualRecords(left, right []Record) bool {
	if len(left) != len(right) {
		return false
	}
	for index, record := range left {
		other := right[index]
		if record.StepID != other.StepID || record.MaterializationID != other.MaterializationID ||
			record.EnvironmentID != other.EnvironmentID || record.Destination != other.Destination ||
			record.ServiceID != other.ServiceID || record.ServiceName != other.ServiceName ||
			record.OutputKind != other.OutputKind || record.UID != other.UID || record.GID != other.GID ||
			record.Mode != other.Mode || record.Length != other.Length || record.SHA256 != other.SHA256 ||
			!equalSource(record.Source, other.Source) {
			return false
		}
	}
	return true
}

func equalSource(left, right Source) bool {
	if left.Kind != right.Kind || !equalReference(left.BlueprintFile, right.BlueprintFile) ||
		!equalReference(
			left.ComponentFile,
			right.ComponentFile,
		) || !equalReference(left.EntryValue, right.EntryValue) ||
		(left.GeneratedEnvironment == nil) != (right.GeneratedEnvironment == nil) {
		return false
	}
	if left.GeneratedEnvironment == nil {
		return true
	}
	first, second := left.GeneratedEnvironment, right.GeneratedEnvironment
	if first.FormatVersion != second.FormatVersion || len(first.Values) != len(second.Values) {
		return false
	}
	for index, value := range first.Values {
		other := second.Values[index]
		if value.Name != other.Name || value.Value != other.Value || !equalReference(value.Secret, other.Secret) {
			return false
		}
	}
	return true
}

func equalReference[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
