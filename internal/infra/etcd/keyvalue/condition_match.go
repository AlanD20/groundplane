package keyvalue

func ConditionMatchesRead(condition Condition, value *KeyValue) bool {
	if condition.Prefix {
		return false
	}
	if condition.ModRevision == 0 {
		return value == nil
	}
	return value != nil && value.Key == condition.Key && value.ModRevision == condition.ModRevision
}
