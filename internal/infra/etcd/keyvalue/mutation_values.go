package keyvalue

func CloneMutations(values []Mutation) []Mutation {
	result := make([]Mutation, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Value = append([]byte(nil), value.Value...)
	}
	return result
}

func ClearMutationValues(values []Mutation) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}
