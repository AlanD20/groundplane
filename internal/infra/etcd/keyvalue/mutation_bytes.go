package keyvalue

func ZeroMutationBytes(mutations []Mutation) {
	for index := range mutations {
		clear(mutations[index].Value)
	}
}
