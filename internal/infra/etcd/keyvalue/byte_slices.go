package keyvalue

func ClearByteSlices(values [][]byte) {
	for _, value := range values {
		clear(value)
	}
}
