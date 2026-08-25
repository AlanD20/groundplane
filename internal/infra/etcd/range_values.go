package etcd

func clearRangeValues(values []KeyValue) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}
