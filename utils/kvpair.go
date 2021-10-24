package utils

type kvPair struct {
	key string
	val interface{}
}
type KVPairs []kvPair

func (pairs KVPairs) Set(key string, val interface{}) (newPairs KVPairs) {
	for i := 0; i < len(pairs); i++ {
		if pairs[i].key == key {
			pairs[i].val = val
			return pairs
		}
	}
	newPairs = append(pairs, kvPair{
		key: key,
		val: val,
	})
	return
}
func (pairs KVPairs) Get(key string) (val interface{}, ok bool) {
	for i := 0; i < len(pairs); i++ {
		if pairs[i].key == key {
			return pairs[i].val, true
		}
	}
	return nil, false
}
