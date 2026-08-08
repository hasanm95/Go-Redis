package store

import (
	"bytes"
	"fmt"
)

// wrongArgsErr returns the standard RESP error for missing arguments.
func wrongArgsErr(command string) []byte {
	return []byte(fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", command))
}

// getExistingItem returns the item for key if present and not expired.
// It deletes the key if expired. Caller MUST hold mapMutex.Lock() (write lock),
// since this may delete from the map.
func getExistingItem(key string) (CacheItem, bool) {
	item, exists := redisMap[key]
	if !exists {
		return CacheItem{}, false
	}
	if IsExpired(item) {
		delete(redisMap, key)
		return CacheItem{}, false
	}
	return item, true
}

func encodeBulkString(val string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
}

func encodeNullBulk() []byte {
	return []byte("$-1\r\n")
}

func encodeArray(values []string) []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
	for _, v := range values {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}
	return buf.Bytes()
}