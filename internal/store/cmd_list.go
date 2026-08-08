package store

import (
	"fmt"
	"slices"
	"strconv"
)

func handleLPush(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("LPUSH")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	values := cmds[2:]
	slices.Reverse(values)
	result := values

	if item, exists := redisMap[cmds[1]]; exists {
		result = append(result, item.ListValue...)
	}

	redisMap[cmds[1]] = CacheItem{ListValue: result, Type: "list"}
	return []byte(fmt.Sprintf("+%d\r\n", len(result)))
}

func handleRPush(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("RPUSH")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	result := make([]string, 0)
	if item, exists := redisMap[cmds[1]]; exists {
		result = append(result, item.ListValue...)
	}
	result = append(result, cmds[2:]...)

	redisMap[cmds[1]] = CacheItem{ListValue: result, Type: "list"}
	return []byte(fmt.Sprintf("+%d\r\n", len(result)))
}

func handleLPop(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("LPOP")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists || len(item.ListValue) < 1 {
		return encodeNullBulk()
	}

	popped := item.ListValue[0]
	remaining := item.ListValue[1:]

	if len(remaining) < 1 {
		delete(redisMap, cmds[1])
	} else {
		redisMap[cmds[1]] = CacheItem{ListValue: remaining, Type: "list"}
	}

	return []byte(fmt.Sprintf("+%s\r\n", popped))
}

func handleRPop(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("RPOP")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists || len(item.ListValue) < 1 {
		return encodeNullBulk()
	}

	lastIndex := len(item.ListValue) - 1
	popped := item.ListValue[lastIndex]
	remaining := item.ListValue[:lastIndex]

	if len(remaining) < 1 {
		delete(redisMap, cmds[1])
	} else {
		redisMap[cmds[1]] = CacheItem{ListValue: remaining, Type: "list"}
	}

	return []byte(fmt.Sprintf("+%s\r\n", popped))
}

func handleLRange(cmds []string) []byte {
	if len(cmds) < 4 {
		return wrongArgsErr("LRANGE")
	}
	mapMutex.RLock()
	defer mapMutex.RUnlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte("*0\r\n")
	}
	result := item.ListValue

	startIdx, err := strconv.Atoi(cmds[2])
	if err != nil {
		return []byte("-ERR failed to convert start index to int")
	}
	endIdx, err := strconv.Atoi(cmds[3])
	if err != nil {
		return []byte("-ERR failed to convert end index to int")
	}

	n := len(result)
	if startIdx < 0 {
		startIdx = n + startIdx
	}
	if endIdx < 0 {
		endIdx = n + endIdx
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx >= n {
		endIdx = n - 1
	}
	if startIdx > endIdx || startIdx >= n {
		return []byte("*0\r\n")
	}

	return encodeArray(result[startIdx : endIdx+1])
}

func replicaLPush(cmds []string) {
	if len(cmds) < 3 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	values := cmds[2:]
	slices.Reverse(values)
	result := values
	if item, exists := redisMap[cmds[1]]; exists {
		result = append(result, item.ListValue...)
	}
	redisMap[cmds[1]] = CacheItem{ListValue: result, Type: "list"}
}

func replicaRPush(cmds []string) {
	if len(cmds) < 3 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	result := make([]string, 0)
	if item, exists := redisMap[cmds[1]]; exists {
		result = append(result, item.ListValue...)
	}
	result = append(result, cmds[2:]...)
	redisMap[cmds[1]] = CacheItem{ListValue: result, Type: "list"}
}

func replicaLPop(cmds []string) {
	if len(cmds) < 2 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists || len(item.ListValue) < 1 {
		return
	}
	remaining := item.ListValue[1:]
	if len(remaining) < 1 {
		delete(redisMap, cmds[1])
	} else {
		redisMap[cmds[1]] = CacheItem{ListValue: remaining, Type: "list"}
	}
}

func replicaRPop(cmds []string) {
	if len(cmds) < 2 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists || len(item.ListValue) < 1 {
		return
	}
	lastIndex := len(item.ListValue) - 1
	remaining := item.ListValue[:lastIndex]
	if len(remaining) < 1 {
		delete(redisMap, cmds[1])
	} else {
		redisMap[cmds[1]] = CacheItem{ListValue: remaining, Type: "list"}
	}
}