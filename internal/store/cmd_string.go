package store

import (
	"fmt"
	"strconv"
	"time"
)

func handleSet(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("SET")
	}

	item := CacheItem{Value: cmds[2]}

	if len(cmds) > 4 && cmds[3] == "EX" {
		ttlSecsInt, err := strconv.Atoi(cmds[4])
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		item.ExpiresAt = time.Now().Add(time.Duration(ttlSecsInt) * time.Second)
	}

	mapMutex.Lock()
	redisMap[cmds[1]] = item
	mapMutex.Unlock()

	return []byte("+OK\r\n")
}

func handleGet(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("GET")
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	data, exists := getExistingItem(cmds[1])
	if !exists {
		return encodeNullBulk()
	}

	if data.Type == "" || data.Type == "string" {
		return encodeBulkString(data.Value)
	}
	return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func handleMSet(cmds []string) []byte {
	if len(cmds) < 3 || (len(cmds)-1)%2 != 0 {
		return wrongArgsErr("MSET")
	}

	mapMutex.Lock()
	for i := 1; i < len(cmds); i += 2 {
		redisMap[cmds[i]] = CacheItem{Value: cmds[i+1]}
	}
	mapMutex.Unlock()

	return []byte("+OK\r\n")
}

func handleMGet(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("MGET")
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	values := make([][]byte, 0, len(cmds)-1)
	for i := 1; i < len(cmds); i++ {
		item, exists := getExistingItem(cmds[i])
		if !exists {
			values = append(values, encodeNullBulk())
		} else {
			if item.Type == "list"{
				values = append(values, encodeNullBulk())
			} else {
				values = append(values, encodeBulkString(item.Value))
			}
			
		}
	}

	result := []byte(fmt.Sprintf("*%d\r\n", len(values)))
	for _, v := range values {
		result = append(result, v...)
	}
	return result
}

func handleIncr(cmds []string) []byte  { return incrDecr(cmds, "INCR", 1) }
func handleDecr(cmds []string) []byte  { return incrDecr(cmds, "DECR", -1) }

func handleIncrBy(cmds []string) []byte { return incrDecrBy(cmds, "INCRBY", 1) }
func handleDecrBy(cmds []string) []byte { return incrDecrBy(cmds, "DECRBY", -1) }

func incrDecr(cmds []string, name string, sign int) []byte {
	if len(cmds) != 2 {
		return wrongArgsErr(name)
	}
	result, err := IncrementBy(cmds[1], sign)
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	return []byte(fmt.Sprintf(":%d\r\n", result))
}

func incrDecrBy(cmds []string, name string, sign int) []byte {
	if len(cmds) != 3 {
		return wrongArgsErr(name)
	}
	amount, err := strconv.Atoi(cmds[2])
	if err != nil {
		return []byte("-ERR amount is not an integer or out of range\r\n")
	}
	result, err := IncrementBy(cmds[1], sign*amount)
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	return []byte(fmt.Sprintf(":%d\r\n", result))
}

func replicaSet(cmds []string) {
	if len(cmds) < 3 {
		return
	}
	item := CacheItem{Value: cmds[2]}
	if len(cmds) > 4 && cmds[3] == "EX" {
		if ttl, err := strconv.Atoi(cmds[4]); err == nil {
			item.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
		}
	}
	mapMutex.Lock()
	redisMap[cmds[1]] = item
	mapMutex.Unlock()
}

func replicaMSet(cmds []string) {
	if len(cmds) < 3 {
		return
	}
	mapMutex.Lock()
	for i := 1; i < len(cmds); i += 2 {
		redisMap[cmds[i]] = CacheItem{Value: cmds[i+1]}
	}
	mapMutex.Unlock()
}

func replicaIncrDecr(cmds []string) {
	amount := 1
	if cmds[0] == "DECR" {
		amount = -1
	}
	if (cmds[0] == "INCRBY" || cmds[0] == "DECRBY") && len(cmds) == 3 {
		if n, err := strconv.Atoi(cmds[2]); err == nil {
			if cmds[0] == "DECRBY" {
				amount = -n
			} else {
				amount = n
			}
		}
	}
	IncrementBy(cmds[1], amount)
}