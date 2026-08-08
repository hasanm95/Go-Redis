package store

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hasanm95/go-redis/internal/parser"
)

func HandleCommands(cmds []string) []byte{
	if len(cmds) == 0 {
		return []byte("-ERR empty command\r\n")
	}
	command := cmds[0]

	writeCommands := map[string]bool{
		"SET": true, "DEL": true, "MSET": true,
		"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	}

	if isReplica && writeCommands[command] {
		return []byte("-ERR this server is read-only\r\n")
	}

	switch command {
	case "COMMAND":
	return []byte("*0\r\n") 
	case "PING":
		return []byte("+PONG\r\n")
	case "SET":
		if len(cmds) < 3 {
			return []byte("-ERR wrong number of arguments for 'SET' command\r\n")
		}

		item := CacheItem{
			Value: cmds[2],
		}

		if len(cmds) > 4 {
			expiryFlag := cmds[3]

			fmt.Println("expiryFlag", expiryFlag)

			if expiryFlag == "EX" {
				ttlSecs := cmds[4]
				ttlSecsInt, err := strconv.Atoi(ttlSecs)

				if err != nil {
					return []byte("-ERR value is not an integer or out of range\r\n")
				}

				item.ExpiresAt = time.Now().Add(time.Duration(ttlSecsInt) * time.Second)
			}
		}

		mapMutex.Lock()
		redisMap[cmds[1]] = item
		mapMutex.Unlock()

		replicas := GetReplicas()
		encoded := parser.EncodeCommand(cmds)
		for _, replica := range replicas {
			replica.Write(encoded)
		}

		return []byte("+OK\r\n")
	case "GET":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'GET' command\r\n")
		}

		mapMutex.Lock()
		defer mapMutex.Unlock()
		data, exists := redisMap[cmds[1]]
		
		if !exists {
			return []byte("$-1\r\n") 
		}

		if IsExpired(data) {
			delete(redisMap, cmds[1])
			return []byte("$-1\r\n")
		}

		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data.Value), data.Value))
	case "DEL", "DELETE":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'DEL' command\r\n")
		}

		count := 0
		mapMutex.Lock()

		for i := 1; i < len(cmds); i++ {
			targetKey := cmds[i] 
			
			if _, exists := redisMap[targetKey]; exists {
				delete(redisMap, targetKey)
				count++
			}
		}
		mapMutex.Unlock()

		replicas := GetReplicas()
		encoded := parser.EncodeCommand(cmds)
		for _, replica := range replicas {
			replica.Write(encoded)
		}

		return []byte(fmt.Sprintf(":%d\r\n", count))
	case "EXISTS":
		_, exists := redisMap[cmds[1]]
		if exists {
			return []byte(":1\r\n")
		} else {
			return []byte(":0\r\n")
		}
	case "INCR": 
		if len(cmds) != 2 {
			return []byte("-ERR wrong number of arguments for 'INCR' command\r\n")
		}
		key := cmds[1]

		result, err := IncrementBy(key, +1)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		return []byte(fmt.Sprintf(":%d\r\n", result))

	case "DECR": 
		if len(cmds) != 2 {
			return []byte("-ERR wrong number of arguments for 'DECR' command\r\n")
		}
		key := cmds[1]
		result, err := IncrementBy(key, -1)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		return []byte(fmt.Sprintf(":%d\r\n", result))
		
	case "INCRBY": 
		if len(cmds) != 3 {
			return []byte("-ERR wrong number of arguments for 'INCRBY' command\r\n")
		}
		key := cmds[1]
		amount, err := strconv.Atoi(cmds[2])
		if err != nil {
			return []byte("-ERR amoint is not an integer or out of range\r\n")
		}
		result, err := IncrementBy(key, +amount)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		return []byte(fmt.Sprintf(":%d\r\n", result))
	case "DECRBY": 
		if len(cmds) != 3 {
			return []byte("-ERR wrong number of arguments for 'DECRBY' command\r\n")
		}
		key := cmds[1]
		amount, err := strconv.Atoi(cmds[2])
		if err != nil {
			return []byte("-ERR amoint is not an integer or out of range\r\n")
		}
		result, err := IncrementBy(key, -amount)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		return []byte(fmt.Sprintf(":%d\r\n", result))

	case "TTL":
		if len(cmds) != 2 {
			return []byte("-ERR wrong number of arguments for 'TTL' command\r\n")
		}
		targetKey := cmds[1]

		mapMutex.Lock()
		defer mapMutex.Unlock()

		item, exists := redisMap[targetKey]

		if !exists {
			return []byte(":-2\r\n")
		}

		// Key exists but has no expiration set
		if item.ExpiresAt.IsZero() {
			return []byte(":-1\r\n") 
		}

		// Key exists but expired
		if IsExpired(item) {
			delete(redisMap, targetKey)
			return []byte(":-2\r\n")
		}

		// Calculate remaining seconds rounded down
		remainder := int(time.Until(item.ExpiresAt).Seconds())
		return []byte(fmt.Sprintf(":%d\r\n", remainder))
		
	case "MSET":
		if len(cmds) < 3 || (len(cmds)-1)%2 != 0 {
			return []byte("-ERR wrong number of arguments for 'MSET' command\r\n")
		}

		mapMutex.Lock()
		for i := 1; i < len(cmds); i += 2 {
			key := cmds[i]
			val := cmds[i+1]
			
			redisMap[key] = CacheItem{
				Value: val,
			}
		}
		mapMutex.Unlock()

		return []byte("+OK\r\n")
		
	case "MGET":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'MGET' command\r\n")
		}

		var responseBuffer bytes.Buffer
		requestedKeysCount := len(cmds) - 1

		// 1. Write the clean master array frame header
		responseBuffer.WriteString(fmt.Sprintf("*%d\r\n", requestedKeysCount))

		mapMutex.Lock()
		defer mapMutex.Unlock()

		// 2. Loop through every requested key using index variable 'i'
		for i := 1; i < len(cmds); i++ {
			targetKey := cmds[i]
			item, exists := redisMap[targetKey]

			// Passive Eviction Check
			if exists && IsExpired(item)  {
				delete(redisMap, targetKey)
				exists = false
			}

			if !exists {
				responseBuffer.WriteString("$-1\r\n")
			} else {
				// Isolate data explicitly with its exact specific string length
				responseBuffer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Value), item.Value))
			}
		}

		// 3. Export the clean buffer back to the connection line
		return responseBuffer.Bytes()

	case "EXPIRE":
		if len(cmds) < 3 {
			return []byte("-ERR wrong number of arguments for 'EXPIRE' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()
		key := cmds[1]
		exTime, err := strconv.Atoi(cmds[2])

		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		item, exists := redisMap[key]

		if !exists {
			return []byte(":0\r\n")
		}

		redisMap[key] = CacheItem{
			Value: item.Value,
			ExpiresAt: time.Now().Add(time.Duration(exTime) * time.Second),
		}

		return []byte(":1\r\n")

	case "PERSIST":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'PERSIST' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1]

		item, exists := redisMap[key]

		if !exists {
			return []byte(":0\r\n")
		}

		redisMap[key] = CacheItem{
			Value: item.Value,
			ExpiresAt: time.Time{},
		}

		return []byte(":1\r\n")

	case "RENAME":
		if len(cmds) < 3 {
			return []byte("-ERR wrong number of arguments for 'RENAME' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		oldKey := cmds[1]
		newKey := cmds[2]

		item, exists := redisMap[oldKey]

		if !exists {
			return []byte("-ERR no such key\r\n")
		}

		redisMap[newKey] = item
		delete(redisMap, oldKey)

		return []byte("+OK\r\n")

	case "TYPE":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'TYPE' command\r\n")
		}

		mapMutex.RLock()
		defer mapMutex.RUnlock()
		key := cmds[1]

		if item, exists := redisMap[key]; exists {
			return []byte(fmt.Sprintf("+%T\r\n", item.Value))
		}

		return []byte("+none\r\n")

	case "LPUSH":
		if len(cmds) < 3 {
			return []byte("-ERR wrong number of arguments for 'LPUSH' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1];
		values := cmds[2:]
		slices.Reverse(values)
		result := values

		if item, exists := redisMap[key]; !exists {
			redisMap[key] = CacheItem{
				ListValue: result, 
				Type:      "list",
			}
		} else {
			result = append(result, item.ListValue...)
		}

		redisMap[key] = CacheItem{
			ListValue: result,
			Type: "list",
		}

		return []byte(fmt.Sprintf("+%d\r\n", len(result)))

	case "RPUSH":
		if len(cmds) < 3 {
			return []byte("-ERR wrong number of arguments for 'RPUSH' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1];
		values := cmds[2:]
		result := make([]string, 0)

		if item, exists := redisMap[key]; !exists {
			redisMap[key] = CacheItem{
				ListValue: result, 
				Type:      "list",
			}
		} else {
			result = append(result, item.ListValue...)
		}

		result = append(result, values...)

		redisMap[key] = CacheItem{
			ListValue: result,
			Type: "list",
		}

		return []byte(fmt.Sprintf("+%d\r\n", len(result)))

	case "LPOP":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'LPOP' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1]
		result := make([]string, 0)

		if item, exists := redisMap[key]; !exists {
			return []byte("$-1\r\n")
		} else {
			result = append(result, item.ListValue...)
		}

		if len(result) < 1 {
			return []byte("$-1\r\n")
		}

		poppedValue := result[0];
		result = result[1:]
		length := len(result)


		if length < 1 {
			delete(redisMap, key)
		} else {
			redisMap[key] = CacheItem{
				ListValue: result,
				Type: "list",
			}
		}

		return []byte(fmt.Sprintf("+%s\r\n", poppedValue))

	case "RPOP":
		if len(cmds) < 2 {
			return []byte("-ERR wrong number of arguments for 'RPOP' command\r\n")
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1]
		result := make([]string, 0)

		if item, exists := redisMap[key]; !exists {
			return []byte("$-1\r\n")
		} else {
			result = append(result, item.ListValue...)
		}

		if len(result) < 1 {
			return []byte("$-1\r\n")
		}

		lastIndex := len(result) - 1
		poppedValue := result[lastIndex];
		result = result[:lastIndex]

		if len(result) < 1 {
			delete(redisMap, key)
		} else {
			redisMap[key] = CacheItem{
				ListValue: result,
				Type: "list",
			}
		}

		return []byte(fmt.Sprintf("+%s\r\n", poppedValue))	

	case "LRANGE":
		if len(cmds) < 4 {
			return []byte("-ERR wrong number of arguments for 'LRANGE' command\r\n")
		}
		mapMutex.RLock()
		defer mapMutex.RUnlock()

		key := cmds[1]
		result := make([]string, 0)
		var responseBuffer bytes.Buffer

		if item, exists := redisMap[key]; !exists {
			return []byte("*0\r\n")
		} else {
			result = append(result, item.ListValue...)
		}

		startIdxStr := cmds[2]
		endIdxStr := cmds[3]

		startIdx, err := strconv.Atoi(startIdxStr)

		if err != nil {
			return []byte("-ERR failed to convert start index to int")
		}

		endIdx, err := strconv.Atoi(endIdxStr)

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
			return []byte("$-1\r\n")
		}
	
		result = result[startIdx:endIdx+1]

		responseBuffer.WriteString(fmt.Sprintf("*%d\r\n", len(result)))

		for _, val := range result {
			responseBuffer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		}

		return responseBuffer.Bytes()

	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
	}
}

func ReplicaExecute(cmds []string) {
	if len(cmds) == 0 {
		return
	}

	command := cmds[0]

	switch command {
	case "SET":
		if len(cmds) < 3 {
			return
		}
		item := CacheItem{Value: cmds[2]}
		if len(cmds) > 4 && cmds[3] == "EX" {
			ttl, err := strconv.Atoi(cmds[4])
			if err == nil {
				item.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
			}
		}
		mapMutex.Lock()
		redisMap[cmds[1]] = item
		mapMutex.Unlock()

	case "DEL":
		if len(cmds) < 2 {
			return
		}
		mapMutex.Lock()
		delete(redisMap, cmds[1])
		mapMutex.Unlock()

	case "MSET":
		if len(cmds) < 3 {
			return
		}
		mapMutex.Lock()
		for i := 1; i < len(cmds); i += 2 {
			redisMap[cmds[i]] = CacheItem{Value: cmds[i+1]}
		}
		mapMutex.Unlock()

	case "INCR", "DECR", "INCRBY", "DECRBY":
		amount := 1
		if command == "DECR" {
			amount = -1
		}
		if (command == "INCRBY" || command == "DECRBY") && len(cmds) == 3 {
			n, err := strconv.Atoi(cmds[2])
			if err == nil {
				if command == "DECRBY" {
					amount = -n
				} else {
					amount = n
				}
			}
		}
		IncrementBy(cmds[1], amount)

	case "EXPIRE":
		if len(cmds) < 3 {
			return
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()
		key := cmds[1]
		exTime, err := strconv.Atoi(cmds[2])

		if err != nil {
			return
		}

		item, exists := redisMap[key]

		if !exists {
			return
		}

		redisMap[key] = CacheItem{
			Value: item.Value,
			ExpiresAt: time.Now().Add(time.Duration(exTime) * time.Second),
		}

	case "PERSIST":
		if len(cmds) < 2 {
			return
		}
		mapMutex.Lock()
		defer mapMutex.Unlock()

		key := cmds[1]

		item, exists := redisMap[key]

		if !exists {
			return
		}

		redisMap[key] = CacheItem{
			Value: item.Value,
			ExpiresAt: time.Time{},
		}

		return
	}
}