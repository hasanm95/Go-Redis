package store

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

func HandleCommands(cmds []string) []byte{
	if len(cmds) == 0 {
		return []byte("-ERR empty command\r\n")
	}
	command := cmds[0]

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
			if exists && !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
				delete(redisMap, targetKey)
				exists = false
			}

			if !exists {
				responseBuffer.WriteString("$-1\r\n")
			} else {
				// 🌟 FIX: Isolate data explicitly with its exact specific string length
				responseBuffer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Value), item.Value))
			}
		}

		// 3. Export the clean buffer back to the connection line
		return responseBuffer.Bytes()


	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
	}
}
