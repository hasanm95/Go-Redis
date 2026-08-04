package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

func main(){
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal("error listening: ", err)
	}
	defer listener.Close()

	fmt.Println("Redis server starting at 6380")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting conn:", err)
            continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		cmds, err := redisParser(reader)

		if err != nil {
			log.Printf("err: %v", err)
			return;
		}

		returnVal := handleCommands(cmds)
		
		_, err = conn.Write(returnVal)

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}

type CacheItem struct {
	Value string
	ExpiresAt time.Time
}

var (
	redisMap = make(map[string]CacheItem)
	mapMutex sync.RWMutex 
)

func handleCommands(cmds []string) []byte{
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

		if isExpired(data) {
			delete(redisMap, cmds[1])
			return []byte("$-1\r\n")
		}

		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data.Value), data.Value))
	case "DELETE":
		_, exists := redisMap[cmds[1]]
		if exists {
			mapMutex.Lock()
			delete(redisMap, cmds[1])
			mapMutex.Unlock()
			return []byte(":1\r\n")
		} else {
			return []byte(":0\r\n")
		}
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

		result, err := incrementBy(key, +1)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		return []byte(fmt.Sprintf(":%d\r\n", result))

	case "DECR": 
		if len(cmds) != 2 {
			return []byte("-ERR wrong number of arguments for 'DECR' command\r\n")
		}
		key := cmds[1]
		result, err := incrementBy(key, -1)
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
		result, err := incrementBy(key, +amount)
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
		result, err := incrementBy(key, -amount)
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
		if isExpired(item) {
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
		// Increment by 2 to loop through key/value pairs sequentially
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

		requestedKeysCount := len(cmds) - 1
		var responseBuffer bytes.Buffer

		// 1. Write the RESP Array header based on how many keys were requested (e.g., *2\r\n)
		responseBuffer.WriteString(fmt.Sprintf("*%d\r\n", requestedKeysCount))

		mapMutex.Lock()
		defer mapMutex.Unlock()

		// 2. Loop through every requested key starting at index 1
		for i := 1; i < len(cmds); i++ {
			targetKey := cmds[i]
			item, exists := redisMap[targetKey]

			// Handle expired key
			if !exists || (isExpired(item)) {
				responseBuffer.WriteString("$-1\r\n")
			} else {
				responseBuffer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Value), item.Value))
			}

			if !exists {
				// Append Null Bulk String to array if key doesn't exist
				responseBuffer.WriteString("$-1\r\n")
			} else {
				// Append valid Bulk String payload
				responseBuffer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Value), item.Value))
			}
		}

		return responseBuffer.Bytes()

	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
	}
}

func incrementBy(key string, amount int) (int, error) {
		var currentNum int

		mapMutex.Lock()
		defer mapMutex.Unlock()

		data, exists := redisMap[key]

		if !exists {
			currentNum = 0
		} else {
			var err error
			currentNum, err = strconv.Atoi(data.Value)
			if err != nil {
				return 0, err
			}
		}
		currentNum = currentNum + amount
		redisMap[key] = CacheItem{
			Value: strconv.Itoa(currentNum),
			ExpiresAt: data.ExpiresAt,
		}
		return currentNum, nil
}

func isExpired(item CacheItem) bool {
    return !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt)
}

func getArrayLength(reader *bufio.Reader)(int, error) {
	prefix, err := reader.ReadByte()

	if err != nil {
		return 0, fmt.Errorf("[length] Error reading first byte: %v", err)
	}

	if prefix != '*' {
		return 0, fmt.Errorf("[length] invalid protocol: expected '*', got %c", prefix)
	}
	
	line, err := reader.ReadBytes('\n')

	if err != nil {
		return 0, fmt.Errorf("Error reading lines: %v", err)
	}

	line = bytes.TrimSuffix(line, []byte("\r\n"))

	length, err := strconv.Atoi(string(line))
	
	if err != nil {
		return 0, fmt.Errorf("Error convert to number: %v", err)
	}

	return length, nil
}

func parseCmd(reader *bufio.Reader) (string, error) {
	prefix, err := reader.ReadByte()

	if err != nil {
		return "", fmt.Errorf("[CMD] Error reading first byte: %v", err)
	}

	if prefix != '$' {
		return "", fmt.Errorf("[CMD] invalid protocol: expected '$', got %c", prefix)
	}

	lengthLine, err := reader.ReadBytes('\n')

	if err != nil {
		return "", fmt.Errorf("[CMD] error reading length bytes: %v", err)
	}

	lengthLine = bytes.TrimSuffix(lengthLine, []byte("\r\n"))

	length, err := strconv.Atoi(string(lengthLine))

	if err != nil {
		return "", fmt.Errorf("[CMD] error converting length to number: %v", err)
	}

	if length < 0 {
		return "", fmt.Errorf("[CMD] invalid length: %d", length)
	}

	payload := make([]byte, length)

	_, err = io.ReadFull(reader, payload)

	if err != nil {
		return "", fmt.Errorf("failed to read full payload: %v", err)
	}

	_, err = reader.Discard(2)

	if err != nil {
		return "", fmt.Errorf("failed to discard last 2 bytes: %v", err)
	}

	return string(payload), nil
}

func redisParser(reader *bufio.Reader) ([]string, error) {
	parsedCmd := []string{}

	length, err := getArrayLength(reader)

	if err != nil {
		return nil, err
	}

	for i := 0; i < length; i++ {
		token, err := parseCmd(reader)

		if err != nil {
			return nil, err
		}

		parsedCmd = append(parsedCmd, token)
	}

	return parsedCmd, nil
}