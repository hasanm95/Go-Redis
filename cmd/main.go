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

		if !data.ExpiresAt.IsZero() && time.Now().After(data.ExpiresAt) {
			delete(redisMap, cmds[1])
			return []byte("$-1\r\n")
		}

		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data.Value), data.Value))
	case "DELETE":
		_, exists := redisMap[cmds[1]]
		if exists {
			delete(redisMap, cmds[1])
			return []byte(":1\r\n")
		} else {
			return []byte(":0\r\n")
		}
		
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
	}
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