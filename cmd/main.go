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
			log.Fatalf("err: %v", err)
			return;
		}

		returnVal := handleCommands(cmds)
		
		_, err = conn.Write(returnVal)

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}
var (
	redisMap = make(map[string]string)
	mapMutex sync.RWMutex 
)

func handleCommands(cmds []string) []byte{
	command := cmds[0]

	switch command {
	case "COMMAND":
	return []byte("*0\r\n") 
	case "PING":
		return []byte("+PONG\r\n")
	case "SET":
		if len(cmds) < 3 {
			return []byte("wrong number fo commds form 'SET' command")
		}
		mapMutex.Lock()
		redisMap[cmds[1]] = cmds[2]
		mapMutex.Unlock()
		return []byte("+OK\r\n")
	case "GET":
		if len(cmds) < 2 {
			return []byte("wrong number fo commds form 'GET' command")
		}

		mapMutex.RLock()
		data, exists := redisMap[cmds[1]]
		mapMutex.RUnlock()
		
		if !exists {
			return []byte("$-1\r\n") 
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
	default:
		fmt.Println("Unknown command")
	}
	return nil
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