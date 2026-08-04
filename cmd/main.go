package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
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
		}

		fmt.Println(cmds)

		_, err = conn.Write([]byte("+OK\r\n"))

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}

func getArrayLength(reader *bufio.Reader)(int, error) {
	prefix, err := reader.ReadByte()

	if err != nil {
		return 0, fmt.Errorf("Error reading first byte: %v", err)
	}

	if prefix != '*' {
		return 0, fmt.Errorf("invalid protocol: expected '*', got %c", prefix)
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

	_, err = reader.ReadBytes('\n')

	if err != nil {
		return "", fmt.Errorf("[CMD] Error reading length line: %v", err)
	}

	line, err := reader.ReadBytes('\n')

	if err != nil {
		return "", fmt.Errorf("[CMD] Error cmd lines: %v", err)
	}

	str := bytes.TrimSuffix(line, []byte("\r\n"))

	return string(str), nil
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
			fmt.Println("Error:", err)
		}

		parsedCmd = append(parsedCmd, token)
	}

	if err != nil {
		return nil, err
	}

	return parsedCmd, nil
}