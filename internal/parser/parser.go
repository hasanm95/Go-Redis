package parser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

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

func RedisParser(reader *bufio.Reader) ([]string, error) {
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