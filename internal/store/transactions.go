package store

import (
	"bytes"
	"fmt"
	"net"
	"sync"
)

type txState struct {
    inTransaction bool
    queued        [][]string
}

var (
    transactions      = make(map[net.Conn]*txState)
    transactionsMutex sync.Mutex
)

func handleMulti(cmds []string, conn net.Conn) []byte {
	transactionsMutex.Lock()
	defer transactionsMutex.Unlock()

	command := cmds[0]

	if _, exists := transactions[conn]; !exists {
		fmt.Println("command", command)
		transactions[conn] = &txState{
			inTransaction: true,
		}
	} else {
		fmt.Println("command else", command)
		if command == "MULTI" {
			return []byte("-ERR MULTI calls can not be nested\r\n")
		}
	}

	return []byte("+OK\r\n")
}

func enqueueIfInTransaction(command string, cmds []string, conn net.Conn) bool {
	transactionsMutex.Lock()
	defer transactionsMutex.Unlock()
	if txState, exists := transactions[conn]; exists && txState.inTransaction {
		if command != "MULTI" && command != "EXEC" && command != "DISCARD" {
			txState.queued = append(txState.queued, cmds)
			return true
		}
	}
	return false
}

func handleDiscard(conn net.Conn) []byte {
	transactionsMutex.Lock()
	defer transactionsMutex.Unlock()

	if _, exists := transactions[conn]; exists {
		delete(transactions, conn)
		return []byte("+OK\r\n")
	}

	return []byte("-ERR DISCARD without MULTI\r\n")
}

func handleExec(conn net.Conn) []byte {
	transactionsMutex.Lock()

	txState, exists := transactions[conn]
	if !exists {
		transactionsMutex.Unlock()
		return []byte("-ERR EXEC without MULTI\r\n")
	}

	queuedItems := txState.queued
	delete(transactions, conn)
	transactionsMutex.Unlock() 

	result := make([]string, 0, len(queuedItems))
	
	for _, item := range queuedItems {
		returnVal := HandleCommands(item, conn)
		result = append(result, string(returnVal))
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(result)))
	
	for _, val := range result {
		buf.WriteString(val)
	}

	return buf.Bytes()
}
