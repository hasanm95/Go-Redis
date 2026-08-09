package store

import (
	"fmt"
	"net"

	"github.com/hasanm95/go-redis/internal/parser"
)

var writeCommands = map[string]bool{
	"SET": true, "DEL": true, "MSET": true,
	"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	"EXPIRE": true, "PERSIST": true, "RENAME": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
}

func HandleCommands(cmds []string, conn net.Conn) []byte {
	if len(cmds) == 0 {
		return []byte("-ERR empty command\r\n")
	}
	command := cmds[0]

	if enqueueIfInTransaction(command, cmds, conn) {
		return []byte("+QUEUED\r\n")
	}

	if isReplica && writeCommands[command] {
		return []byte("-ERR this server is read-only\r\n")
	}

	var returnVal []byte

	switch command {
	case "COMMAND":
		returnVal = []byte("*0\r\n")
	case "PING":
		returnVal = []byte("+PONG\r\n")
	case "SET":
		returnVal = handleSet(cmds)
	case "GET":
		returnVal = handleGet(cmds)
	case "DEL", "DELETE":
		returnVal = handleDel(cmds)
	case "EXISTS":
		returnVal = handleExists(cmds)
	case "INCR":
		returnVal = handleIncr(cmds)
	case "DECR":
		returnVal = handleDecr(cmds)
	case "INCRBY":
		returnVal = handleIncrBy(cmds)
	case "DECRBY":
		returnVal = handleDecrBy(cmds)
	case "TTL":
		returnVal = handleTTL(cmds)
	case "MSET":
		returnVal = handleMSet(cmds)
	case "MGET":
		returnVal = handleMGet(cmds)
	case "EXPIRE":
		returnVal = handleExpire(cmds)
	case "PERSIST":
		returnVal = handlePersist(cmds)
	case "RENAME":
		returnVal = handleRename(cmds)
	case "TYPE":
		returnVal = handleType(cmds)
	case "LPUSH":
		returnVal = handleLPush(cmds)
	case "RPUSH":
		returnVal = handleRPush(cmds)
	case "LPOP":
		returnVal = handleLPop(cmds)
	case "RPOP":
		returnVal = handleRPop(cmds)
	case "LRANGE":
		returnVal = handleLRange(cmds)
	case "SUBSCRIBE":
		returnVal = handleSubscription(cmds, conn)
	case "PUBLISH":
		returnVal = handlePublish(cmds)
	case "UNSUBSCRIBE":
		returnVal = handleUnsubscription(cmds, conn)
	case "MULTI":
		returnVal = handleMulti(cmds, conn)
	case "DISCARD":
		returnVal = handleDiscard(conn)
	case "EXEC":
		returnVal = handleExec(conn)
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
	}

	// Propagate to replicas only for successful writes.
	// A failed write starts with '-' (RESP error) — don't propagate those.
	if writeCommands[command] && len(returnVal) > 0 && returnVal[0] != '-' {
		propagate(cmds)
		appendToAOF(cmds)
	}

	return returnVal
}

func propagate(cmds []string) {
	replicas := GetReplicas()
	if len(replicas) == 0 {
		return
	}
	encoded := parser.EncodeCommand(cmds)
	for _, replica := range replicas {
		replica.Write(encoded)
	}
}

func ReplicaExecute(cmds []string) {
	if len(cmds) == 0 {
		return
	}
	switch cmds[0] {
	case "SET":
		replicaSet(cmds)
	case "DEL":
		replicaDel(cmds)
	case "MSET":
		replicaMSet(cmds)
	case "INCR", "DECR", "INCRBY", "DECRBY":
		replicaIncrDecr(cmds)
	case "EXPIRE":
		replicaExpire(cmds)
	case "PERSIST":
		replicaPersist(cmds)
	case "RENAME":
		replicaRename(cmds)
	case "LPUSH":
		replicaLPush(cmds)
	case "RPUSH":
		replicaRPush(cmds)
	case "LPOP":
		replicaLPop(cmds)
	case "RPOP":
		replicaRPop(cmds)
	}
}