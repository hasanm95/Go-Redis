package store

import "fmt"

func HandleCommands(cmds []string) []byte {
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
		return handleSet(cmds)
	case "GET":
		return handleGet(cmds)
	case "DEL", "DELETE":
		return handleDel(cmds)
	case "EXISTS":
		return handleExists(cmds)
	case "INCR":
		return handleIncr(cmds)
	case "DECR":
		return handleDecr(cmds)
	case "INCRBY":
		return handleIncrBy(cmds)
	case "DECRBY":
		return handleDecrBy(cmds)
	case "TTL":
		return handleTTL(cmds)
	case "MSET":
		return handleMSet(cmds)
	case "MGET":
		return handleMGet(cmds)
	case "EXPIRE":
		return handleExpire(cmds)
	case "PERSIST":
		return handlePersist(cmds)
	case "RENAME":
		return handleRename(cmds)
	case "TYPE":
		return handleType(cmds)
	case "LPUSH":
		return handleLPush(cmds)
	case "RPUSH":
		return handleRPush(cmds)
	case "LPOP":
		return handleLPop(cmds)
	case "RPOP":
		return handleRPop(cmds)
	case "LRANGE":
		return handleLRange(cmds)
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmds[0]))
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
	}
}