package store

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hasanm95/go-redis/internal/parser"
)

func handleDel(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("DEL")
	}

	count := 0
	mapMutex.Lock()
	for i := 1; i < len(cmds); i++ {
		if _, exists := redisMap[cmds[i]]; exists {
			delete(redisMap, cmds[i])
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
}

func handleExists(cmds []string) []byte {
	_, exists := redisMap[cmds[1]]
	if exists {
		return []byte(":1\r\n")
	}
	return []byte(":0\r\n")
}

func handleTTL(cmds []string) []byte {
	if len(cmds) != 2 {
		return wrongArgsErr("TTL")
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte(":-2\r\n")
	}
	if item.ExpiresAt.IsZero() {
		return []byte(":-1\r\n")
	}
	if IsExpired(item) {
		delete(redisMap, cmds[1])
		return []byte(":-2\r\n")
	}

	remainder := int(time.Until(item.ExpiresAt).Seconds())
	return []byte(fmt.Sprintf(":%d\r\n", remainder))
}

func handleExpire(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("EXPIRE")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	exTime, err := strconv.Atoi(cmds[2])
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte(":0\r\n")
	}

	item.ExpiresAt = time.Now().Add(time.Duration(exTime) * time.Second)
	redisMap[cmds[1]] = item

	return []byte(":1\r\n")
}

func handlePersist(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("PERSIST")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte(":0\r\n")
	}

	item.ExpiresAt = time.Time{}
	redisMap[cmds[1]] = item

	return []byte(":1\r\n")
}

func handleRename(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("RENAME")
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte("-ERR no such key\r\n")
	}

	redisMap[cmds[2]] = item
	delete(redisMap, cmds[1])

	return []byte("+OK\r\n")
}

func handleType(cmds []string) []byte {
	if len(cmds) < 2 {
		return wrongArgsErr("TYPE")
	}

	mapMutex.RLock()
	defer mapMutex.RUnlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return []byte("+none\r\n")
	}

	varType := "string"
	if item.Type == "list" {
		varType = "list"
	}
	return []byte(fmt.Sprintf("+%s\r\n", varType))
}

func replicaDel(cmds []string) {
	if len(cmds) < 2 {
		return
	}
	mapMutex.Lock()
	delete(redisMap, cmds[1])
	mapMutex.Unlock()
}

func replicaExpire(cmds []string) {
	if len(cmds) < 3 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	exTime, err := strconv.Atoi(cmds[2])
	if err != nil {
		return
	}
	item, exists := redisMap[cmds[1]]
	if !exists {
		return
	}
	item.ExpiresAt = time.Now().Add(time.Duration(exTime) * time.Second)
	redisMap[cmds[1]] = item
}

func replicaPersist(cmds []string) {
	if len(cmds) < 2 {
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()

	item, exists := redisMap[cmds[1]]
	if !exists {
		return
	}
	item.ExpiresAt = time.Time{}
	redisMap[cmds[1]] = item
}