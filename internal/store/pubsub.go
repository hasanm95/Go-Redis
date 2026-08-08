package store

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"slices"
	"sync"
)

var (
	channels = make(map[string][]net.Conn)
	channelsMutex sync.RWMutex
	connChannels = make(map[net.Conn][]string)
    connChannelsMutex sync.RWMutex
) 

func handleSubscription (cmds []string, conn net.Conn) []byte  {
	if len(cmds) < 2 {
		wrongArgsErr("SUBSCRIBE")
	}
	channelsMutex.Lock()
	connChannelsMutex.Lock()
	defer channelsMutex.Unlock()
	defer connChannelsMutex.Unlock()

	incomingChannels := cmds[1:]
	var buf bytes.Buffer

	for _, val := range incomingChannels {
		if !slices.Contains(channels[val], conn) {
			channels[val] = append(channels[val], conn)
		}
		if !slices.Contains(connChannels[conn], val) {
			connChannels[conn] = append(connChannels[conn], val)
		}

		buf.WriteString(fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(val), val, len(connChannels[conn])))
	}
	
	return buf.Bytes()
}

func handlePublish(cmds []string) []byte {
	if len(cmds) < 3 {
		return wrongArgsErr("PUBLISH")
	}

	channelsMutex.RLock()
	defer channelsMutex.RUnlock()

	key := cmds[1]
	message := cmds[2]

	conns := channels[key]; 
	connLength := len(conns)
	for _, conn := range conns {
		conn.Write([]byte(fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(message), message)))
	}

	return []byte(fmt.Sprintf(":%d\r\n", connLength))
}

func RemoveSubscriber(conn net.Conn) {
	log.Print("RemoveSubscriber called")
	channelsMutex.Lock()
	connChannelsMutex.Lock()
	defer channelsMutex.Unlock()
	defer connChannelsMutex.Unlock()

	currChannels, exists := connChannels[conn]
	if !exists {
		return
	}

	for _, chName := range currChannels {
		connections := channels[chName]
		
		connections = slices.DeleteFunc(connections, func(c net.Conn) bool {
			return c == conn
		})

		if len(connections) == 0 {
			delete(channels, chName)
		} else {
			channels[chName] = connections
		}
	}

	delete(connChannels, conn)
}
