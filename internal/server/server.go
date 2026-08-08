package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/hasanm95/go-redis/internal/parser"
	"github.com/hasanm95/go-redis/internal/store"
)

func ListerLoop(listener net.Listener) {
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

    peek, _ := reader.Peek(7)
    
	if strings.TrimSpace(string(peek)) == "REPLICA" {
		reader.ReadBytes('\n')
		store.AddReplica(conn)
		
		buf := make([]byte, 1)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				store.RemoveReplica(conn)
				return
			}
		}
	}

	for {
		cmds, err := parser.RedisParser(reader)

		if err != nil {
			store.RemoveSubscriber(conn)
			log.Printf("err: %v", err)
			return;
		}

		returnVal := store.HandleCommands(cmds, conn)
		
		_, err = conn.Write(returnVal)

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}

func StartReplica(masterAddr string) {
	conn, err := net.Dial("tcp", masterAddr)

	if err != nil {
		log.Fatal("Failed to connect with master")
		return
	}
	fmt.Printf("Connected to master: %s\n", masterAddr)

	_, err = conn.Write([]byte("REPLICA\r\n"))

	if err != nil {
		log.Fatalf("Failed to ping from replica %v", err)
		return
	}

	reader := bufio.NewReader(conn)

	for {
		cmds, err := parser.RedisParser(reader)

		if err != nil {
			log.Printf("err: %v", err)
			return;
		}

		store.ReplicaExecute(cmds)
	}
}