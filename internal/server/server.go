package server

import (
	"bufio"
	"log"
	"net"

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
	for {
		cmds, err := parser.RedisParser(reader)

		if err != nil {
			log.Printf("err: %v", err)
			return;
		}

		returnVal := store.HandleCommands(cmds)
		
		_, err = conn.Write(returnVal)

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}