package main

import (
	"fmt"
	"log"
	"net"
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
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)	
		if err != nil {
			log.Printf("Read error: %v", err)
			return
		}
		fmt.Println(string(buf[:n]))
		_, err = conn.Write([]byte("+OK\r\n"))

		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}