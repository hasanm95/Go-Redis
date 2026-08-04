package main

import (
	"fmt"
	"log"
	"net"

	"github.com/hasanm95/go-redis/internal/server"
)

func main(){
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal("error listening: ", err)
	}
	defer listener.Close()

	fmt.Println("Redis server starting at 6380")

	server.ListerLoop(listener)
}

