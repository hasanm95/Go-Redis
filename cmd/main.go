package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/hasanm95/go-redis/internal/server"
	"github.com/hasanm95/go-redis/internal/store"
)

func main(){
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal("error listening: ", err)
	}
	defer listener.Close()

	store.LoadFromDisk()

  	done := make(chan bool)
    saved := make(chan bool)
	go store.StartPeriodicSave(done, saved)
	go store.StartActiveExpiry(done)

	fmt.Println("Redis server starting at 6380")
	go server.ListerLoop(listener)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	<-sigChan 
	close(done)
	<- saved
}

