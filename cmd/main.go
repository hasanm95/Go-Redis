package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/hasanm95/go-redis/internal/config"
	"github.com/hasanm95/go-redis/internal/server"
	"github.com/hasanm95/go-redis/internal/store"
)


func main(){
    cfg := config.SetupFlags()

    listener, err := net.Listen("tcp", ":"+cfg.Port)
    if err != nil {
        log.Fatal("error listening: ", err)
    }
    defer listener.Close()

    done := make(chan bool)
    saved := make(chan bool)

    if cfg.Mode == "master" {
        fmt.Printf("Starting as Master on port %s\n", cfg.Port)
        store.LoadFromDisk()
        go store.StartPeriodicSave(done, saved)
        go store.StartActiveExpiry(done)
    } else if cfg.Mode == "replica" {
        fmt.Printf("Starting as Replica on port %s\n", cfg.Port)
		store.SetReplicaMode(true)
        go server.StartReplica(cfg.MasterAddr)
        go store.StartActiveExpiry(done)
    } else {
        log.Fatal("invalid mode")
    }

    fmt.Printf("Redis server starting at %s\n", cfg.Port)
    go server.ListerLoop(listener)

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt)

    <-sigChan

    if cfg.Mode == "master" {
        close(done)
        <-saved
    } else {
        close(done)
    }
}

