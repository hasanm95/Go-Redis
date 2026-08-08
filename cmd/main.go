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

    switch cfg.Mode{
    case "master": 
        fmt.Printf("Starting as Master on port %s\n", cfg.Port)
        store.LoadFromDisk()

        if err := store.LoadAOF("aof.log"); err != nil {
            log.Fatal("failed to load AOF: ", err)
        }

        if err := store.InitAOF("aof.log"); err != nil {
            log.Fatal("failed to init AOF: ", err)
        }

        go store.StartPeriodicSave(done, saved)
        go store.StartActiveExpiry(done)
    case "replica":
        fmt.Printf("Starting as Replica on port %s\n", cfg.Port)
		store.SetReplicaMode(true)
        go server.StartReplica(cfg.MasterAddr)
        go store.StartActiveExpiry(done)
    default:
        log.Fatal("invalid mode")
    }

    fmt.Printf("Redis server starting at %s\n", cfg.Port)
    go server.ListerLoop(listener)

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt)

    <-sigChan

    if cfg.Mode == "master" {
        store.CloseAOF()
        close(done)
        <-saved
    } else {
        close(done)
    }
}

