package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type CacheItem struct {
	Value string
	ExpiresAt time.Time
}

var (
	redisMap = make(map[string]CacheItem)
	mapMutex sync.RWMutex 
)


var (
	replicas []net.Conn
	replicaMutex sync.RWMutex 
)

var isReplica bool

func IncrementBy(key string, amount int) (int, error) {
		var currentNum int

		mapMutex.Lock()
		defer mapMutex.Unlock()

		data, exists := redisMap[key]

		if !exists {
			currentNum = 0
		} else {
			var err error
			currentNum, err = strconv.Atoi(data.Value)
			if err != nil {
				return 0, err
			}
		}
		currentNum = currentNum + amount
		redisMap[key] = CacheItem{
			Value: strconv.Itoa(currentNum),
			ExpiresAt: data.ExpiresAt,
		}
		return currentNum, nil
}

func IsExpired(item CacheItem) bool {
    return !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt)
}

func SaveToDisk(){
	mapMutex.RLock()
	defer mapMutex.RUnlock()
	fileData, err := json.MarshalIndent(redisMap, "", "    ")
	filename := "dump.json"

	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}
	err = os.WriteFile(filename, fileData, 0644)

	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}

	log.Printf("JSON file created successfully!")
}

func LoadFromDisk() {
	filename := "dump.json"
	_, err := os.Stat(filename)
	
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("File '%s' does not exist.\n", filename)
		return
	} else if err != nil {
		fmt.Printf("Error checking file: %v\n", err)
		return
	}

	fmt.Printf("File '%s' exists! Proceeding to read...\n", filename)


	mapMutex.Lock()
	defer mapMutex.Unlock()

	fileData, err := os.ReadFile(filename)

	if err != nil {
		fmt.Println("Error reading JSON:", err)
		return
	}

	err = json.Unmarshal(fileData, &redisMap)

	if err != nil {
		fmt.Println("Error writing redis map:", err)
		return
	}

	fmt.Println("Load from JSON file successfully!")
}

func StartPeriodicSave(done <- chan bool, saved chan <- bool){
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			SaveToDisk()
			fmt.Println("Ticker stopped.")
			saved <- true
			return
		case <- ticker.C:
			SaveToDisk()
		}
	}
}

func StartActiveExpiry(done <-chan bool) {
	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			fmt.Println("Start Active Expiry Ticker stopped.")
			return
		case <- ticker.C:
			removeExpiredKeys()
		}
	}
}

func removeExpiredKeys(){
	mapMutex.Lock()
	defer mapMutex.Unlock()

	for key, val := range redisMap {
		if IsExpired(val) {
			delete(redisMap, key)
		}
	}
}

func AddReplica(conn net.Conn){
	replicaMutex.Lock()
	defer replicaMutex.Unlock()

	replicas = append(replicas, conn)
}

func GetReplicas() []net.Conn{
	replicaMutex.RLock()
	defer replicaMutex.RUnlock()

	return replicas
}

func RemoveReplica(conn net.Conn) {
    replicaMutex.Lock()
    defer replicaMutex.Unlock()

    newReplicas := []net.Conn{}
    for _, r := range replicas {
        if r != conn {
            newReplicas = append(newReplicas, r)
        }
    }
    replicas = newReplicas
}

func SetReplicaMode(val bool) {
    isReplica = val
}