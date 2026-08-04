package store

import (
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