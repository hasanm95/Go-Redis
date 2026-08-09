package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/hasanm95/go-redis/internal/parser"
)


var (
	aofFile  *os.File
	aofMutex sync.Mutex
	aofWG    sync.WaitGroup
)

func InitAOF(path string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	aofFile = f
	return nil
}

func appendToAOF(cmds []string) {
	if aofFile == nil {
		return
	}

	aofWG.Add(1)
	defer aofWG.Done()

	aofMutex.Lock()
	defer aofMutex.Unlock()

	encoded := parser.EncodeCommand(cmds)
	if _, err := aofFile.Write(encoded); err != nil {
		log.Printf("AOF write error: %v", err)
	}
}

func LoadAOF(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return os.ErrNotExist
	}
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	count := 0

	for {
		cmds, err := parser.RedisParser(reader)

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("AOF corrupted: %w", err)
		}
		
		ReplicaExecute(cmds)
		count++
	}

	log.Printf("AOF replay: applied %d commands", count)
	return nil
}

func CloseAOF() {
	aofWG.Wait()
	if aofFile != nil {
		aofFile.Close()
	}
}