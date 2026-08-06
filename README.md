# go-redis

A Redis-compatible in-memory key-value store built from scratch in Go — including a hand-written RESP parser, persistence, key expiry, and master/replica replication.

---

## How to Run

**Master:**
```bash
go run cmd/main.go --mode master --port 6380
```

**Replica:**
```bash
go run cmd/main.go --mode replica --port 6381 --masterAddr localhost:6380
```

**Connect with redis-cli:**
```bash
redis-cli -p 6380  # master
redis-cli -p 6381  # replica (read-only)
```

---

## Supported Commands

| Command | Description |
|---|---|
| `PING` | Returns PONG |
| `SET key value [EX seconds]` | Set a key with optional expiry |
| `GET key` | Get value by key |
| `DEL key` | Delete a key |
| `EXISTS key` | Check if a key exists |
| `TTL key` | Get remaining expiry in seconds |
| `INCR key` | Increment value by 1 |
| `DECR key` | Decrement value by 1 |
| `INCRBY key amount` | Increment value by amount |
| `DECRBY key amount` | Decrement value by amount |
| `MSET key1 val1 key2 val2 ...` | Set multiple keys at once |
| `MGET key1 key2 ...` | Get multiple keys at once |

---

## Project Structure

```
go-redis/
├── cmd/
│   └── main.go              # Entry point — starts the server
├── internal/
│   ├── config/
│   │   └── config.go        # CLI flags (mode, port, masterAddr)
│   ├── parser/
│   │   └── parser.go        # Hand-written RESP protocol parser
│   ├── server/
│   │   ├── server.go        # TCP listener, connection handler
│   │   └── replica.go       # Replica connection and command receiver
│   └── store/
│       ├── store.go         # In-memory map, expiry, persistence, replication
│       └── commands.go      # Command handler (SET, GET, DEL etc.)
├── go.mod
└── README.md
```

---

## Features

- **Hand-written RESP parser** — parses the Redis Serialization Protocol from raw TCP bytes without any library
- **Concurrent access** — all map operations protected with `sync.RWMutex`
- **Key expiry** — `EX` flag on `SET`, lazy deletion on `GET`, active expiry via background goroutine
- **Persistence** — periodic RDB-style snapshot to `dump.json`, loaded on startup
- **Graceful shutdown** — `Ctrl+C` triggers a final save before exit using Go channels
- **Replication** — master/replica setup over raw TCP; writes on master propagate to all replicas automatically; replicas are read-only

---

## What I Learned

- How Redis actually works under the hood — TCP server, RESP wire protocol, command parsing
- Go networking with the `net` package — `net.Listen`, `net.Dial`, `net.Conn`
- Goroutines and channels for concurrency — background tickers, graceful shutdown coordination, broadcast with `close(chan)`
- `sync.RWMutex` for safe concurrent map access
- Building a real project from scratch in Go without frameworks