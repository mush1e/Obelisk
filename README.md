# Obelisk

A high-performance, fault-tolerant message broker built in Go that serves as the single source of truth for microservices, real-time systems, and event-driven architectures. Obelisk combines the reliability of Kafka with the simplicity of Redis, delivering enterprise-grade messaging with minimal operational overhead.

## 🚀 Features

### Core Messaging
- **Topic-based Architecture**: Organize messages into logical topics with independent storage and indexing
- **Binary Protocol**: Efficient binary serialization (~38 bytes vs 90 for JSON) for minimal network overhead
- **Offset-based Consumers**: Track message consumption progress with commit/rollback capabilities
- **At-least-once Delivery**: Guaranteed message delivery with crash recovery support

### Performance & Reliability
- **Batched Disk I/O**: Smart batching system (size + time triggers) minimizes disk operations
- **Memory-mapped Indexes**: Fast message lookup using offset-to-position mapping
- **Ring Buffer Caching**: In-memory buffers for recent messages enable sub-millisecond reads
- **Thread-safe Operations**: Concurrent producers and consumers with proper mutex protection

### Operational Excellence
- **Zero Configuration**: Works out of the box with sensible defaults
- **Graceful Shutdown**: Clean resource cleanup with proper signal handling
- **Topic Auto-creation**: Topics created automatically on first message
- **File-based Storage**: Simple, debuggable storage format with no external dependencies

## 🏗️ Architecture

### High-Level Overview
```
┌─────────────┐    TCP/Binary     ┌─────────────────┐
│  Producers  │ ─────────────────▶│  Obelisk Server │
│             │                   │                 │
└─────────────┘                   └─────────────────┘
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                    ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
                    │  Ring Buffers   │ │  Batch Manager  │ │ Topic Storage   │
                    │   (Fast Reads)  │ │ (Efficient I/O) │ │ (Persistence)   │
                    └─────────────────┘ └─────────────────┘ └─────────────────┘
                                                                      │
                                                            ┌─────────┼─────────┐
                                                            ▼         ▼         ▼
                                                    ┌──────────────────────────────┐
                                                    │      Per-Topic Storage       │
                                                    │  ┌──────────┐┌─────────────┐ │
                                                    │  │ .log     ││    .idx     │ │
                                                    │  │(messages)││(offset→pos) │ │
                                                    │  └──────────┘└─────────────┘ │
                                                    └──────────────────────────────┘
                                                                      │
                                                                      ▼
                                                            ┌─────────────────┐
                                                            │   Consumers     │
                                                            │ (Poll/Commit)   │
                                                            └─────────────────┘
```

### Message Flow Architecture
```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                OBELISK SERVER                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Producer Message ──▶ │ TCP Handler │ ──▶ │ Ring Buffer │                           │
│                                     │                   │                           │
│                                     └──▶ │ Topic Batcher │ ──▶ │ Disk Storage │     │
│                                                                │                    │
│                                                                │  topic-0.log       │
│                                                                │  topic-0.idx       │
│                                                                │  topic-1.log       │
│                                                                │  topic-1.idx       │
│                                                                                     │
│                                                                                     │
│  Consumer Poll Request ──▶ │ Storage Layer │ ──▶ │ Index Lookup │ ──▶ Response      │
│                                            │                                        │
│                                            └──▶  │ File Seek & Read │               │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Storage Layer Deep Dive
```
Topic: user-events
├── user-events.log (Binary Messages)
│   ┌──────────────────────────────────────┐
│   │ [Length][Timestamp][Topic][Key][Value] │ ← Message 0
│   │ [Length][Timestamp][Topic][Key][Value] │ ← Message 1  
│   │ [Length][Timestamp][Topic][Key][Value] │ ← Message 2
│   └──────────────────────────────────────┘
│
└── user-events.idx (Offset Index)
    ┌─────────────────────────────────────┐
    │ [0] → byte position 0               │
    │ [1] → byte position 156             │ 
    │ [2] → byte position 312             │
    └─────────────────────────────────────┘

Consumer tracks: "I'm at offset 1" → Index lookup → Seek to byte 156 → Read from there
```

## 🛠️ Installation & Setup

### Prerequisites
- Go 1.21 or higher
- 500MB disk space (for logs and indexes)

### Quick Start
```bash
# Clone the repository
git clone https://github.com/mush1e/obelisk.git
cd obelisk

# Install dependencies
go mod tidy

# Create data directories
mkdir -p data/topics

# Start the Obelisk server
go run cmd/Obelisk/main.go
```

Server starts on `:8080` and creates topic files in `data/topics/`

## 📚 Usage Examples

### Producer (Sending Messages)
```go
package main

import (
    "bufio"
    "net"
    "time"
    
    "github.com/mush1e/obelisk/internal/message"
    "github.com/mush1e/obelisk/pkg/protocol"
)

func main() {
    // Connect to Obelisk server
    conn, _ := net.Dial("tcp", "localhost:8080")
    defer conn.Close()
    
    writer := bufio.NewWriter(conn)
    
    // Create and send message
    msg := message.Message{
        Timestamp: time.Now(),
        Topic:     "user-events",
        Key:       "user123",
        Value:     "User logged in",
    }
    
    msgBytes, _ := message.Serialize(msg)
    protocol.WriteMessage(writer, msgBytes)
}
```

### Consumer (Reading Messages)
```go
package main

import (
    "fmt"
    "github.com/mush1e/obelisk/internal/consumer"
)

func main() {
    // Create consumer for topic
    consumer := consumer.NewConsumer("data/topics", "user-events")
    
    for {
        // Poll for new messages
        messages, _ := consumer.Poll("user-events")
        
        if len(messages) > 0 {
            // Process messages
            for _, msg := range messages {
                fmt.Printf("Processing: %s -> %s\n", msg.Key, msg.Value)
            }
            
            // Commit progress (enables crash recovery)
            offset, _ := consumer.GetCurrentOffset("user-events")
            consumer.Commit("user-events", offset + uint64(len(messages)))
        }
        
        time.Sleep(1 * time.Second)
    }
}
```

### Advanced Consumer Operations
```go
// Subscribe to multiple topics
consumer := consumer.NewConsumer("data/topics", "orders", "payments", "notifications")

// Get topic statistics
count, _ := consumer.GetTopicMessageCount("orders")
fmt.Printf("Total messages in orders: %d\n", count)

// Reset to replay all messages
consumer.Reset("orders")

// Check current position
offset, _ := consumer.GetCurrentOffset("orders")
fmt.Printf("Currently at offset: %d\n", offset)
```

## 🧪 Testing & Examples

### Built-in Test Clients

**Producer Test Client:**
```bash
# Send 150 messages quickly (tests batching)
go run cmd/test-client/main.go -test=size

# Send messages with delays (tests time-based flushing)  
go run cmd/test-client/main.go -test=time

# Realistic workload simulation
go run cmd/test-client/main.go -test=realistic
```

**Consumer Test Client:**
```bash
# Single poll (get messages once)
go run cmd/test-consumer/main.go -topic=topic-0 -mode=poll

# Continuous polling (keep checking for new messages)
go run cmd/test-consumer/main.go -topic=topic-1 -mode=continuous

# Reset and replay from beginning
go run cmd/test-consumer/main.go -topic=topic-2 -mode=reset
```

**Read All Messages:**
```bash
# Read all topics
go run cmd/test-reader/main.go

# Read specific directory
go run cmd/test-reader/main.go /path/to/topics
```

### End-to-End Test Flow
```bash
# Terminal 1: Start server
go run cmd/Obelisk/main.go

# Terminal 2: Send test messages
go run cmd/test-client/main.go -test=realistic

# Terminal 3: Start consumer
go run cmd/test-consumer/main.go -topic=topic-1 -mode=continuous

# Terminal 2: Send more messages (watch consumer pick them up)
go run cmd/test-client/main.go -test=size
```

## 🏢 Production Use Cases

### Real-time Event Processing
```
┌─────────────────┐    ┌─────────────┐    ┌─────────────────┐
│   Web App       │───▶│  Obelisk    │───▶│  Analytics      │
│ (User Actions)  │    │ Topic:      │    │  Service        │
│                 │    │ "clicks"    │    │                 │
└─────────────────┘    └─────────────┘    └─────────────────┘
```

### Microservices Coordination
```
┌─────────────────┐    ┌─────────────┐    ┌─────────────────┐
│   Order         │───▶│  Obelisk    │───▶│  Inventory      │
│   Service       │    │ Topic:      │    │  Service        │
│                 │    │ "orders"    │    │                 │
└─────────────────┘    └─────────────┘    └─────────────────┘
                                      └───▶┌─────────────────┐
                                           │  Payment        │
                                           │  Service        │
                                           └─────────────────┘
```

### IoT Data Collection
```
┌─────────────────┐    ┌─────────────┐    ┌─────────────────┐
│   1000s of      │───▶│  Obelisk    │───▶│  Time Series    │
│   Sensors       │    │ Topic:      │    │  Database       │
│                 │    │ "sensors"   │    │                 │
└─────────────────┘    └─────────────┘    └─────────────────┘
```


## 🗂️ Project Structure

```
obelisk/
├── cmd/
│   ├── Obelisk/main.go         # Server entry point
│   ├── test-client/main.go     # Producer test client
│   ├── test-consumer/main.go   # Consumer test client
│   └── test-reader/main.go     # Debug message reader
├── internal/
│   ├── batch/                  # Batched disk writes
│   │   └── batcher.go         # TopicBatcher with per-topic batching
│   ├── buffer/                 # Ring buffer for fast reads
│   │   ├── buffer.go          # TopicBuffers + Buffer implementation
│   │   └── buffer_test.go     # Unit tests
│   ├── consumer/               # Consumer API
│   │   └── consumer.go        # Poll/Commit/Reset functionality
│   ├── message/                # Message format
│   │   ├── message.go         # Binary serialization
│   │   └── message_test.go    # Serialization tests
│   ├── server/                 # TCP server
│   │   └── server.go          # Connection handling + routing
│   └── storage/                # Persistent storage
│       ├── storage.go         # Main storage interface
│       ├── index.go           # Offset-to-position indexing
│       └── storage_test.go    # Storage tests
├── pkg/
│   └── protocol/              # Wire protocol
│       └── protocol.go        # Length-prefixed binary protocol
├── data/topics/               # Topic storage (created at runtime)
│   ├── topic-0.log           # Message data
│   ├── topic-0.idx           # Offset index
│   ├── topic-1.log
│   └── topic-1.idx
├── go.mod
├── LICENSE
└── README.md
```

## 🛣️ Roadmap

### Phase 2: Core Features (Current) ✅
- [x] Topic-based messaging
- [x] Consumer offset tracking  
- [x] Indexed storage for fast seeks
- [x] Batched disk I/O
- [x] Ring buffer caching
- [x] Binary protocol optimization

### Phase 3: Enhanced APIs (Next)
- [ ] HTTP/REST API for easier integration
- [ ] gRPC support for high-performance clients
- [ ] Consumer groups (multiple consumers sharing work)
- [ ] Message retention policies
- [ ] Topic compaction
- [ ] Metrics and monitoring endpoints

### Phase 4: Distributed Systems (Future)
- [ ] Multi-node clustering
- [ ] Leader election and consensus
- [ ] Cross-datacenter replication
- [ ] Automatic partitioning
- [ ] Load balancing

### Phase 5: Enterprise Features (Future)
- [ ] Authentication and authorization
- [ ] TLS encryption
- [ ] Schema registry
- [ ] Dead letter queues
- [ ] Message tracing
- [ ] Backup and restore

## 🤝 Contributing

We welcome contributions! Here's how to get started:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** with tests
4. **Run the test suite**: `go test ./...`
5. **Commit your changes**: `git commit -m 'Add amazing feature'`
6. **Push to the branch**: `git push origin feature/amazing-feature`
7. **Open a Pull Request**

### Development Setup
```bash
# Install development dependencies
go mod tidy

# Run tests
go test ./...

# Run integration tests
make test-integration

# Start development server
go run cmd/Obelisk/main.go
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by Apache Kafka's distributed log architecture
- Binary protocol design influenced by Redis and MessagePack
- Batching strategy adapted from RocksDB and LevelDB

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/mush1e/obelisk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/mush1e/obelisk/discussions)
- **Documentation**: [Wiki](https://github.com/mush1e/obelisk/wiki)

---

**Obelisk: Standing tall as your distributed system's source of truth.** 🗿
