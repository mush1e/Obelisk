# Obelisk

A high-performance, fault-tolerant message broker built in Go that serves as the single source of truth for microservices, real-time systems, and event-driven architectures. Obelisk combines the reliability of Kafka with the simplicity of Redis, delivering enterprise-grade messaging with minimal operational overhead.

## 🚀 Features

### Core Messaging
- **Topic-based Architecture**: Organize messages into logical topics with independent storage and indexing
- **Binary Protocol**: Efficient binary serialization (~38 bytes vs 90 for JSON) for minimal network overhead
- **Dual Interface**: TCP for high-performance message ingestion, HTTP for monitoring and administration
- **At-least-once Delivery**: Guaranteed message delivery with crash recovery support

### Performance & Reliability
- **Batched Disk I/O**: Smart batching system (size + time triggers) minimizes disk operations
- **Memory-mapped Indexes**: Fast message lookup using offset-to-position mapping
- **Ring Buffer Caching**: In-memory buffers for recent messages enable sub-millisecond reads
- **Thread-safe Operations**: Concurrent producers and consumers with proper mutex protection
- **File Pooling**: Efficient file descriptor management with automatic cleanup

### Operational Excellence
- **Zero Configuration**: Works out of the box with sensible defaults
- **Graceful Shutdown**: Clean resource cleanup with proper signal handling
- **Topic Auto-creation**: Topics created automatically on first message
- **File-based Storage**: Simple, debuggable storage format with no external dependencies
- **Comprehensive Health Monitoring**: Kubernetes-ready health checks with component-level status
- **Prometheus Metrics**: Built-in monitoring and observability

### Advanced Features
- **Corruption Recovery**: Intelligent data corruption detection and recovery
- **Retry Mechanisms**: Configurable retry policies for transient failures
- **Error Categorization**: Sophisticated error handling with proper error types
- **Connection Management**: Active connection tracking and error handling

## 🏗️ Architecture

### High-Level Overview
```
┌─────────────┐        TCP/Binary      ┌─────────────────┐
│  Producers  │ ──────────────────────▶│  Obelisk Server │
│             │                        │                 │
└─────────────┘                        └─────────────────┘
                                                 │
                              ┌──────────────────┼───────────────────┐
                              ▼                  ▼                   ▼
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

Server starts on:
- **TCP Server**: `:8080` (message ingestion)
- **HTTP Server**: `:8081` (REST API, health checks, metrics)

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
    
    // Read acknowledgment
    response := make([]byte, 3)
    conn.Read(response)
    // Response: "OK\n" for success, "NACK:reason" for failure
}
```

### HTTP API Usage

#### Health Checks
```bash
# Overall system health
curl http://localhost:8081/health

# Kubernetes readiness probe
curl http://localhost:8081/health/ready

# Kubernetes liveness probe
curl http://localhost:8081/health/live

# Prometheus metrics
curl http://localhost:8081/metrics
```

#### Health Response Example
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "uptime": "2h15m30s",
  "components": {
    "buffer": {
      "status": "healthy",
      "details": {
        "success_rate": 0.98,
        "threshold": 0.95,
        "operation_count": 15420
      },
      "last_check": "2024-01-15T10:30:00Z"
    },
    "batcher": {
      "status": "healthy",
      "details": {
        "success_rate": 0.99,
        "last_flush": "2024-01-15T10:29:55Z",
        "threshold": 0.95,
        "operation_count": 15420
      },
      "last_check": "2024-01-15T10:30:00Z"
    }
  },
  "summary": {
    "total_components": 4,
    "healthy": 4,
    "degraded": 0,
    "unhealthy": 0
  }
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

## 📊 Monitoring & Observability

### Prometheus Metrics
The server exposes comprehensive metrics at `/metrics`:

- **Message Throughput**: `obelisk_messages_received_total`, `obelisk_messages_stored_total`
- **Performance**: `obelisk_batch_size`, `obelisk_flush_duration_seconds`
- **System Health**: `obelisk_health_status`, `obelisk_component_health`
- **Connection Stats**: `obelisk_active_connections`, `obelisk_connections_total`
- **Error Tracking**: `obelisk_connection_errors_total`, `obelisk_messages_failed_total`

### Health Monitoring
- **Liveness Probe** (`/health/live`): Basic service availability
- **Readiness Probe** (`/health/ready`): Service ready to accept traffic
- **Health Check** (`/health`): Comprehensive system health status

### Component Health Tracking
- **Buffer Health**: Success rate and operation count
- **Batcher Health**: Flush success rate and timing
- **Storage Health**: File system accessibility
- **TCP Server Health**: Connection status and listener health

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
│   ├── errors/                 # Error handling and categorization
│   │   └── errors.go          # Error types and retry logic
│   ├── handlers/               # HTTP request handlers
│   │   ├── health.go          # Health check endpoints
│   │   ├── messages.go        # Message REST API
│   │   ├── stats.go           # Statistics endpoints
│   │   └── middleware.go      # HTTP middleware
│   ├── health/                 # Health tracking system
│   │   ├── tracker.go         # Health status tracking
│   │   └── ring.go            # Health history ring buffer
│   ├── message/                # Message format
│   │   ├── message.go         # Binary serialization
│   │   └── message_test.go    # Serialization tests
│   ├── metrics/                # Prometheus metrics
│   │   └── metrics.go         # Metric definitions
│   ├── retry/                  # Retry mechanisms
│   │   └── retry.go           # Configurable retry policies
│   ├── server/                 # Server infrastructure
│   │   ├── server.go          # Main server orchestration
│   │   ├── tcp_server.go      # TCP message handling
│   │   └── http_server.go     # HTTP REST API
│   ├── services/               # Business logic services
│   │   └── broker_service.go  # Core broker functionality
│   └── storage/                # Persistent storage
│       ├── storage.go         # Main storage interface
│       ├── filepool.go        # File descriptor pooling
│       └── corruption_test.go # Corruption recovery tests
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
- [x] HTTP REST API
- [x] Comprehensive health monitoring
- [x] Prometheus metrics
- [x] Error categorization and retry logic
- [x] File pooling and corruption recovery

### Phase 3: Enhanced APIs (Next)
- [ ] gRPC support for high-performance clients
- [ ] Consumer groups (multiple consumers sharing work)
- [ ] Message retention policies
- [ ] Topic compaction
- [ ] Advanced filtering and routing

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

# Run specific test suites
go test ./internal/storage -v
go test ./internal/buffer -v

# Start development server
go run cmd/Obelisk/main.go
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by Apache Kafka's distributed log architecture
- Binary protocol design influenced by Redis and MessagePack
- Batching strategy adapted from RocksDB and LevelDB
- Health monitoring patterns from Kubernetes and modern microservices

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/mush1e/obelisk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/mush1e/obelisk/discussions)
- **Documentation**: [Wiki](https://github.com/mush1e/obelisk/wiki)

---

**Obelisk: Standing tall as your distributed system's source of truth.** 🗿
