# Obelisk

A high-performance, fault-tolerant, distributed event log system designed to be the single source of truth for microservices, real-time systems, and event-driven architectures.

## Overview

Obelisk is a distributed event log system built in Go that provides reliable message storage and retrieval capabilities. It implements a commit log architecture with efficient binary serialization, ring buffer memory management, and persistent storage designed for high-throughput scenarios.

## Features

- **High Performance**: Efficient binary serialization/deserialization for minimal overhead
- **Ring Buffer Architecture**: Memory-efficient circular buffer for recent message caching
- **Persistent Storage**: Append-only log files with length-prefixed message format
- **Fault Tolerant**: Designed for reliability and data consistency
- **Event-Driven**: Perfect for microservices and real-time event processing
- **Single Source of Truth**: Centralized event logging for distributed systems

## Architecture

Obelisk consists of several core components:

### Message System
- **Timestamp**: Nanosecond precision using Unix time
- **Key-Value Structure**: Flexible message format with string keys and values
- **Binary Serialization**: Compact binary format using little-endian encoding

### Storage Engine
- **Append-Only Logs**: Immutable log segments stored on disk
- **Length-Prefixed Format**: Each message prefixed with 4-byte length header
- **Sequential Access**: Optimized for high-throughput sequential writes

### Buffer Management
- **Ring Buffer**: Fixed-size circular buffer for recent messages
- **Automatic Overflow**: Oldest messages automatically evicted when buffer fills
- **O(1) Operations**: Constant time push and peek operations

## Installation

```bash
git clone https://github.com/mush1e/obelisk.git
cd obelisk
go mod tidy
```

## Usage

### Basic Example

```go
package main

import (
    "time"
    "github.com/mush1e/obelisk/internal/message"
    "github.com/mush1e/obelisk/internal/storage"
    "github.com/mush1e/obelisk/internal/buffer"
)

func main() {
    // Create a message
    msg := message.Message{
        Timestamp: time.Now(),
        Key:       "user123",
        Value:     "Hello, Obelisk!",
    }
    
    // Store to disk
    storage.AppendMessage("data/segments/app.log", msg)
    
    // Use ring buffer for recent messages
    buf := buffer.NewBuffer(1000)
    buf.Push(msg)
    
    // Retrieve recent messages
    recent := buf.GetRecent()
}
```

### Message Serialization

```go
// Serialize message to binary
data, err := message.Serialize(msg)
if err != nil {
    log.Fatal(err)
}

// Deserialize from binary
restoredMsg, err := message.Deserialize(data)
if err != nil {
    log.Fatal(err)
}
```

### Persistent Storage

```go
// Append messages to log file
err := storage.AppendMessage("mylog.log", msg)
if err != nil {
    log.Fatal(err)
}

// Read all messages from log file
messages, err := storage.ReadAllMessages("mylog.log")
if err != nil {
    log.Fatal(err)
}
```

### Ring Buffer Operations

```go
// Create buffer with capacity of 100
buf := buffer.NewBuffer(100)

// Add messages
buf.Push(message1)
buf.Push(message2)

// Peek at oldest message
oldest := buf.Peek()

// Get all recent messages
allRecent := buf.GetRecent()
```

## Running the Example

```bash
# Create data directory
mkdir -p data/segments

# Run the main example
go run cmd/Obelisk/main.go
```

This will demonstrate:
- Binary serialization performance
- File-based message storage
- Ring buffer overflow behavior

## File Structure

```
obelisk/
├── cmd/Obelisk/           # Main application entry point
├── internal/
│   ├── buffer/            # Ring buffer implementation
│   ├── message/           # Message types and serialization
│   └── storage/           # Persistent storage operations
├── data/segments/         # Log file storage directory
├── .gitignore
├── LICENSE
└── README.md
```

## Performance Characteristics

- **Serialization**: Compact binary format with minimal overhead
- **Storage**: Append-only design optimized for write-heavy workloads
- **Memory**: Ring buffer provides O(1) access to recent messages
- **Scalability**: Designed for high-throughput distributed environments

## Use Cases

- **Event Sourcing**: Store and replay application events
- **Message Queuing**: Reliable message delivery between services  
- **Audit Logging**: Immutable log of system events
- **Real-time Analytics**: Stream processing of live events
- **Microservices Communication**: Inter-service event coordination
- **Data Replication**: Event-driven data synchronization

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Roadmap

- [ ] Distributed consensus implementation
- [ ] HTTP/gRPC API server
- [ ] Message compaction and cleanup
- [ ] Multi-segment log management  
- [ ] Replication and clustering
- [ ] Metrics and monitoring
- [ ] Client libraries for multiple languages

## Author

**Mustafa Siddiqui** - [@mush1e](https://github.com/mush1e)

---

*Obelisk: Standing tall as your distributed system's source of truth.*
