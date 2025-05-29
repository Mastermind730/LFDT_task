# Raft Consensus Algorithm Implementation in Go

![Go Version](https://img.shields.io/badge/go-1.16+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A simplified implementation of the Raft consensus algorithm in pure Go, demonstrating leader election, log replication, and fault tolerance in distributed systems.

## Features

- 🚀 Leader election with randomized timeouts
- 📜 Log replication and commitment
- 💪 Crash fault tolerance (handles node failures)
- 🌐 HTTP-based communication between nodes
- 📊 Real-time logging of cluster state changes
- 🔧 Configurable cluster size and timing parameters

## Prerequisites

- Go 1.16 or higher
- Visual Studio Code (or any Go-compatible IDE)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/go-raft-impl.git
cd go-raft-impl