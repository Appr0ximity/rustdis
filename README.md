# rustdis

**rustdis** is a Redis-like in-memory (currently) data store written in Rust. The project aims to provide a fast, lightweight, and reliable key-value store with a focus on learning, experimentation, and extensibility. It is inspired by the design and features of Redis, but built from scratch using Rust for safety and performance.

## Project Goals
- Implement a core set of Redis-compatible commands and data structures
- Provide a simple TCP server for client connections
- Explore Rust's concurrency and networking capabilities
- Serve as a learning resource for systems programming and distributed systems

## Current Features
- Basic key-value storage
- Simple command parsing and response
- TCP server for client connections

## Planned Features (Not Yet Implemented)
- Lists
- Streams
- Transactions
- Replication
- RDB persistence
- Pub/Sub
- Sorted sets
- Geospatial commands
- Authentication

## Getting Started
1. Clone the repository
2. Build with Cargo: `cargo build`
3. Run the server: `cargo run`

## License
MIT License
