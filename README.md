# DynamoDB Distributed System Implementation

## Team Project - Distributed Systems Case Study

This project implements core distributed systems concepts inspired by DynamoDB:
- **Leader Election** using Bully Algorithm
- **Mutual Exclusion** using Ricart-Agrawala Algorithm
- **Deadlock Detection** using DAG (Wait-For Graph) with DFS Cycle Detection
- **Data Replication** across all nodes
- **Communication** using GoRPC

---

## Project Structure

```
dynamo-go/
├── config.json              # Node configuration (IPs, ports)
├── main.go                  # Node entry point (initializes all managers)
├── go.mod                   # Go module file
│
├── node/
│   ├── node.go              # Node structure, Lamport clock, interfaces
│   └── rpc_handlers.go      # All RPC methods (storage, election, mutex, DAG)
│
├── election/
│   └── bully.go             # Bully Algorithm implementation
│
├── mutex/
│   └── ricart_agrawala.go   # Ricart-Agrawala implementation
│
├── deadlock/
│   └── dag.go               # DAG Wait-For Graph deadlock detection
│
├── client/
│   └── client.go            # CLI client
│
└── testfiles/
    ├── test.txt             # Basic test file
    ├── data1.txt            # Mutual exclusion demo
    ├── data2.txt            # Mutual exclusion demo
    └── bigdata.txt          # Deadlock demo
```

---

## How to Run

### Step 1: Start 3 Nodes (3 Terminals)

Open 3 terminals. In each terminal, navigate to the project folder.

**Terminal 1:**
```bash
go run main.go 1
```

**Terminal 2:**
```bash
go run main.go 2
```

**Terminal 3:**
```bash
go run main.go 3
```

Wait ~3 seconds for leader election to complete. Node 3 becomes leader (highest ID).

### Step 2: Run Client (4th Terminal)

**Terminal 4:**
```bash
go run ./client/client.go
```

---

## Demo Commands

### Use Case 1: Distributed Storage (with Replication)
```
> put 1 myfile testfiles/test.txt
> get 2 myfile
```
Store on Node 1, retrieve from Node 2. Works because data is replicated automatically.

### Use Case 2: Leader Election (Bully Algorithm)
```
> status
```
Shows Node 3 as leader. Kill Node 3 (Ctrl+C in Terminal 3). Run `status` again to see Node 2 become leader.

### Use Case 3: Mutual Exclusion (Ricart-Agrawala)
```
> mutex-put sharedfile testfiles/data1.txt
```
Uses Ricart-Agrawala to request critical section from all nodes before writing.

### Use Case 4: Deadlock Detection (DAG)
```
> deadlock-test inventory
```
Creates circular resource dependency, detects cycle in Wait-For Graph, resolves deadlock.

---

## Configuration

Edit `config.json` to change node IPs:

**For local testing (3 terminals on 1 laptop):**
```json
{
    "nodes": [
        {"id": 1, "ip": "127.0.0.1", "port": 8001},
        {"id": 2, "ip": "127.0.0.1", "port": 8002},
        {"id": 3, "ip": "127.0.0.1", "port": 8003}
    ]
}
```

**For 3 actual laptops (update IPs to match your WiFi):**
```json
{
    "nodes": [
        {"id": 1, "ip": "192.168.1.101", "port": 8001},
        {"id": 2, "ip": "192.168.1.102", "port": 8002},
        {"id": 3, "ip": "192.168.1.103", "port": 8003}
    ]
}
```

---

## Algorithms Implemented

### 1. Bully Algorithm (Leader Election)
- Node with highest ID becomes leader
- Heartbeat every 3 seconds
- If leader dies, election triggers automatically
- File: `election/bully.go`

### 2. Ricart-Agrawala (Mutual Exclusion)
- Uses Lamport timestamps for ordering
- Node must get permission from all alive nodes before writing
- Lower timestamp = higher priority
- Deferred replies for concurrent requests
- File: `mutex/ricart_agrawala.go`

### 3. DAG / Wait-For Graph (Deadlock Detection)
- Directed graph tracks "who waits for whom"
- DFS algorithm detects cycles in the graph
- Cycle = Deadlock
- Resolution: abort one transaction to break the cycle
- File: `deadlock/dag.go`

---

## Real-World Mapping

This system maps to a **Distributed Inventory System for E-Commerce**:

- Node 1 = Mumbai Warehouse Server
- Node 2 = Delhi Warehouse Server
- Node 3 = Bangalore Warehouse Server

**Use Cases:**
1. Store/retrieve inventory data across warehouses (replicated)
2. One warehouse acts as coordinator via leader election
3. Mutual exclusion prevents conflicting inventory updates
4. Deadlock detection ensures system doesn't freeze when multiple warehouses compete for same resources

---

## Team Members

- Member 1: [Name] - [Roll Number]
- Member 2: [Name] - [Roll Number]
- Member 3: [Name] - [Roll Number]
- Member 4: [Name] - [Roll Number]
