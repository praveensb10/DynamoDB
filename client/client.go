package main

import (
	"bufio"
	"dynamo-go/node"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

var config *node.Config

func main() {
	// Load configuration - try current dir first, then parent dir
	var err error
	config, err = node.LoadConfig("config.json")
	if err != nil {
		config, err = node.LoadConfig("../config.json")
		if err != nil {
			fmt.Printf("Failed to load config: %v\n", err)
			fmt.Println("Make sure config.json is in the current or parent directory")
			os.Exit(1)
		}
	}

	fmt.Println("====================================")
	fmt.Println("   DISTRIBUTED SYSTEM CLIENT")
	fmt.Println("   DynamoDB-style Key-Value Store")
	fmt.Println("====================================")
	fmt.Println("")
	fmt.Println("Basic Operations:")
	fmt.Println("  put <node_id> <key> <filename>     - Store file (replicated)")
	fmt.Println("  get <node_id> <key>                - Retrieve file")
	fmt.Println("  delete <node_id> <key>             - Delete key")
	fmt.Println("  list <node_id>                     - List keys on node")
	fmt.Println("  status                             - Show all nodes status")
	fmt.Println("  status <node_id>                   - Show specific node")
	fmt.Println("  partition-info <node_id> <key>     - Show partition ownership")
	fmt.Println("  quorum-get <key>                   - Quorum read + read-repair")
	fmt.Println("")
	fmt.Println("Mutual Exclusion (Auto):")
	fmt.Println("  mutex-put <key> <filename>         - Full auto Ricart-Agrawala")
	fmt.Println("")
	fmt.Println("Mutual Exclusion (Manual Step-by-Step):")
	fmt.Println("  mutex-request <node_id>            - Step 1: Request CS")
	fmt.Println("  mutex-write <node_id> <key> <file> - Step 2: Write inside CS")
	fmt.Println("  mutex-release <node_id>            - Step 3: Release CS")
	fmt.Println("")
	fmt.Println("Deadlock (Auto):")
	fmt.Println("  deadlock-test <resource>           - Full auto deadlock demo")
	fmt.Println("  deadlock-scenario                  - Alice & Bob scenario")
	fmt.Println("")
	fmt.Println("Deadlock (Manual Step-by-Step):")
	fmt.Println("  deadlock-reset                     - Clear all DAG state")
	fmt.Println("  deadlock-lock <node_id> <resource> - Lock a resource")
	fmt.Println("  deadlock-unlock <node_id> <res>    - Unlock a resource")
	fmt.Println("  deadlock-show                      - Show Wait-For Graph")
	fmt.Println("  deadlock-detect                    - Run cycle detection")
	fmt.Println("  deadlock-resolve <node_id>         - Abort node to resolve")
	fmt.Println("")
	fmt.Println("Other:")
	fmt.Println("  help  - Show commands    exit  - Quit")
	fmt.Println("====================================")
	fmt.Println("")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]

		switch command {
		case "put":
			handlePut(parts)
		case "get":
			handleGet(parts)
		case "quorum-get":
			handleQuorumGet(parts)
		case "delete":
			handleDelete(parts)
		case "status":
			handleStatus(parts)
		case "list":
			handleList(parts)
		case "partition-info":
			handlePartitionInfo(parts)
		// Auto mutex
		case "mutex-put":
			handleMutexPut(parts)
		// Manual mutex
		case "mutex-request":
			handleMutexRequest(parts)
		case "mutex-write":
			handleMutexWrite(parts)
		case "mutex-release":
			handleMutexRelease(parts)
		// Auto deadlock
		case "deadlock-test":
			handleDeadlockTest(parts)
		case "deadlock-scenario":
			handleDeadlockScenario(parts)
		// Manual deadlock
		case "deadlock-reset":
			handleDeadlockReset(parts)
		case "deadlock-lock":
			handleDeadlockLock(parts)
		case "deadlock-unlock":
			handleDeadlockUnlock(parts)
		case "deadlock-show":
			handleDeadlockShow(parts)
		case "deadlock-detect":
			handleDeadlockDetect(parts)
		case "deadlock-resolve":
			handleDeadlockResolve(parts)
		// Legacy (keep for compatibility)
		case "deadlock-create":
			handleDeadlockCreate(parts)
		case "help":
			printHelp()
		case "exit", "quit":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Type 'help' for available commands")
		}

		fmt.Println("")
	}
}

func connectToNode(nodeID int) (*rpc.Client, error) {
	for _, nc := range config.Nodes {
		if nc.ID == nodeID {
			address := fmt.Sprintf("%s:%d", nc.IP, nc.Port)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return nil, fmt.Errorf("cannot connect to Node %d at %s: %v", nodeID, address, err)
			}
			return client, nil
		}
	}
	return nil, fmt.Errorf("node %d not found in config", nodeID)
}

func findLeader() (int, *rpc.Client) {
	for _, nc := range config.Nodes {
		client, err := connectToNode(nc.ID)
		if err != nil {
			continue
		}

		statusArgs := &node.StatusArgs{}
		var statusReply node.StatusReply
		err = client.Call("Node.Status", statusArgs, &statusReply)
		if err != nil {
			client.Close()
			continue
		}

		if statusReply.IsLeader {
			return nc.ID, client
		}
		client.Close()
	}
	return -1, nil
}

// ===== Basic Operations =====

func handlePut(parts []string) {
	if len(parts) < 4 {
		fmt.Println("Usage: put <node_id> <key> <filename>")
		fmt.Println("Example: put 1 myfile testfiles/test.txt")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	key := parts[2]
	filename := parts[3]

	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Cannot read file '%s': %v\n", filename, err)
		return
	}

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	args := &node.PutArgs{
		Key:       key,
		Value:     data,
		Timestamp: time.Now().UnixNano(),
		IsReplica: false,
	}
	var reply node.PutReply

	fmt.Printf("Storing '%s' -> '%s' on Node %d (with replication)...\n", key, filename, nodeID)

	err = client.Call("Node.Put", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Printf("SUCCESS: %s\n", reply.Message)
		if reply.Replicated {
			fmt.Println("Data replicated to all other nodes")
		}
	} else {
		fmt.Printf("FAILED: %s\n", reply.Message)
	}
}

func handleGet(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: get <node_id> <key>")
		fmt.Println("Example: get 2 myfile")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	key := parts[2]

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	args := &node.GetArgs{Key: key}
	var reply node.GetReply

	fmt.Printf("Retrieving '%s' from Node %d...\n", key, nodeID)

	err = client.Call("Node.Get", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Found {
		outputFile := key + "_downloaded.txt"
		err = os.WriteFile(outputFile, reply.Value, 0644)
		if err != nil {
			fmt.Printf("Cannot save file: %v\n", err)
			return
		}
		fmt.Printf("SUCCESS: Downloaded to '%s' (%d bytes)\n", outputFile, len(reply.Value))
		fmt.Printf("Content preview: %s\n", string(reply.Value[:min(len(reply.Value), 200)]))
	} else {
		fmt.Printf("NOT FOUND: %s\n", reply.Message)
	}
}

func handleQuorumGet(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: quorum-get <key>")
		fmt.Println("Example: quorum-get myfile")
		return
	}

	key := parts[1]

	//Connect to ANY node to find actual owners
	// (any node can answer this since all have same ring)
	client, err := connectToNode(config.Nodes[0].ID)
	if err != nil {
		fmt.Printf("Cannot connect to get partition info: %v\n", err)
		return
	}

	//Ask who actually OWNS this key
	partArgs := &node.PartitionInfoArgs{Key: key}
	var partReply node.PartitionInfoReply
	err = client.Call("Node.GetPartitionInfo", partArgs, &partReply)
	client.Close()
	if err != nil {
		fmt.Printf("Cannot get partition info: %v\n", err)
		return
	}

	// partReply.Owners = [Node2, Node4, Node1] (actual owners)
	if len(partReply.Owners) == 0 {
		fmt.Println("No owners found for this key")
		return
	}

	//Read from first 2 ACTUAL OWNERS
	// R = 2 from the real owners
	readCount := 2
	if len(partReply.Owners) < 2 {
		readCount = len(partReply.Owners)
	}
	nodeIDs := partReply.Owners[:readCount]
	// nodeIDs = [Node2, Node4] ← actual owners!

	fmt.Printf("Key '%s' owned by nodes: %v\n", key, partReply.Owners)
	fmt.Printf("Reading from nodes: %v (R=%d)\n", nodeIDs, readCount)

	replies := make([]node.GetReply, readCount)
	founds := make([]bool, readCount)

	//Try next owner if a node fails (fallback)
	for i, nid := range nodeIDs {
		client, err := connectToNode(nid)
		if err != nil {
			fmt.Printf("Node %d unreachable, trying next owner...\n", nid)

			//Try next owner instead of giving up!
			if len(partReply.Owners) > readCount {
				fallbackNode := partReply.Owners[readCount]
				fmt.Printf("Falling back to Node %d\n", fallbackNode)
				client, err = connectToNode(fallbackNode)
				if err != nil {
					fmt.Printf("Fallback Node %d also unreachable\n", fallbackNode)
					continue
				}
				nodeIDs[i] = fallbackNode // update to fallback node
			} else {
				continue // no more owners to try
			}
		}

		args := &node.GetArgs{Key: key}
		var reply node.GetReply
		err = client.Call("Node.Get", args, &reply)
		client.Close()
		if err != nil {
			fmt.Printf("RPC Error from Node %d: %v\n", nid, err)
			continue
		}
		replies[i] = reply
		founds[i] = reply.Found
	}

	//Same timestamp comparison
	var selected node.GetReply
	var selectedNode int

	if founds[0] && founds[1] {
		if replies[0].Timestamp >= replies[1].Timestamp {
			selected = replies[0]
			selectedNode = nodeIDs[0]
		} else {
			selected = replies[1]
			selectedNode = nodeIDs[1]
		}
	} else if founds[0] {
		selected = replies[0]
		selectedNode = nodeIDs[0]
	} else if founds[1] {
		selected = replies[1]
		selectedNode = nodeIDs[1]
	} else {
		fmt.Println("NOT FOUND on any quorum node")
		return
	}

	//Read repair on actual owners (not random nodes)
	for i, nid := range nodeIDs {
		if !founds[i] || replies[i].Timestamp < selected.Timestamp {
			fmt.Printf("Read-repairing Node %d (older/missing)\n", nid)
			client, err := connectToNode(nid)
			if err != nil {
				fmt.Printf("Cannot connect for repair to Node %d\n", nid)
				continue
			}
			putArgs := &node.PutArgs{
				Key:       key,
				Value:     selected.Value,
				Timestamp: selected.Timestamp,
				IsReplica: true, //IsReplica=true to prevent re-replication
			}
			var putReply node.PutReply
			client.Call("Node.Put", putArgs, &putReply)
			client.Close()
			fmt.Printf("Repaired Node %d to timestamp %d\n", nid, selected.Timestamp)
		}
	}

	// Save and return result
	outputFile := key + "_downloaded.txt"
	err = os.WriteFile(outputFile, selected.Value, 0644)
	if err != nil {
		fmt.Printf("Cannot save file: %v\n", err)
		return
	}
	fmt.Printf("SUCCESS: Downloaded freshest version from Node %d\n", selectedNode)
	fmt.Printf("Saved to '%s' (%d bytes)\n", outputFile, len(selected.Value))
	fmt.Printf("Content preview: %s\n",
		string(selected.Value[:min(len(selected.Value), 200)]))
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func handleDelete(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: delete <node_id> <key>")
		fmt.Println("Example: delete 1 myfile")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	key := parts[2]

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	args := &node.DeleteArgs{Key: key}
	var reply node.DeleteReply

	fmt.Printf("Deleting '%s' from Node %d...\n", key, nodeID)

	err = client.Call("Node.Delete", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Printf("SUCCESS: %s\n", reply.Message)
	} else {
		fmt.Printf("FAILED: %s\n", reply.Message)
	}
}

func handleStatus(parts []string) {
	if len(parts) >= 2 {
		nodeID, err := strconv.Atoi(parts[1])
		if err != nil {
			fmt.Println("Invalid node ID")
			return
		}
		showNodeStatus(nodeID)
	} else {
		fmt.Println("====================================")
		fmt.Println("       CLUSTER STATUS")
		fmt.Println("====================================")

		for _, nc := range config.Nodes {
			showNodeStatus(nc.ID)
		}

		fmt.Println("====================================")
	}
}

func showNodeStatus(nodeID int) {
	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Printf("Node %d: OFFLINE (cannot connect)\n", nodeID)
		return
	}
	defer client.Close()

	args := &node.StatusArgs{}
	var reply node.StatusReply

	err = client.Call("Node.Status", args, &reply)
	if err != nil {
		fmt.Printf("Node %d: ERROR (%v)\n", nodeID, err)
		return
	}

	role := "Follower"
	if reply.IsLeader {
		role = "LEADER"
	}

	fmt.Printf("Node %d: ONLINE | Role: %s | Leader: Node %d | Clock: %d | Data: %d items\n",
		reply.NodeID, role, reply.LeaderID, reply.LamportClock, reply.DataCount)
}

func handleList(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: list <node_id>")
		fmt.Println("Example: list 1")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	args := &node.ListKeysArgs{}
	var reply node.ListKeysReply

	err = client.Call("Node.ListKeys", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if len(reply.Keys) == 0 {
		fmt.Printf("Node %d has no stored keys\n", nodeID)
	} else {
		fmt.Printf("Keys on Node %d:\n", nodeID)
		for _, key := range reply.Keys {
			fmt.Printf("  - %s\n", key)
		}
	}
}

func handlePartitionInfo(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: partition-info <node_id> <key>")
		fmt.Println("Example: partition-info 1 mykey")
		fmt.Println("Shows which nodes are responsible for storing a key")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	key := parts[2]

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	args := &node.PartitionInfoArgs{Key: key}
	var reply node.PartitionInfoReply

	err = client.Call("Node.GetPartitionInfo", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	fmt.Println("====================================")
	fmt.Printf("Partition Info for Key: '%s'\n", key)
	fmt.Println("====================================")
	fmt.Printf("Partitioning Enabled: %v\n", reply.IsPartitioned)
	fmt.Printf("Replication Factor: %d\n", reply.ReplicationFactor)

	if len(reply.Owners) > 0 {
		fmt.Printf("Primary Owner: Node %d\n", reply.Primary)
		if len(reply.Replicas) > 0 {
			fmt.Print("Replicas: ")
			for i, replicaID := range reply.Replicas {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Printf("Node %d", replicaID)
			}
			fmt.Println()
		}
	}

	fmt.Printf("\n%s\n", reply.Message)
	fmt.Println("====================================")
}

// ===== Auto Mutex (Ricart-Agrawala) =====

func handleMutexPut(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: mutex-put <key> <filename>")
		fmt.Println("This stores data using mutual exclusion (Ricart-Agrawala) — FULLY AUTO")
		fmt.Println("Example: mutex-put sharedfile testfiles/data1.txt")
		return
	}

	key := parts[1]
	filename := parts[2]

	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Cannot read file '%s': %v\n", filename, err)
		return
	}

	fmt.Println("====================================")
	fmt.Println("  MUTUAL EXCLUSION WRITE (AUTO)")
	fmt.Println("  Algorithm: Ricart-Agrawala")
	fmt.Println("====================================")
	fmt.Printf("Key: %s\n", key)
	fmt.Printf("File: %s (%d bytes)\n", filename, len(data))
	fmt.Println("")

	// Find leader node
	leaderID, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found. Please ensure nodes are running.")
		return
	}
	defer leaderClient.Close()

	fmt.Printf("Leader found: Node %d\n", leaderID)
	fmt.Println("Sending MutexPut request (Ricart-Agrawala will run on server)...")
	fmt.Println("")

	args := &node.MutexPutArgs{
		Key:       key,
		Value:     data,
		Timestamp: time.Now().UnixNano(),
	}
	var reply node.MutexPutReply

	err = leaderClient.Call("Node.MutexPut", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Println("SUCCESS!")
		fmt.Printf("Result: %s\n", reply.Message)
		fmt.Println("")
		fmt.Println("What happened on the server:")
		fmt.Println("  1. Node requested Critical Section from all other nodes")
		fmt.Println("  2. Used Lamport timestamps for priority (lower = higher priority)")
		fmt.Println("  3. All alive nodes granted permission")
		fmt.Println("  4. Data written inside Critical Section")
		fmt.Println("  5. Data replicated to all nodes")
		fmt.Println("  6. Critical Section released")
		fmt.Println("  7. Deferred requests processed")
	} else {
		fmt.Printf("FAILED: %s\n", reply.Message)
	}

	fmt.Println("====================================")
}

// ===== Manual Mutex (Step-by-Step Ricart-Agrawala) =====

func handleMutexRequest(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: mutex-request <node_id>")
		fmt.Println("Step 1: Request Critical Section from all other nodes")
		fmt.Println("Example: mutex-request 4")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	fmt.Println("====================================")
	fmt.Println("  STEP 1: REQUEST CRITICAL SECTION")
	fmt.Println("  Algorithm: Ricart-Agrawala")
	fmt.Println("====================================")
	fmt.Printf("Node %d sending CS REQUEST to all other nodes...\n", nodeID)
	fmt.Println("Each node will GRANT or DEFER based on Lamport timestamps:")
	fmt.Println("  - GRANT: if not requesting CS, or requesting with higher timestamp")
	fmt.Println("  - DEFER: if requesting CS with lower timestamp (higher priority)")
	fmt.Println("")

	args := &node.MutexRequestArgs{
		Timestamp: time.Now().UnixNano(),
	}
	var reply node.MutexRequestReply

	err = client.Call("Node.MutexRequest", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Println("✓ " + reply.Message)
		fmt.Println("")
		fmt.Println("Next steps:")
		fmt.Printf("  mutex-write %d <key> <filename>  — write data inside CS\n", nodeID)
		fmt.Printf("  mutex-release %d                 — release CS when done\n", nodeID)
	} else {
		fmt.Println("✗ " + reply.Message)
	}
	fmt.Println("====================================")
}

func handleMutexWrite(parts []string) {
	if len(parts) < 4 {
		fmt.Println("Usage: mutex-write <node_id> <key> <filename>")
		fmt.Println("Step 2: Write data while inside Critical Section")
		fmt.Println("Example: mutex-write 4 sharedfile testfiles/data1.txt")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	key := parts[2]
	filename := parts[3]

	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Cannot read file '%s': %v\n", filename, err)
		return
	}

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	fmt.Println("====================================")
	fmt.Println("  STEP 2: WRITE INSIDE CRITICAL SECTION")
	fmt.Println("====================================")
	fmt.Printf("Writing key '%s' from file '%s' (%d bytes) on Node %d...\n", key, filename, len(data), nodeID)
	fmt.Println("")

	args := &node.MutexWriteArgs{
		Key:       key,
		Value:     data,
		Timestamp: time.Now().UnixNano(),
	}
	var reply node.MutexWriteReply

	err = client.Call("Node.MutexWrite", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Println("✓ " + reply.Message)
		fmt.Println("")
		fmt.Println("Data written & replicated while other nodes are BLOCKED from writing.")
		fmt.Printf("Next: mutex-release %d\n", nodeID)
	} else {
		fmt.Println("✗ " + reply.Message)
	}
	fmt.Println("====================================")
}

func handleMutexRelease(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: mutex-release <node_id>")
		fmt.Println("Step 3: Release Critical Section and send deferred replies")
		fmt.Println("Example: mutex-release 4")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}

	client, err := connectToNode(nodeID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	fmt.Println("====================================")
	fmt.Println("  STEP 3: RELEASE CRITICAL SECTION")
	fmt.Println("====================================")

	args := &node.MutexReleaseArgs{
		Timestamp: time.Now().UnixNano(),
	}
	var reply node.MutexReleaseReply

	err = client.Call("Node.MutexRelease", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Success {
		fmt.Println("✓ " + reply.Message)
		fmt.Println("")
		fmt.Println("What happened:")
		fmt.Println("  1. Critical Section released")
		fmt.Println("  2. All deferred REPLY messages sent to waiting nodes")
		fmt.Println("  3. Other nodes can now enter CS")
	} else {
		fmt.Println("✗ " + reply.Message)
	}
	fmt.Println("====================================")
}

// ===== Auto Deadlock Test =====

func handleDeadlockTest(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: deadlock-test <resource_name>")
		fmt.Println("This tests DAG (Wait-For Graph) deadlock detection — FULLY AUTO")
		fmt.Println("Example: deadlock-test shared_resource")
		return
	}

	resource := parts[1]
	resourceA := resource + "_A"
	resourceB := resource + "_B"

	fmt.Println("====================================")
	fmt.Println("  DAG DEADLOCK DETECTION TEST (AUTO)")
	fmt.Println("  Algorithm: Wait-For Graph (DAG)")
	fmt.Println("====================================")
	fmt.Println("")
	fmt.Println("This test creates a real deadlock scenario:")
	fmt.Println("  - Node 1 locks Resource A, then tries Resource B")
	fmt.Println("  - Node 2 locks Resource B, then tries Resource A")
	fmt.Println("  - This creates a cycle in the Wait-For Graph")
	fmt.Println("  - DAG cycle detection finds the deadlock")
	fmt.Println("  - System resolves it by aborting one transaction")
	fmt.Println("")

	// Find leader
	leaderID, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found. Please ensure nodes are running.")
		return
	}
	defer leaderClient.Close()

	fmt.Printf("Leader (coordinator): Node %d\n", leaderID)
	fmt.Println("")

	// Reset any previous DAG state
	leaderClient.Call("Node.DAGReset", &node.DAGResetArgs{}, &node.DAGResetReply{})

	// Step 1: Node 1 locks Resource A
	fmt.Println("--- Step 1: Node 1 locks Resource A ---")
	lockArgs1 := &node.DAGLockArgs{NodeID: 1, Resource: resourceA}
	var lockReply1 node.DAGLockReply
	err := leaderClient.Call("Node.DAGLock", lockArgs1, &lockReply1)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", lockReply1.Message)
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 2: Node 2 locks Resource B
	fmt.Println("--- Step 2: Node 2 locks Resource B ---")
	lockArgs2 := &node.DAGLockArgs{NodeID: 2, Resource: resourceB}
	var lockReply2 node.DAGLockReply
	err = leaderClient.Call("Node.DAGLock", lockArgs2, &lockReply2)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", lockReply2.Message)
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 3: Show current state
	fmt.Println("--- Current State ---")
	showDAGGraph(leaderClient)
	fmt.Println("No deadlock yet - no cycles in graph")
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 4: Node 1 tries to lock Resource B (held by Node 2) -> WAIT
	fmt.Println("--- Step 3: Node 1 tries to lock Resource B (held by Node 2) ---")
	lockArgs3 := &node.DAGLockArgs{NodeID: 1, Resource: resourceB}
	var lockReply3 node.DAGLockReply
	err = leaderClient.Call("Node.DAGLock", lockArgs3, &lockReply3)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", lockReply3.Message)
	if !lockReply3.Granted {
		fmt.Printf("Wait-For Graph edge added: Node 1 → Node %d\n", lockReply3.WaitingFor)
	}
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 5: Node 2 tries to lock Resource A (held by Node 1) -> WAIT -> DEADLOCK!
	fmt.Println("--- Step 4: Node 2 tries to lock Resource A (held by Node 1) ---")
	lockArgs4 := &node.DAGLockArgs{NodeID: 2, Resource: resourceA}
	var lockReply4 node.DAGLockReply
	err = leaderClient.Call("Node.DAGLock", lockArgs4, &lockReply4)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", lockReply4.Message)
	if !lockReply4.Granted {
		fmt.Printf("Wait-For Graph edge added: Node 2 → Node %d\n", lockReply4.WaitingFor)
	}
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 6: Show the Wait-For Graph
	fmt.Println("--- Wait-For Graph (DAG) ---")
	showDAGGraph(leaderClient)
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 7: Detect cycle (deadlock)
	fmt.Println("--- Step 5: Running Cycle Detection (DFS) ---")
	detectArgs := &node.DAGDetectArgs{}
	var detectReply node.DAGDetectReply
	err = leaderClient.Call("Node.DAGDetect", detectArgs, &detectReply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", detectReply.Message)
	if detectReply.HasCycle {
		fmt.Printf("Cycle path: %v\n", detectReply.Cycle)
		fmt.Println("")
		fmt.Println("DEADLOCK CONFIRMED!")
		fmt.Println("Node 1 waits for Node 2 (wants Resource B)")
		fmt.Println("Node 2 waits for Node 1 (wants Resource A)")
		fmt.Println("This forms a cycle: 1 → 2 → 1")
	}
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 8: Resolve deadlock by aborting Node 2's transaction
	fmt.Println("--- Step 6: Resolving Deadlock ---")
	fmt.Println("Strategy: Abort the younger transaction (Node 2)")
	resolveArgs := &node.DAGResolveArgs{AbortNodeID: 2}
	var resolveReply node.DAGResolveReply
	err = leaderClient.Call("Node.DAGResolve", resolveArgs, &resolveReply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", resolveReply.Message)
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 9: Now Node 1 can get Resource B
	fmt.Println("--- Step 7: Node 1 retries Resource B ---")
	lockArgs5 := &node.DAGLockArgs{NodeID: 1, Resource: resourceB}
	var lockReply5 node.DAGLockReply
	err = leaderClient.Call("Node.DAGLock", lockArgs5, &lockReply5)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", lockReply5.Message)
	fmt.Println("")
	time.Sleep(500 * time.Millisecond)

	// Step 10: Verify no more deadlocks
	fmt.Println("--- Step 8: Verify no deadlock ---")
	var detectReply2 node.DAGDetectReply
	leaderClient.Call("Node.DAGDetect", &node.DAGDetectArgs{}, &detectReply2)
	fmt.Printf("Result: %s\n", detectReply2.Message)
	fmt.Println("")

	// Cleanup
	fmt.Println("--- Cleanup ---")
	leaderClient.Call("Node.DAGUnlock", &node.DAGUnlockArgs{NodeID: 1, Resource: resourceA}, &node.DAGUnlockReply{})
	leaderClient.Call("Node.DAGUnlock", &node.DAGUnlockArgs{NodeID: 1, Resource: resourceB}, &node.DAGUnlockReply{})
	fmt.Println("All resources released")
	fmt.Println("")

	// Show final graph
	fmt.Println("--- Final Wait-For Graph ---")
	showDAGGraph(leaderClient)
	fmt.Println("")

	fmt.Println("====================================")
	fmt.Println("  DEADLOCK TEST COMPLETE")
	fmt.Println("====================================")
	fmt.Println("")
	fmt.Println("Summary:")
	fmt.Println("  1. Created circular dependency between Node 1 and Node 2")
	fmt.Println("  2. Wait-For Graph (DAG) detected the cycle using DFS")
	fmt.Println("  3. Resolved by aborting younger transaction (Node 2)")
	fmt.Println("  4. Node 1 successfully acquired both resources")
	fmt.Println("  5. System continues operating normally")
	fmt.Println("")
	fmt.Println("DEADLOCK DETECTED AND RESOLVED using DAG algorithm!")
	fmt.Println("====================================")
}

func handleDeadlockScenario(parts []string) {
	fmt.Println("====================================")
	fmt.Println("  SCENARIO: SHARED DOCUMENT ACCESS")
	fmt.Println("  Persons: Alice (Node 1), Bob (Node 2)")
	fmt.Println("  Documents: WorkReport.doc, Budget.doc")
	fmt.Println("====================================")
	fmt.Println("")

	leaderID, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("Error: No leader (coordinator) found.")
		return
	}
	defer leaderClient.Close()

	fmt.Printf("Coordinator: Node %d\n", leaderID)
	leaderClient.Call("Node.DAGReset", &node.DAGResetArgs{}, &node.DAGResetReply{})

	// Step 1: Alice locks WorkReport.doc
	fmt.Println("\n--- Step 1: Alice (Node 1) accesses 'WorkReport.doc' ---")
	var reply node.DAGLockReply
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 1, Resource: "WorkReport.doc"}, &reply)
	fmt.Printf("Result: %s\n", reply.Message)
	time.Sleep(1 * time.Second)

	// Step 2: Bob locks Budget.doc
	fmt.Println("\n--- Step 2: Bob (Node 2) accesses 'Budget.doc' ---")
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 2, Resource: "Budget.doc"}, &reply)
	fmt.Printf("Result: %s\n", reply.Message)
	time.Sleep(1 * time.Second)

	// Step 3: Alice tries to access Budget.doc (held by Bob)
	fmt.Println("\n--- Step 3: Alice (Node 1) wants to read 'Budget.doc' (held by Bob) ---")
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 1, Resource: "Budget.doc"}, &reply)
	fmt.Printf("Result: %s\n", reply.Message)
	if !reply.Granted {
		fmt.Printf("WAIT: Alice is now waiting for Bob (Node %d)\n", reply.WaitingFor)
	}
	time.Sleep(1 * time.Second)

	// Step 4: Bob tries to access WorkReport.doc (held by Alice)
	fmt.Println("\n--- Step 4: Bob (Node 2) wants to edit 'WorkReport.doc' (held by Alice) ---")
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 2, Resource: "WorkReport.doc"}, &reply)
	fmt.Printf("Result: %s\n", reply.Message)
	if !reply.Granted {
		fmt.Printf("WAIT: Bob is now waiting for Alice (Node %d)\n", reply.WaitingFor)
	}
	time.Sleep(1 * time.Second)

	fmt.Println("\n--- CURRENT WAIT-FOR GRAPH ---")
	showDAGGraph(leaderClient)

	// Step 5: Detect Deadlock
	fmt.Println("\n--- Step 5: Detecting Deadlock ---")
	var detectReply node.DAGDetectReply
	leaderClient.Call("Node.DAGDetect", &node.DAGDetectArgs{}, &detectReply)
	fmt.Printf("Result: %s\n", detectReply.Message)
	if detectReply.HasCycle {
		fmt.Printf("DEADLOCK CONFIRMED: Cycle %v found!\n", detectReply.Cycle)
		fmt.Println("Alice -> Bob -> Alice")
	}
	time.Sleep(1 * time.Second)

	// Step 6: Resolve Deadlock
	fmt.Println("\n--- Step 6: Resolving Deadlock ---")
	fmt.Println("Action: Aborting Bob's (Node 2) access to resolve the cycle")
	var resolveReply node.DAGResolveReply
	leaderClient.Call("Node.DAGResolve", &node.DAGResolveArgs{AbortNodeID: 2}, &resolveReply)
	fmt.Printf("Result: %s\n", resolveReply.Message)

	fmt.Println("\n--- FINAL WAIT-FOR GRAPH ---")
	showDAGGraph(leaderClient)
	fmt.Println("\nDeadlock resolved. Alice can now proceed with her work.")
	fmt.Println("====================================")
}

// ===== Manual Deadlock Commands =====

func handleDeadlockReset(parts []string) {
	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	leaderClient.Call("Node.DAGReset", &node.DAGResetArgs{}, &node.DAGResetReply{})
	fmt.Println("✓ DAG state cleared. Wait-For Graph is now empty.")
}

func handleDeadlockLock(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: deadlock-lock <node_id> <resource>")
		fmt.Println("Example: deadlock-lock 1 file_A")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}
	resource := parts[2]

	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	args := &node.DAGLockArgs{NodeID: nodeID, Resource: resource}
	var reply node.DAGLockReply
	err = leaderClient.Call("Node.DAGLock", args, &reply)
	if err != nil {
		fmt.Printf("RPC Error: %v\n", err)
		return
	}

	if reply.Granted {
		fmt.Printf("✓ Resource '%s' GRANTED to Node %d\n", resource, nodeID)
	} else {
		fmt.Printf("✗ Node %d must WAIT — resource '%s' held by Node %d\n", nodeID, resource, reply.WaitingFor)
		fmt.Printf("  Wait-For edge added: Node %d → Node %d\n", nodeID, reply.WaitingFor)
	}
}

func handleDeadlockUnlock(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: deadlock-unlock <node_id> <resource>")
		fmt.Println("Example: deadlock-unlock 1 file_A")
		return
	}

	nodeID, err := strconv.Atoi(parts[1])
	if err != nil {
		fmt.Println("Invalid node ID")
		return
	}
	resource := parts[2]

	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	args := &node.DAGUnlockArgs{NodeID: nodeID, Resource: resource}
	var reply node.DAGUnlockReply
	leaderClient.Call("Node.DAGUnlock", args, &reply)
	fmt.Printf("✓ Resource '%s' RELEASED by Node %d\n", resource, nodeID)
}

func handleDeadlockShow(parts []string) {
	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	fmt.Println("====================================")
	fmt.Println("  WAIT-FOR GRAPH (DAG)")
	fmt.Println("====================================")
	showDAGGraph(leaderClient)
	fmt.Println("====================================")
}

func handleDeadlockDetect(parts []string) {
	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	fmt.Println("Running DFS cycle detection on Wait-For Graph...")
	var reply node.DAGDetectReply
	err := leaderClient.Call("Node.DAGDetect", &node.DAGDetectArgs{}, &reply)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if reply.HasCycle {
		fmt.Printf("🔴 DEADLOCK DETECTED! Cycle: %v\n", reply.Cycle)
		fmt.Println("Use 'deadlock-resolve <node_id>' to break the cycle")
	} else {
		fmt.Println("🟢 No deadlock detected (no cycles in Wait-For Graph)")
	}
}

func handleDeadlockResolve(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: deadlock-resolve <node_id>")
		fmt.Println("Aborts the specified node's transactions to break the deadlock")
		return
	}

	nodeID, _ := strconv.Atoi(parts[1])

	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	var reply node.DAGResolveReply
	leaderClient.Call("Node.DAGResolve", &node.DAGResolveArgs{AbortNodeID: nodeID}, &reply)
	fmt.Printf("✓ %s\n", reply.Message)
	fmt.Println("Deadlock resolved. Run 'deadlock-detect' to verify.")
}

// Legacy: deadlock-create (kept for compatibility)
func handleDeadlockCreate(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: deadlock-create <resource_name>")
		return
	}

	resource := parts[1]
	resourceA := resource + "_A"
	resourceB := resource + "_B"

	_, leaderClient := findLeader()
	if leaderClient == nil {
		fmt.Println("No leader found.")
		return
	}
	defer leaderClient.Close()

	leaderClient.Call("Node.DAGReset", &node.DAGResetArgs{}, &node.DAGResetReply{})

	fmt.Println("--- Creating Deadlock ---")
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 1, Resource: resourceA}, &node.DAGLockReply{})
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 2, Resource: resourceB}, &node.DAGLockReply{})

	var reply node.DAGLockReply
	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 1, Resource: resourceB}, &reply)
	fmt.Printf("Node 1 waiting for Node %d on %s\n", reply.WaitingFor, resourceB)

	leaderClient.Call("Node.DAGLock", &node.DAGLockArgs{NodeID: 2, Resource: resourceA}, &reply)
	fmt.Printf("Node 2 waiting for Node %d on %s\n", reply.WaitingFor, resourceA)

	fmt.Println("Deadlock scenario created (Circular Wait: 1->2->1)")
}

// ===== Helper Functions =====

func showDAGGraph(client *rpc.Client) {
	graphArgs := &node.DAGGraphArgs{}
	var graphReply node.DAGGraphReply
	err := client.Call("Node.DAGGraph", graphArgs, &graphReply)
	if err != nil {
		fmt.Printf("Cannot get graph: %v\n", err)
		return
	}

	fmt.Println("Held Resources:")
	if len(graphReply.HeldResources) == 0 {
		fmt.Println("  (none)")
	} else {
		for resource, nodeID := range graphReply.HeldResources {
			fmt.Printf("  '%s' -> Node %d\n", resource, nodeID)
		}
	}

	fmt.Println("Wait-For Edges:")
	if len(graphReply.WaitForGraph) == 0 {
		fmt.Println("  (none)")
	} else {
		for from, toList := range graphReply.WaitForGraph {
			for _, to := range toList {
				fmt.Printf("  Node %d → Node %d\n", from, to)
			}
		}
	}
}

func printHelp() {
	fmt.Println("====================================")
	fmt.Println("         AVAILABLE COMMANDS")
	fmt.Println("====================================")
	fmt.Println("")
	fmt.Println("Basic Operations:")
	fmt.Println("  put <node_id> <key> <filename>     - Store file (auto-replicates)")
	fmt.Println("  get <node_id> <key>                - Retrieve file from any node")
	fmt.Println("  delete <node_id> <key>             - Delete key (propagates)")
	fmt.Println("  list <node_id>                     - List all keys on node")
	fmt.Println("  status                             - Show all nodes status")
	fmt.Println("  status <node_id>                   - Show specific node")
	fmt.Println("  partition-info <node_id> <key>     - Show partition ownership")
	fmt.Println("  quorum-get <key>                   - Quorum read + read-repair")
	fmt.Println("")
	fmt.Println("Mutual Exclusion (Ricart-Agrawala):")
	fmt.Println("  mutex-put <key> <filename>         - FULL AUTO: request CS → write → release")
	fmt.Println("  mutex-request <node_id>            - MANUAL Step 1: Request CS")
	fmt.Println("  mutex-write <node_id> <key> <file> - MANUAL Step 2: Write inside CS")
	fmt.Println("  mutex-release <node_id>            - MANUAL Step 3: Release CS")
	fmt.Println("")
	fmt.Println("Deadlock Detection (DAG Wait-For Graph):")
	fmt.Println("  deadlock-test <resource>           - FULL AUTO: create → detect → resolve")
	fmt.Println("  deadlock-scenario                  - FULL AUTO: Alice & Bob scenario")
	fmt.Println("  deadlock-reset                     - MANUAL: Clear all DAG state")
	fmt.Println("  deadlock-lock <node_id> <resource> - MANUAL: Lock a resource")
	fmt.Println("  deadlock-unlock <node_id> <res>    - MANUAL: Unlock a resource")
	fmt.Println("  deadlock-show                      - MANUAL: Show Wait-For Graph")
	fmt.Println("  deadlock-detect                    - MANUAL: Run cycle detection")
	fmt.Println("  deadlock-resolve <node_id>         - MANUAL: Abort node's transactions")
	fmt.Println("")
	fmt.Println("Other:")
	fmt.Println("  help    - Show this help")
	fmt.Println("  exit    - Quit the client")
	fmt.Println("")
	fmt.Println("====================================")
}
