package node

import (
	"fmt"
	"net/rpc"
	"strings"
	"time"
)

// ===== Basic Storage Operations =====

type PutArgs struct {
	Key       string
	Value     []byte
	Timestamp int64
	IsReplica bool
}

type PutReply struct {
	Success    bool
	Message    string
	Replicated bool
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value     []byte
	Found     bool
	Timestamp int64
	Message   string
}

type DeleteArgs struct {
	Key       string
	Timestamp int64
	IsReplica bool
}

type DeleteReply struct {
	Success bool
	Message string
}

func (n *Node) Put(args *PutArgs, reply *PutReply) error {
	n.UpdateClock(args.Timestamp)

	n.DataMutex.Lock()
	n.DataStore[args.Key] = StoredItem{
		Value:     args.Value,
		Timestamp: args.Timestamp,
	}
	n.DataMutex.Unlock()

	reply.Success = true

	if args.IsReplica {
		reply.Message = fmt.Sprintf("Replicated key '%s' on Node %d", args.Key, n.ID)
		fmt.Printf("[Node %d] REPLICATED: key='%s', size=%d bytes\n", n.ID, args.Key, len(args.Value))
	} else {
		fmt.Printf("[Node %d] PUT: key='%s', size=%d bytes, timestamp=%d\n", n.ID, args.Key, len(args.Value), args.Timestamp)
		replicatedTo := n.replicateToPartitionNodes(args.Key, args.Value, args.Timestamp)
		reply.Replicated = true
		reply.Message = fmt.Sprintf("Stored key '%s' on Node %d, replicated to: %s", args.Key, n.ID, strings.Join(replicatedTo, ", "))
	}

	return nil
}

func (n *Node) replicateToPartitionNodes(key string, value []byte, timestamp int64) []string {
	results := make([]string, 0)
	var targetNodes []int

	if n.PartitionMgr.IsEnabled() {
		partitionOwners := n.PartitionMgr.GetPartitionOwners(key)
		for _, nodeID := range partitionOwners {
			if nodeID != n.ID {
				targetNodes = append(targetNodes, nodeID)
			}
		}
		fmt.Printf("[Node %d] PARTITION-AWARE REPLICATION: key='%s' to nodes %v\n", n.ID, key, partitionOwners)
	} else {
		targetNodes = n.GetOtherNodes()
		fmt.Printf("[Node %d] FULL REPLICATION: key='%s' to all other nodes\n", n.ID, key)
	}

	for _, nodeID := range targetNodes {
		address := n.GetNodeAddress(nodeID)
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			fmt.Printf("[Node %d] Cannot replicate to Node %d: %v\n", n.ID, nodeID, err)
			results = append(results, fmt.Sprintf("Node %d (failed)", nodeID))
			continue
		}

		putArgs := &PutArgs{Key: key, Value: value, Timestamp: timestamp, IsReplica: true}
		var putReply PutReply
		err = client.Call("Node.Put", putArgs, &putReply)
		client.Close()

		if err != nil {
			results = append(results, fmt.Sprintf("Node %d (failed)", nodeID))
		} else {
			fmt.Printf("[Node %d] Replicated key '%s' to Node %d\n", n.ID, key, nodeID)
			results = append(results, fmt.Sprintf("Node %d (ok)", nodeID))
		}
	}

	return results
}

func (n *Node) Get(args *GetArgs, reply *GetReply) error {
	n.IncrementClock()
	n.DataMutex.RLock()
	defer n.DataMutex.RUnlock()

	item, found := n.DataStore[args.Key]
	reply.Found = found

	if found {
		reply.Value = item.Value
		reply.Timestamp = item.Timestamp
		reply.Message = fmt.Sprintf("Found key '%s' on Node %d", args.Key, n.ID)
		fmt.Printf("[Node %d] GET: key='%s', found=true, size=%d bytes\n", n.ID, args.Key, len(item.Value))
	} else {
		reply.Message = fmt.Sprintf("Key '%s' not found on Node %d", args.Key, n.ID)
		fmt.Printf("[Node %d] GET: key='%s', found=false\n", n.ID, args.Key)
	}

	return nil
}

func (n *Node) Delete(args *DeleteArgs, reply *DeleteReply) error {
	n.UpdateClock(args.Timestamp)

	n.DataMutex.Lock()
	_, found := n.DataStore[args.Key]
	if found {
		delete(n.DataStore, args.Key)
	}
	n.DataMutex.Unlock()

	reply.Success = found

	if args.IsReplica {
		reply.Message = fmt.Sprintf("Deleted key '%s' from Node %d (replica)", args.Key, n.ID)
		fmt.Printf("[Node %d] DELETED (replica): key='%s'\n", n.ID, args.Key)
	} else {
		if found {
			fmt.Printf("[Node %d] DELETE: key='%s', success=true\n", n.ID, args.Key)
			if n.PartitionMgr.IsEnabled() {
				partitionOwners := n.PartitionMgr.GetPartitionOwners(args.Key)
				for _, nodeID := range partitionOwners {
					if nodeID != n.ID {
						go n.propagateDelete(nodeID, args.Key, args.Timestamp)
					}
				}
			} else {
				for _, nodeID := range n.GetOtherNodes() {
					go n.propagateDelete(nodeID, args.Key, args.Timestamp)
				}
			}
			reply.Message = fmt.Sprintf("Deleted key '%s' from Node %d", args.Key, n.ID)
		} else {
			reply.Message = fmt.Sprintf("Key '%s' not found on Node %d", args.Key, n.ID)
		}
	}

	return nil
}

func (n *Node) propagateDelete(nodeID int, key string, timestamp int64) {
	address := n.GetNodeAddress(nodeID)
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return
	}
	defer client.Close()

	deleteArgs := &DeleteArgs{Key: key, Timestamp: timestamp, IsReplica: true}
	var deleteReply DeleteReply
	client.Call("Node.Delete", deleteArgs, &deleteReply)
}

// ===== Leader Election (Bully Algorithm) =====

type ElectionArgs struct {
	CandidateID int
	Timestamp   int64
}

type ElectionReply struct {
	Acknowledged bool
	NodeID       int
}

type CoordinatorArgs struct {
	LeaderID  int
	Timestamp int64
}

type CoordinatorReply struct {
	Acknowledged bool
}

type HeartbeatArgs struct {
	LeaderID  int
	Timestamp int64
}

type HeartbeatReply struct {
	Alive bool
}

// Election handles incoming election message (Bully Algorithm)
func (n *Node) Election(args *ElectionArgs, reply *ElectionReply) error {
	n.UpdateClock(args.Timestamp)

	fmt.Printf("[Node %d] Received ELECTION from Node %d\n", n.ID, args.CandidateID)

	reply.Acknowledged = true
	reply.NodeID = n.ID

	// If I have higher ID than the candidate, I should take over
	if n.ID > args.CandidateID {
		fmt.Printf("[Node %d] I have higher ID than Node %d, taking over election\n", n.ID, args.CandidateID)
		if n.ElectionMgr != nil {
			go n.ElectionMgr.TriggerElection()
		}
	}

	return nil
}

// isNodeAlive checks if a node can be reached
func (n *Node) isNodeAlive(nodeID int) bool {
	address := n.GetNodeAddress(nodeID)
	if address == "" {
		return false
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return false
	}
	defer client.Close()

	// Try a quick heartbeat
	args := &HeartbeatArgs{LeaderID: nodeID, Timestamp: n.GetClock()}
	var reply HeartbeatReply

	call := client.Go("Node.Heartbeat", args, &reply, nil)

	select {
	case <-call.Done:
		return call.Error == nil && reply.Alive
	case <-time.After(1 * time.Second):
		return false
	}
}

// Coordinator handles coordinator announcement
// FIXED: Accept new leader if current leader is dead
func (n *Node) Coordinator(args *CoordinatorArgs, reply *CoordinatorReply) error {
	n.UpdateClock(args.Timestamp)

	currentLeader := n.GetLeader()

	// Accept the new leader if:
	// 1. We have no leader (-1)
	// 2. New leader has higher or equal ID
	// 3. Current leader is DEAD (can't be reached)
	shouldAccept := false

	if currentLeader == -1 {
		// No leader set
		shouldAccept = true
	} else if args.LeaderID >= currentLeader {
		// New leader has higher or equal ID
		shouldAccept = true
	} else if currentLeader != n.ID && !n.isNodeAlive(currentLeader) {
		// Current leader is dead, accept new leader
		fmt.Printf("[Node %d] Current leader Node %d is DEAD, accepting Node %d as new leader\n",
			n.ID, currentLeader, args.LeaderID)
		shouldAccept = true
	}

	if shouldAccept {
		fmt.Printf("[Node %d] Accepted COORDINATOR: Node %d is the new leader\n", n.ID, args.LeaderID)
		n.SetLeader(args.LeaderID)
	} else {
		fmt.Printf("[Node %d] Ignoring COORDINATOR from Node %d (current leader Node %d is still alive)\n",
			n.ID, args.LeaderID, currentLeader)
	}

	reply.Acknowledged = true
	return nil
}

// Heartbeat handles heartbeat from leader
func (n *Node) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	n.UpdateClock(args.Timestamp)

	// If we receive heartbeat, the sender is alive
	// Update leader if this is from the current leader or if we have no leader
	currentLeader := n.GetLeader()
	if currentLeader == -1 || args.LeaderID == currentLeader {
		if currentLeader == -1 {
			n.SetLeader(args.LeaderID)
		}
	}

	reply.Alive = true
	return nil
}

// ===== Mutual Exclusion (Ricart-Agrawala) =====

type CSRequestArgs struct {
	NodeID    int
	Timestamp int64
}

type CSRequestReply struct {
	Granted bool
}

type CSReleaseArgs struct {
	NodeID    int
	Timestamp int64
}

type CSReleaseReply struct {
	Acknowledged bool
}

func (n *Node) RequestCS(args *CSRequestArgs, reply *CSRequestReply) error {
	n.UpdateClock(args.Timestamp)
	fmt.Printf("[Node %d] Received CS REQUEST from Node %d (timestamp=%d)\n", n.ID, args.NodeID, args.Timestamp)

	n.MutexLock.Lock()
	defer n.MutexLock.Unlock()

	shouldDefer := false
	if n.RequestingCS {
		if args.Timestamp > n.RequestTime {
			shouldDefer = true
		} else if args.Timestamp == n.RequestTime && args.NodeID > n.ID {
			shouldDefer = true
		}
	}

	if shouldDefer {
		n.DeferredQueue = append(n.DeferredQueue, DeferredRequest{NodeID: args.NodeID, Timestamp: args.Timestamp})
		reply.Granted = false
		fmt.Printf("[Node %d] Deferred reply to Node %d\n", n.ID, args.NodeID)
	} else {
		reply.Granted = true
		fmt.Printf("[Node %d] Granted CS to Node %d immediately\n", n.ID, args.NodeID)
	}

	return nil
}

func (n *Node) ReleaseCS(args *CSReleaseArgs, reply *CSReleaseReply) error {
	n.UpdateClock(args.Timestamp)
	fmt.Printf("[Node %d] Node %d released Critical Section\n", n.ID, args.NodeID)
	reply.Acknowledged = true
	return nil
}

// ===== Mutex Put =====

type MutexPutArgs struct {
	Key       string
	Value     []byte
	Timestamp int64
}

type MutexPutReply struct {
	Success bool
	Message string
}

func (n *Node) MutexPut(args *MutexPutArgs, reply *MutexPutReply) error {
	n.UpdateClock(args.Timestamp)
	fmt.Printf("[Node %d] ===== MUTEX-PUT START for key '%s' =====\n", n.ID, args.Key)

	if n.MutexMgr == nil {
		reply.Success = false
		reply.Message = "Mutex manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] Requesting critical section via Ricart-Agrawala...\n", n.ID)

	success := n.MutexMgr.ExecuteInCriticalSection(func() {
		n.DataMutex.Lock()
		n.DataStore[args.Key] = StoredItem{Value: args.Value, Timestamp: args.Timestamp}
		n.DataMutex.Unlock()
		fmt.Printf("[Node %d] MUTEX-PUT: Stored key '%s' locally inside Critical Section\n", n.ID, args.Key)
		n.replicateToPartitionNodes(args.Key, args.Value, args.Timestamp)
	})

	if success {
		reply.Success = true
		reply.Message = fmt.Sprintf("Key '%s' stored with mutual exclusion (Ricart-Agrawala) and replicated", args.Key)
		fmt.Printf("[Node %d] ===== MUTEX-PUT COMPLETE for key '%s' =====\n", n.ID, args.Key)
	} else {
		reply.Success = false
		reply.Message = "Failed to acquire critical section"
	}

	return nil
}

// ===== Manual Mutex Commands (Step-by-Step Ricart-Agrawala) =====

type MutexRequestArgs struct {
	Timestamp int64
}

type MutexRequestReply struct {
	Success bool
	Message string
}

type MutexWriteArgs struct {
	Key       string
	Value     []byte
	Timestamp int64
}

type MutexWriteReply struct {
	Success bool
	Message string
}

type MutexReleaseArgs struct {
	Timestamp int64
}

type MutexReleaseReply struct {
	Success bool
	Message string
}

// MutexRequest - Step 1: Request critical section from all nodes (manual)
func (n *Node) MutexRequest(args *MutexRequestArgs, reply *MutexRequestReply) error {
	n.UpdateClock(args.Timestamp)

	if n.MutexMgr == nil {
		reply.Success = false
		reply.Message = "Mutex manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] ===== MANUAL MUTEX-REQUEST =====\n", n.ID)
	fmt.Printf("[Node %d] Sending CS REQUEST to all other nodes (Ricart-Agrawala)...\n", n.ID)

	granted := n.MutexMgr.RequestOnly()
	if granted {
		reply.Success = true
		reply.Message = fmt.Sprintf("Node %d ENTERED Critical Section — all nodes granted permission. Ready for writes.", n.ID)
	} else {
		reply.Success = false
		reply.Message = fmt.Sprintf("Node %d could NOT enter Critical Section", n.ID)
	}

	return nil
}

// MutexWrite - Step 2: Write data while inside critical section (manual)
func (n *Node) MutexWrite(args *MutexWriteArgs, reply *MutexWriteReply) error {
	n.UpdateClock(args.Timestamp)

	n.MutexLock.Lock()
	inCS := n.InCriticalSection
	n.MutexLock.Unlock()

	if !inCS {
		reply.Success = false
		reply.Message = "Not in Critical Section! Run mutex-request first."
		return nil
	}

	fmt.Printf("[Node %d] ===== MUTEX-WRITE inside Critical Section =====\n", n.ID)

	// Write data locally
	n.DataMutex.Lock()
	n.DataStore[args.Key] = StoredItem{Value: args.Value, Timestamp: args.Timestamp}
	n.DataMutex.Unlock()
	fmt.Printf("[Node %d] Stored key '%s' locally (%d bytes)\n", n.ID, args.Key, len(args.Value))

	// Replicate
	replicatedTo := n.replicateToPartitionNodes(args.Key, args.Value, args.Timestamp)
	reply.Success = true
	reply.Message = fmt.Sprintf("Key '%s' written inside Critical Section on Node %d, replicated to: %s",
		args.Key, n.ID, strings.Join(replicatedTo, ", "))

	return nil
}

// MutexRelease - Step 3: Release critical section (manual)
func (n *Node) MutexRelease(args *MutexReleaseArgs, reply *MutexReleaseReply) error {
	n.UpdateClock(args.Timestamp)

	n.MutexLock.Lock()
	inCS := n.InCriticalSection
	n.MutexLock.Unlock()

	if !inCS {
		reply.Success = false
		reply.Message = "Not in Critical Section! Nothing to release."
		return nil
	}

	if n.MutexMgr == nil {
		reply.Success = false
		reply.Message = "Mutex manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] ===== MANUAL MUTEX-RELEASE =====\n", n.ID)
	n.MutexMgr.ManualRelease()

	reply.Success = true
	reply.Message = fmt.Sprintf("Node %d RELEASED Critical Section. Deferred replies sent.", n.ID)

	return nil
}

// ===== DAG Deadlock Detection =====

type DAGLockArgs struct {
	NodeID   int
	Resource string
}

type DAGLockReply struct {
	Granted    bool
	WaitingFor int
	Message    string
}

type DAGUnlockArgs struct {
	NodeID   int
	Resource string
}

type DAGUnlockReply struct {
	Acknowledged bool
	Message      string
}

type DAGDetectArgs struct{}

type DAGDetectReply struct {
	HasCycle bool
	Cycle    []int
	Message  string
}

type DAGGraphArgs struct{}

type DAGGraphReply struct {
	WaitForGraph  map[int][]int
	HeldResources map[string]int
	Message       string
}

type DAGResolveArgs struct {
	AbortNodeID int
}

type DAGResolveReply struct {
	Success bool
	Message string
}

type DAGResetArgs struct{}

type DAGResetReply struct {
	Acknowledged bool
}

type DAGLogsArgs struct{}

type DAGLogsReply struct {
	Logs []string
}

func (n *Node) DAGLock(args *DAGLockArgs, reply *DAGLockReply) error {
	n.IncrementClock()
	if n.DeadlockMgr == nil {
		reply.Message = "Deadlock manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] DAG-LOCK request: Node %d wants resource '%s'\n", n.ID, args.NodeID, args.Resource)
	granted, waitFor := n.DeadlockMgr.Lock(args.NodeID, args.Resource)
	reply.Granted = granted
	reply.WaitingFor = waitFor

	if granted {
		reply.Message = fmt.Sprintf("Resource '%s' granted to Node %d", args.Resource, args.NodeID)
	} else {
		reply.Message = fmt.Sprintf("Node %d waiting for Node %d to release '%s'", args.NodeID, waitFor, args.Resource)
	}

	return nil
}

func (n *Node) DAGUnlock(args *DAGUnlockArgs, reply *DAGUnlockReply) error {
	n.IncrementClock()
	if n.DeadlockMgr == nil {
		reply.Message = "Deadlock manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] DAG-UNLOCK: Node %d releasing resource '%s'\n", n.ID, args.NodeID, args.Resource)
	n.DeadlockMgr.Unlock(args.NodeID, args.Resource)
	reply.Acknowledged = true
	reply.Message = fmt.Sprintf("Resource '%s' released by Node %d", args.Resource, args.NodeID)

	return nil
}

func (n *Node) DAGDetect(args *DAGDetectArgs, reply *DAGDetectReply) error {
	if n.DeadlockMgr == nil {
		reply.Message = "Deadlock manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] Running DAG cycle detection...\n", n.ID)
	hasCycle, cycle := n.DeadlockMgr.DetectCycle()
	reply.HasCycle = hasCycle
	reply.Cycle = cycle

	if hasCycle {
		reply.Message = fmt.Sprintf("DEADLOCK DETECTED! Cycle: %v", cycle)
	} else {
		reply.Message = "No deadlock detected"
	}

	return nil
}

func (n *Node) DAGGraph(args *DAGGraphArgs, reply *DAGGraphReply) error {
	if n.DeadlockMgr == nil {
		reply.Message = "Deadlock manager not initialized"
		return nil
	}

	reply.WaitForGraph = n.DeadlockMgr.GetWaitForGraph()
	reply.HeldResources = n.DeadlockMgr.GetHeldResources()
	reply.Message = "Wait-For Graph retrieved"

	return nil
}

func (n *Node) DAGResolve(args *DAGResolveArgs, reply *DAGResolveReply) error {
	if n.DeadlockMgr == nil {
		reply.Message = "Deadlock manager not initialized"
		return nil
	}

	fmt.Printf("[Node %d] RESOLVING deadlock: aborting Node %d\n", n.ID, args.AbortNodeID)
	n.DeadlockMgr.Resolve(args.AbortNodeID)
	reply.Success = true
	reply.Message = fmt.Sprintf("Deadlock resolved by aborting Node %d", args.AbortNodeID)

	return nil
}

func (n *Node) DAGReset(args *DAGResetArgs, reply *DAGResetReply) error {
	if n.DeadlockMgr == nil {
		return nil
	}
	n.DeadlockMgr.Reset()
	reply.Acknowledged = true
	return nil
}

func (n *Node) DAGLogs(args *DAGLogsArgs, reply *DAGLogsReply) error {
	if n.DeadlockMgr == nil {
		return nil
	}
	reply.Logs = n.DeadlockMgr.GetLogs()
	return nil
}

// ===== Status Operations =====

type StatusArgs struct{}

type StatusReply struct {
	NodeID       int
	IsLeader     bool
	LeaderID     int
	LamportClock int64
	DataCount    int
	IsAlive      bool
}

func (n *Node) Status(args *StatusArgs, reply *StatusReply) error {
	n.LeaderMutex.RLock()
	reply.NodeID = n.ID
	reply.IsLeader = n.IsLeader
	reply.LeaderID = n.LeaderID
	n.LeaderMutex.RUnlock()

	reply.LamportClock = n.GetClock()

	n.DataMutex.RLock()
	reply.DataCount = len(n.DataStore)
	n.DataMutex.RUnlock()

	reply.IsAlive = true

	return nil
}

type ListKeysArgs struct{}

type ListKeysReply struct {
	Keys []string
}

func (n *Node) ListKeys(args *ListKeysArgs, reply *ListKeysReply) error {
	n.DataMutex.RLock()
	defer n.DataMutex.RUnlock()

	reply.Keys = make([]string, 0, len(n.DataStore))
	for key := range n.DataStore {
		reply.Keys = append(reply.Keys, key)
	}

	return nil
}

// ===== Partition Management =====

type PartitionInfoArgs struct {
	Key string
}

type PartitionInfoReply struct {
	Key               string
	Owners            []int
	Primary           int
	Replicas          []int
	IsPartitioned     bool
	ReplicationFactor int
	Message           string
}

func (n *Node) GetPartitionInfo(args *PartitionInfoArgs, reply *PartitionInfoReply) error {
	reply.Key = args.Key
	reply.IsPartitioned = n.PartitionMgr.IsEnabled()
	reply.ReplicationFactor = n.PartitionMgr.GetReplicationFactor()

	owners := n.PartitionMgr.GetPartitionOwners(args.Key)
	reply.Owners = owners

	if len(owners) > 0 {
		reply.Primary = owners[0]
		if len(owners) > 1 {
			reply.Replicas = owners[1:]
		}
		reply.Message = n.PartitionMgr.DescribePartition(args.Key)
	} else {
		reply.Message = "All nodes store this key (no partitioning)"
	}

	return nil
}