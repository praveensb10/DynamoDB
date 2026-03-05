package election

import (
	"dynamo-go/node"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// BullyElection manages leader election using Bully Algorithm
type BullyElection struct {
	Node               *node.Node
	electionInProgress bool
	electionMutex      sync.Mutex
	stopHeartbeat      chan bool
	stopped            bool
}

// NewBullyElection creates a new Bully Election manager
func NewBullyElection(n *node.Node) *BullyElection {
	be := &BullyElection{
		Node:          n,
		stopHeartbeat: make(chan bool, 1),
		stopped:       false,
	}
	n.ElectionMgr = be
	return be
}

// TriggerElection is called by RPC handler
func (be *BullyElection) TriggerElection() {
	go be.StartElection()
}

// AnnounceLeadershipTo sends COORDINATOR message to a specific node
func (be *BullyElection) AnnounceLeadershipTo(nodeID int) {
	be.sendCoordinatorMessage(nodeID)
}

// StartElection initiates the Bully Algorithm election process
func (be *BullyElection) StartElection() {
	be.electionMutex.Lock()
	if be.electionInProgress {
		be.electionMutex.Unlock()
		return
	}
	be.electionInProgress = true
	be.electionMutex.Unlock()

	defer func() {
		be.electionMutex.Lock()
		be.electionInProgress = false
		be.electionMutex.Unlock()
	}()

	fmt.Printf("[Node %d] Starting ELECTION (Bully Algorithm)\n", be.Node.ID)

	higherNodes := be.Node.GetHigherNodes()

	if len(higherNodes) == 0 {
		// No higher nodes exist in config, I become the leader
		fmt.Printf("[Node %d] No higher nodes in config, I am the LEADER\n", be.Node.ID)
		be.becomeLeader()
		return
	}

	// Send ELECTION message to all higher nodes
	gotResponse := false
	var wg sync.WaitGroup
	var responseMutex sync.Mutex

	for _, higherID := range higherNodes {
		wg.Add(1)
		go func(nodeID int) {
			defer wg.Done()

			if be.sendElectionMessage(nodeID) {
				responseMutex.Lock()
				gotResponse = true
				responseMutex.Unlock()
			}
		}(higherID)
	}

	// Wait for responses with timeout
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(time.Duration(be.Node.Config.ElectionTimeout) * time.Millisecond):
		fmt.Printf("[Node %d] Election timeout waiting for higher nodes\n", be.Node.ID)
	}

	responseMutex.Lock()
	receivedResponse := gotResponse
	responseMutex.Unlock()

	if !receivedResponse {
		// No higher node responded (they're all dead/offline), I become the leader
		fmt.Printf("[Node %d] No response from higher nodes, I am the LEADER\n", be.Node.ID)
		be.becomeLeader()
	} else {
		// Higher node responded, they will take over
		fmt.Printf("[Node %d] Higher node responded, waiting for COORDINATOR announcement\n", be.Node.ID)
	}
}

// sendElectionMessage sends ELECTION message to a specific node
func (be *BullyElection) sendElectionMessage(nodeID int) bool {
	address := be.Node.GetNodeAddress(nodeID)

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Printf("[Node %d] Cannot reach Node %d for election (offline)\n", be.Node.ID, nodeID)
		return false
	}
	defer client.Close()

	args := &node.ElectionArgs{
		CandidateID: be.Node.ID,
		Timestamp:   be.Node.IncrementClock(),
	}
	var reply node.ElectionReply

	call := client.Go("Node.Election", args, &reply, nil)

	select {
	case <-call.Done:
		if call.Error != nil {
			fmt.Printf("[Node %d] Election RPC to Node %d failed: %v\n", be.Node.ID, nodeID, call.Error)
			return false
		}
		fmt.Printf("[Node %d] Node %d responded to ELECTION\n", be.Node.ID, nodeID)
		return reply.Acknowledged
	case <-time.After(time.Duration(be.Node.Config.ElectionTimeout) * time.Millisecond):
		fmt.Printf("[Node %d] Election timeout from Node %d\n", be.Node.ID, nodeID)
		return false
	}
}

// becomeLeader makes this node the leader and announces to all
func (be *BullyElection) becomeLeader() {
	be.Node.SetLeader(be.Node.ID)

	// Announce to ALL other nodes (not just those who responded)
	otherNodes := be.Node.GetOtherNodes()

	for _, nodeID := range otherNodes {
		go be.sendCoordinatorMessage(nodeID)
	}
}

// sendCoordinatorMessage sends COORDINATOR message to announce new leader
func (be *BullyElection) sendCoordinatorMessage(nodeID int) {
	address := be.Node.GetNodeAddress(nodeID)

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		// Node might be offline, that's okay
		return
	}
	defer client.Close()

	args := &node.CoordinatorArgs{
		LeaderID:  be.Node.ID,
		Timestamp: be.Node.IncrementClock(),
	}
	var reply node.CoordinatorReply

	err = client.Call("Node.Coordinator", args, &reply)
	if err != nil {
		return
	}

	fmt.Printf("[Node %d] Announced leadership to Node %d\n", be.Node.ID, nodeID)
}

// StartHeartbeatMonitor monitors leader's heartbeat and triggers election if leader dies
func (be *BullyElection) StartHeartbeatMonitor() {
	go func() {
		for {
			select {
			case <-be.stopHeartbeat:
				return
			default:
				time.Sleep(time.Duration(be.Node.Config.HeartbeatInterval) * time.Millisecond)

				if be.stopped {
					return
				}

				if be.Node.AmILeader() {
					be.sendHeartbeats()
				} else {
					leaderID := be.Node.GetLeader()
					if leaderID == -1 {
						fmt.Printf("[Node %d] No leader set, starting election\n", be.Node.ID)
						go be.StartElection()
					} else if !be.checkLeaderAlive() {
						fmt.Printf("[Node %d] Leader not responding, starting election\n", be.Node.ID)
						go be.StartElection()
					}
				}
			}
		}
	}()
}

// sendHeartbeats sends heartbeat to all other nodes
func (be *BullyElection) sendHeartbeats() {
	otherNodes := be.Node.GetOtherNodes()

	for _, nodeID := range otherNodes {
		go func(id int) {
			address := be.Node.GetNodeAddress(id)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return
			}
			defer client.Close()

			args := &node.HeartbeatArgs{
				LeaderID:  be.Node.ID,
				Timestamp: be.Node.IncrementClock(),
			}
			var reply node.HeartbeatReply
			client.Call("Node.Heartbeat", args, &reply)
		}(nodeID)
	}
}

// checkLeaderAlive checks if current leader is responding
func (be *BullyElection) checkLeaderAlive() bool {
	leaderID := be.Node.GetLeader()

	if leaderID == -1 {
		return false
	}

	if leaderID == be.Node.ID {
		return true
	}

	address := be.Node.GetNodeAddress(leaderID)
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return false
	}
	defer client.Close()

	args := &node.HeartbeatArgs{
		LeaderID:  leaderID,
		Timestamp: be.Node.IncrementClock(),
	}
	var reply node.HeartbeatReply

	call := client.Go("Node.Heartbeat", args, &reply, nil)

	select {
	case <-call.Done:
		if call.Error != nil {
			return false
		}
		return reply.Alive
	case <-time.After(time.Duration(be.Node.Config.ElectionTimeout) * time.Millisecond):
		return false
	}
}

// StopHeartbeatMonitor stops the heartbeat monitoring
func (be *BullyElection) StopHeartbeatMonitor() {
	be.stopped = true
	select {
	case be.stopHeartbeat <- true:
	default:
	}
}

// TriggerElectionOnStartup starts election when node first comes up
// THIS IS THE KEY FIX: Always start election on startup to challenge any existing leader
func (be *BullyElection) TriggerElectionOnStartup() {
	// Wait a bit for network to stabilize
	time.Sleep(2 * time.Second)

	// ALWAYS start election on startup
	// If I have the highest ID among online nodes, I'll become leader
	// If someone higher is online, they'll take over
	fmt.Printf("[Node %d] Startup complete, triggering election...\n", be.Node.ID)
	be.StartElection()
}
