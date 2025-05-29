package main
import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
	"bytes"
)

// Node represents a single node in the cluster
type Node struct {
	ID          string
	Port        string
	Peers       map[string]string // peer ID -> peer address
	State       string            // follower, candidate, leader
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex  map[string]int
	MatchIndex map[string]int

	// Channels for internal communication
	appendEntriesChan chan AppendEntriesRequest
	voteRequestChan   chan RequestVoteRequest
	submitChan        chan string
	shutdownChan      chan struct{}

	mu sync.Mutex
}

// LogEntry represents a single entry in the log
type LogEntry struct {
	Term    int
	Command string
}

// AppendEntriesRequest is the RPC struct for append entries
type AppendEntriesRequest struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesResponse is the response for append entries
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// RequestVoteRequest is the RPC struct for requesting votes
type RequestVoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResponse is the response for vote requests
type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// NewNode creates a new node
func NewNode(id, port string, peers map[string]string) *Node {
	return &Node{
		ID:                id,
		Port:              port,
		Peers:             peers,
		State:             "follower",
		CurrentTerm:       0,
		VotedFor:          "",
		Log:               make([]LogEntry, 0),
		CommitIndex:       -1,
		LastApplied:       -1,
		NextIndex:         make(map[string]int),
		MatchIndex:        make(map[string]int),
		appendEntriesChan: make(chan AppendEntriesRequest),
		voteRequestChan:   make(chan RequestVoteRequest),
		submitChan:        make(chan string),
		shutdownChan:      make(chan struct{}),
	}
}

// Start initializes the node
func (n *Node) Start() {
	rand.Seed(time.Now().UnixNano())

	// Start HTTP server for RPCs
	go n.startServer()

	// Start state machine
	go n.run()
}

// Stop shuts down the node
func (n *Node) Stop() {
	close(n.shutdownChan)
}

// run is the main loop for the node's state machine
func (n *Node) run() {
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	electionTimer := time.NewTimer(electionTimeout)

	for {
		select {
		case <-n.shutdownChan:
			return

		case <-electionTimer.C:
			n.mu.Lock()
			if n.State != "leader" {
				n.startElection()
			}
			n.mu.Unlock()
			electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
			electionTimer.Reset(electionTimeout)

		case ae := <-n.appendEntriesChan:
			n.mu.Lock()
			response := n.handleAppendEntries(ae)
			n.mu.Unlock()
			if response.Success {
				electionTimer.Reset(electionTimeout)
			}

		case vr := <-n.voteRequestChan:
			n.mu.Lock()
			response := n.handleRequestVote(vr)
			n.mu.Unlock()
			if response.VoteGranted {
				electionTimer.Reset(electionTimeout)
			}

		case cmd := <-n.submitChan:
			n.mu.Lock()
			if n.State == "leader" {
				n.Log = append(n.Log, LogEntry{
					Term:    n.CurrentTerm,
					Command: cmd,
				})
				n.log("Leader appended command to log: %s", cmd)
				n.replicateLog()
			} else {
				n.log("Not leader - cannot process command: %s", cmd)
			}
			n.mu.Unlock()
		}
	}
}

// startElection begins a new election
func (n *Node) startElection() {
	n.log("Starting election for term %d", n.CurrentTerm+1)
	n.State = "candidate"
	n.CurrentTerm++
	n.VotedFor = n.ID

	votesReceived := 1 // vote for self

	// Request votes from all peers
	var wg sync.WaitGroup
	for peerID := range n.Peers {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()

			lastLogIndex, lastLogTerm := n.getLastLogInfo()
			request := RequestVoteRequest{
				Term:         n.CurrentTerm,
				CandidateID:  n.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			response, err := n.sendRequestVote(pid, request)
			if err != nil {
				n.log("Error requesting vote from %s: %v", pid, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if response.Term > n.CurrentTerm {
				n.stepDown(response.Term)
				return
			}

			if response.VoteGranted {
				votesReceived++
				n.log("Received vote from %s (total: %d)", pid, votesReceived)
				if votesReceived > len(n.Peers)/2 && n.State == "candidate" {
					n.becomeLeader()
				}
			}
		}(peerID)
	}

	// Wait for all votes to come in (or timeout)
	go func() {
		wg.Wait()
	}()
}

// becomeLeader transitions the node to leader state
func (n *Node) becomeLeader() {
	n.log("Becoming leader for term %d", n.CurrentTerm)
	n.State = "leader"

	// Initialize leader state
	for peerID := range n.Peers {
		n.NextIndex[peerID] = len(n.Log)
		n.MatchIndex[peerID] = -1
	}

	// Send initial empty AppendEntries RPC
	n.replicateLog()

	// Start sending periodic heartbeats
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.mu.Lock()
				if n.State == "leader" {
					n.replicateLog()
				} else {
					n.mu.Unlock()
					return
				}
				n.mu.Unlock()
			case <-n.shutdownChan:
				return
			}
		}
	}()
}

// replicateLog sends AppendEntries RPC to all peers
func (n *Node) replicateLog() {
	if n.State != "leader" {
		return
	}

	for peerID := range n.Peers {
		go func(pid string) {
			n.mu.Lock()
			nextIdx := n.NextIndex[pid]
			prevLogIndex := nextIdx - 1
			var prevLogTerm int
			if prevLogIndex >= 0 {
				prevLogTerm = n.Log[prevLogIndex].Term
			} else {
				prevLogTerm = -1
			}

			var entries []LogEntry
			if nextIdx < len(n.Log) {
				entries = n.Log[nextIdx:]
			}

			request := AppendEntriesRequest{
				Term:         n.CurrentTerm,
				LeaderID:     n.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex,
			}
			n.mu.Unlock()

			response, err := n.sendAppendEntries(pid, request)
			if err != nil {
				n.log("Error sending AppendEntries to %s: %v", pid, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if response.Term > n.CurrentTerm {
				n.stepDown(response.Term)
				return
			}

			if response.Success {
				n.NextIndex[pid] = nextIdx + len(entries)
				n.MatchIndex[pid] = n.NextIndex[pid] - 1
				n.updateCommitIndex()
			} else {
				n.NextIndex[pid]--
				if n.NextIndex[pid] < 0 {
					n.NextIndex[pid] = 0
				}
			}
		}(peerID)
	}
}

// updateCommitIndex updates the commit index based on matchIndex
func (n *Node) updateCommitIndex() {
	if n.State != "leader" {
		return
	}

	// Find the median matchIndex
	matchIndices := make([]int, 0, len(n.MatchIndex))
	for _, idx := range n.MatchIndex {
		matchIndices = append(matchIndices, idx)
	}
	matchIndices = append(matchIndices, len(n.Log)-1) // include leader's log

	// Sort to find median
	for i := range matchIndices {
		for j := i + 1; j < len(matchIndices); j++ {
			if matchIndices[i] > matchIndices[j] {
				matchIndices[i], matchIndices[j] = matchIndices[j], matchIndices[i]
			}
		}
	}

	medianIndex := matchIndices[len(matchIndices)/2]

	// Only commit entries from current term
	if medianIndex > n.CommitIndex && medianIndex < len(n.Log) && n.Log[medianIndex].Term == n.CurrentTerm {
		n.CommitIndex = medianIndex
		n.log("Updated commit index to %d", n.CommitIndex)
		n.applyLog()
	}
}

// applyLog applies committed entries to the state machine
func (n *Node) applyLog() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry := n.Log[n.LastApplied]
		n.log("Applying log entry: %+v", entry)
		// In a real system, we would apply the command to the state machine here
	}
}

// stepDown steps down from leader or candidate state
func (n *Node) stepDown(term int) {
	n.log("Stepping down from %s to follower at term %d", n.State, term)
	n.State = "follower"
	n.CurrentTerm = term
	n.VotedFor = ""
}

// handleAppendEntries processes an incoming AppendEntries RPC
func (n *Node) handleAppendEntries(ae AppendEntriesRequest) AppendEntriesResponse {
	response := AppendEntriesResponse{Term: n.CurrentTerm, Success: false}

	if ae.Term < n.CurrentTerm {
		return response
	}

	if ae.Term > n.CurrentTerm {
		n.CurrentTerm = ae.Term
		n.stepDown(ae.Term)
	}

	// Reset election timeout
	response.Term = n.CurrentTerm

	// Check log consistency
	if ae.PrevLogIndex >= 0 {
		if ae.PrevLogIndex >= len(n.Log) {
			return response
		}
		if n.Log[ae.PrevLogIndex].Term != ae.PrevLogTerm {
			return response
		}
	}

	// Append new entries (ignoring duplicates)
	for i, entry := range ae.Entries {
		idx := ae.PrevLogIndex + 1 + i
		if idx < len(n.Log) {
			if n.Log[idx].Term != entry.Term {
				n.Log = n.Log[:idx]
				n.Log = append(n.Log, entry)
			}
		} else {
			n.Log = append(n.Log, entry)
		}
	}

	// Update commit index
	if ae.LeaderCommit > n.CommitIndex {
		newCommitIndex := ae.LeaderCommit
		if newCommitIndex >= len(n.Log) {
			newCommitIndex = len(n.Log) - 1
		}
		if newCommitIndex > n.CommitIndex {
			n.CommitIndex = newCommitIndex
			n.applyLog()
		}
	}

	response.Success = true
	return response
}

// handleRequestVote processes an incoming RequestVote RPC
func (n *Node) handleRequestVote(vr RequestVoteRequest) RequestVoteResponse {
	response := RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: false}

	if vr.Term < n.CurrentTerm {
		return response
	}

	if vr.Term > n.CurrentTerm {
		n.CurrentTerm = vr.Term
		n.stepDown(vr.Term)
	}

	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	logOk := (vr.LastLogTerm > lastLogTerm) ||
		(vr.LastLogTerm == lastLogTerm && vr.LastLogIndex >= lastLogIndex)

	if (n.VotedFor == "" || n.VotedFor == vr.CandidateID) && logOk {
		n.VotedFor = vr.CandidateID
		response.VoteGranted = true
	}

	return response
}

// getLastLogInfo returns the last log index and term
func (n *Node) getLastLogInfo() (int, int) {
	if len(n.Log) == 0 {
		return -1, -1
	}
	last := n.Log[len(n.Log)-1]
	return len(n.Log) - 1, last.Term
}

// SubmitCommand submits a command to the cluster (leader only)
func (n *Node) SubmitCommand(cmd string) {
	n.submitChan <- cmd
}

// log prints a log message with node ID and term
func (n *Node) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[%s][%s][T%d] %s", n.ID, n.State, n.CurrentTerm, msg)
}

// HTTP handlers
func (n *Node) startServer() {
	http.HandleFunc("/appendEntries", func(w http.ResponseWriter, r *http.Request) {
		var ae AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&ae); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		n.appendEntriesChan <- ae
		response := n.handleAppendEntries(ae)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	http.HandleFunc("/requestVote", func(w http.ResponseWriter, r *http.Request) {
		var vr RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&vr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		n.voteRequestChan <- vr
		response := n.handleRequestVote(vr)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	log.Printf("Node %s listening on %s", n.ID, n.Port)
	if err := http.ListenAndServe(n.Port, nil); err != nil {
		log.Fatalf("Node %s failed to start server: %v", n.ID, err)
	}
}

// sendAppendEntries sends an AppendEntries RPC to a peer
func (n *Node) sendAppendEntries(peerID string, ae AppendEntriesRequest) (AppendEntriesResponse, error) {
	url := fmt.Sprintf("http://%s/appendEntries", n.Peers[peerID])
	resp, err := postJSON(url, ae)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	var response AppendEntriesResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	resp.Body.Close()
	return response, err
}

// sendRequestVote sends a RequestVote RPC to a peer
func (n *Node) sendRequestVote(peerID string, vr RequestVoteRequest) (RequestVoteResponse, error) {
	url := fmt.Sprintf("http://%s/requestVote", n.Peers[peerID])
	resp, err := postJSON(url, vr)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	var response RequestVoteResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	resp.Body.Close()
	return response, err
}

// postJSON is a helper function to send JSON POST requests
func postJSON(url string, data interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func main() {
	// Define cluster configuration
	cluster := map[string]string{
		"node1": ":8080",
		"node2": ":8081",
		"node3": ":8082",
	}

	// Create nodes
	node1 := NewNode("node1", ":8080", removeSelf("node1", cluster))
	node2 := NewNode("node2", ":8081", removeSelf("node2", cluster))
	node3 := NewNode("node3", ":8082", removeSelf("node3", cluster))

	// Start nodes
	go node1.Start()
	go node2.Start()
	go node3.Start()

	// Wait a bit for nodes to start
	time.Sleep(1 * time.Second)

	// Submit some commands to random nodes
	nodes := []*Node{node1, node2, node3}
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)
		node := nodes[rand.Intn(len(nodes))]
		cmd := fmt.Sprintf("command-%d", i)
		node.log("Submitting command: %s", cmd)
		node.SubmitCommand(cmd)
	}

	// Simulate a node failure after some time
	time.Sleep(5 * time.Second)
	node3.log("Simulating node crash")
	node3.Stop()

	// Continue submitting commands
	for i := 5; i < 10; i++ {
		time.Sleep(2 * time.Second)
		node := nodes[rand.Intn(2)] // only node1 and node2
		cmd := fmt.Sprintf("command-%d", i)
		node.log("Submitting command: %s", cmd)
		node.SubmitCommand(cmd)
	}

	// Wait before exiting
	time.Sleep(5 * time.Second)
	node1.Stop()
	node2.Stop()
}

// removeSelf creates a peer list without the node itself
func removeSelf(selfID string, cluster map[string]string) map[string]string {
	peers := make(map[string]string)
	for id, addr := range cluster {
		if id != selfID {
			peers[id] = addr
		}
	}
	return peers
}