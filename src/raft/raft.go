package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import (
	"labrpc"
)

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type 	ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status    string                 // follower,candidate, leader
	receiveAt int64         // unix time if receive a hear beat
	// Persistent state on all servers:
	currentTerm int 			 // latest term server has seen
	votedFor    int           // candidateId for this term
	log         []LogEntry           //
	// Volatile state on all servers:
	commitIndex int             // index of highest log entry known to be committed
	lastApplied  int            // index of highest log entry applied to state machine
	// Volatile state on leaders:
	nextIndex[]  int           // for each server, index of the next log entry to send to that server
	matchIndex[] int 		   // for each server, index of highest log entry known to be replicated on server
	// vote buckets
	voteBucket[] int          // save voter

	// timer uuid
	followerTimerUUID string
	candidateTimerUUID string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	status := rf.status
	rf.mu.Unlock()
	return term, status == "Leader"
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int             // candidate term
	CandidateId  int        // candidate's id
	LastLogIndex int           // index of candidate’s last log entry
	LastLogTerm  int           // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term       int            // currentTerm, for candidate to update itself
	VoteGranted bool          // true if vote for this candidate
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// safety unlock
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// term check
	if args.Term < rf.currentTerm {
		_, _ = DPrintf("Raft %d-%s: Candidate term {%d} is smaller than current term {%d}", rf.me, rf.status, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		go rf.TurnToFollower()
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		_, _ = DPrintf("Raft %d-%s-%d: has voted for %d, candidate %d is loser.",
			rf.me, rf.status, rf.currentTerm, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}


	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
		if args.LastLogIndex >= rf.lastApplied {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if rf.currentTerm < args.Term {
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm
				go rf.TurnToFollower()
			}
			return
		} else {
			_, _ = DPrintf("Raft %d-%s: candidate's log index {%d} are not as up-to-date as mine {%d}",
				rf.me, rf.status, args.LastLogIndex, rf.lastApplied)
			return
		}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// log entry
type LogEntry struct {

}

// append entries
type AppendEntriesArgs struct {
	Term          int     //leader's term
	LeaderId      int	   //follower can redirect client
	PrevLogIndex  int  //index of log entry immediately preceding new ones
	PrevLogTerm   int    //term of prevLogIndex entry
	Entries       []*LogEntry // log entries to store, empty for heartbeat; send mor than one for efficiency
	LeaderCommit  int    // leader's commitIndex
}

type AppendEntriesReply struct {
	Term     int // currentTerm, for leader to update itself
	Success  bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntry RPC handler
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		_, _ = DPrintf("Raft %d-%s: append entry: Leader %d term {%d} is less than mine: {%d}.", rf.me, rf.status, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 2A heartbeat
	if len(args.Entries) == 0 {
		rf.heartBeatHandler(args)
		reply.Success = true
		return
	}
}

func (rf *Raft) heartBeatHandler(args *AppendEntriesArgs) {
	_, _ = DPrintf("Raft %d-%s-%d: receive heartbeat from leader: %d.", rf.me, rf.status,rf.currentTerm, args.LeaderId)
	// term check
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = "Follower"
		rf.TurnToFollower()
		return
	}

	// candidate to follower
	if rf.status == "Candidate" {
		rf.status = "Follower"
		rf.TurnToFollower()
		return
	}

	// just rest timeout
	go rf.FollowerCountDown()
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// use to send entry to follower
func (rf *Raft) SendHeartbeat(index int) {
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogTerm: 0,
		PrevLogIndex: 0,
		Entries: []*LogEntry{},
		LeaderCommit: 0,
	}

	reply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}

	ok := rf.sendAppendEntry(index, &args, &reply)

	if !ok {
		_, _ = DPrintf("Raft %d-%s-%d: peer: %d refused my heartbeat.", rf.me, rf.status, rf.currentTerm, index)
	} else  {
		_, _ = DPrintf("Raft %d-%s-%d: peer: %d response my heartbeat.", rf.me, rf.status, rf.currentTerm, index)
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			go rf.TurnToFollower()
		}
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// if term in rpc req > currentTerm, turn to follower
func (rf *Raft) TurnToFollower() {
	rf.mu.Lock()
	rf.status = "Follower"
	rf.votedFor = -1
	rf.mu.Unlock()
	go rf.FollowerCountDown()
}

func (rf *Raft) FollowerCountDown() {
	idBefore := CreateCaptcha()
	rf.followerTimerUUID = idBefore
	_, _ = DPrintf("Rafe %d-%s-%d: timer id: %s Timer before sleep %s", rf.me, rf.status, rf.currentTerm, idBefore, rf.followerTimerUUID)
	countDown := randomACountDown()
	time.Sleep(time.Duration(countDown) * time.Millisecond)

	idAfter := rf.followerTimerUUID

	_, _ = DPrintf("Rafe %d-%s-%d: timer id: %s Timer after sleep %s", rf.me, rf.status, rf.currentTerm, idBefore ,rf.followerTimerUUID)
	validTimer := idBefore == idAfter

	if !validTimer {
		_, _ = DPrintf("Rafe %d-%s-%d: follower timer has been instead, deprecate it.", rf.me, rf.status, rf.currentTerm)
		return
	}

	if rf.status != "Follower" {
		_, _ = DPrintf("Rafe %d-%s-%d: I'm not follower now, deprecate timeout.", rf.me, rf.status, rf.currentTerm)
		return
	}

	// turn to candidate
	if rf.status == "Follower" {
		_, _ = DPrintf("Rafe %d-%s-%d: valid follower Timer!!", rf.me, rf.status, rf.currentTerm)
		rf.turnStateToCandidate()
	}
}

func (rf *Raft) CandidateCountDown() {
	//idBefore := CreateCaptcha()
	//rf.candidateTimerUUID = idBefore
	_, _ = DPrintf("Rafe %d-%s-%d: Start candidate count down.", rf.me, rf.status, rf.currentTerm)
	countDown := randomACountDown()
	time.Sleep(time.Duration(countDown) * time.Millisecond)

	//idAfter := rf.followerTimerUUID
	//validTimer := idBefore == idAfter
	//if !validTimer {
	//	_, _ = DPrintf("Rafe %d-%s-%d: candidate timer has been instead, deprecate it.", rf.me, rf.status, rf.currentTerm)
	//	return
	//}

	// reset vote
	rf.mu.Lock()
	status := rf.status
	rf.mu.Unlock()

	if status == "Leader" {
		_, _ = DPrintf("Rafe %d-%s-%d: Now i'm a Leader.", rf.me, rf.status, rf.currentTerm)
		return
	}

	if status == "Follower" {
		_, _ = DPrintf("Rafe %d-%s-%d: Now i'm a follower.", rf.me, rf.status, rf.currentTerm)
		return
	}

	if status == "Candidate" {
		_, _ = DPrintf("Rafe %d-%s-%d: start a new term for candidate.", rf.me, rf.status, rf.currentTerm)
		rf.voteBucket = []int {}
		rf.votedFor = -1
		go rf.turnStateToCandidate()
	}

}

// periodically sent heart beat
func (rf *Raft) LeaderHeartbeatPeriodically() {
	// send heartbeat
	for index, _ := range rf.peers {
		if rf.me != index {
			go rf.SendHeartbeat(index)
		}
	}

	for {
		countDown := heartBeatDuration()
		time.Sleep(time.Duration(countDown) * time.Millisecond)

		// if not leader, stop leader heartbeat send
		isLeader := rf.status == "Leader"
		if !isLeader {
			_, _ = DPrintf("Rafe %d-%s-%d: I'm no longer leader!.", rf.me, rf.status, rf.currentTerm)
			break
		}

		// send heartbeat
		for index, _ := range rf.peers {
			if rf.me != index {
				go rf.SendHeartbeat(index)
			}
		}
	}
}

// candidate part
func (rf *Raft) turnStateToCandidate() {
	_, _ = DPrintf("Rafe %d-%s-%d: I'm becoming a candidate!.", rf.me, rf.status, rf.currentTerm)

	rf.followerTimerUUID = ""

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.status = "Candidate"
	rf.voteBucket = []int {}
	rf.votedFor = -1

	go rf.CandidateCountDown()
	// send request vote
	rf.parallelRequestVote()

}

func (rf *Raft) parallelRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.votedFor = i
		} else {
			go rf.RequestForVote(i)
		}
	}
}

func (rf *Raft) RequestForVote(index int) {
	args := RequestVoteArgs {
		rf.currentTerm,
		rf.me,
		rf.lastApplied,
		0,
	}

	reply := RequestVoteReply{}

	rf.sendRequestVote(index, &args, &reply)

	rf.mu.Lock()
	thisTerm := rf.currentTerm
	rf.mu.Unlock()

	if thisTerm < reply.Term {
		_, _ = DPrintf("Rafe %d-%s-%d : peer %d term %d is larger than mine %d ! become follower",
			rf.me, rf.status, rf.currentTerm, index, reply.Term, thisTerm,
		)
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.mu.Unlock()
		rf.TurnToFollower()
		return
	}

	if reply.VoteGranted {
		// handle res
		_, _ = DPrintf("Rafe %d-%s-%d : peer %d grant me!", rf.me, rf.status, rf.currentTerm, index)

		if reply.Term < rf.currentTerm {
			_, _ = DPrintf("Rafe %d-%s-%d : peer %d vote's term %d is older than mine %d ! invalid vote",
				rf.me, rf.status, rf.currentTerm, index, reply.Term, rf.currentTerm,
			)
			return
		}

		rf.voteBucket = append(rf.voteBucket, index)
		rf.CheckIfEnoughVote()
	} else {
		_, _ = DPrintf("Raft %d-%s-%d : peer %d refused my vote req.", rf.me, rf.status, rf.currentTerm, index)
	}

}

func (rf *Raft) CheckIfEnoughVote()  {
	voteCount := len(rf.voteBucket)
	if voteCount >= len(rf.peers)/ 2 {
		rf.TurnToLeader()
	}
}

// leader state
func (rf *Raft) TurnToLeader() {
	statusBefore := ""

	rf.mu.Lock()
	statusBefore = rf.status
	if rf.status == "Candidate" {
		rf.status = "Leader"
		rf.voteBucket = []int {}
		rf.votedFor = -1
	}
	rf.mu.Unlock()

	// send heartbeat
	if statusBefore == "Candidate" {
		_, _ = DPrintf("Raft %d-%s-%d: I'm becoming a leader!.", rf.me, rf.status, rf.currentTerm)
		go rf.LeaderHeartbeatPeriodically()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	_, _ = DPrintf("Raft %d-%s-%d : I'm dead!!!", rf.me, rf.status, rf.currentTerm)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.status = "Follower"
	rf.followerTimerUUID = ""

	_, _ = DPrintf("Raft %d-%s : I'm starting", rf.me, rf.status)

	// Your initialization code here (2A, 2B, 2C).
	go rf.FollowerCountDown()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func GetMillsSecond() int64 {
	return time.Now().UnixNano() / 1e6
}


func CreateCaptcha() string {
	return fmt.Sprintf("%06v", rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000000))
}

func randomACountDown() int {
	countDown := rand.Intn(250) + 300
	return countDown
}

func heartBeatDuration() int {
	countDown := rand.Intn(80) + 100
	return countDown
}
