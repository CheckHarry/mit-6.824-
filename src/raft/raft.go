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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

var Leader = 1
var Follower = 2
var Candidate = 3

var name_mapping = map[int]string{
	1: "Leader",
	2: "Follower",
	3: "Candidate",
}

type voteinfo struct {
	VotedFor  int64
	VotedTerm int64
}

type generalServerState struct {
	CurrentTerm int64
	Role        int
}

type ballot struct {
	Term   int64
	IsVote bool
	From   int
}

type voteHandler struct {
	count      int
	votingTerm int64
}

const (
	TYPE_AE = 1
	TYPE_RV = 2
	TYPE_HB = 3
)

type message struct {
	term        int64
	MessageType int
	SentPeer    int
	SentIndex   int64
	IsAEsuccess bool
}

type raftLog struct {
	Term int64
	CMD  interface{}
}

type applyhandler struct {
	CurApplyCommitIndex int64
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applych   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	GeneralState generalServerState
	VoteInfo     voteinfo
	VoteQueue    []ballot
	Votemu       sync.Mutex

	VoteHandler  voteHandler
	ApplyHandler applyhandler

	MessageQueue   []message
	MessageQueueMu sync.Mutex
	//	log []Log
	// Volatile Server State
	CommitIndex      int64
	LastApplied      int64
	IsPingSinceLast  bool
	PreviousPingTime int64
	// Volatile Leader State
	LogIndex int64
	Log      []raftLog
	//VoteTimer int64 // A timer that count weather to start a election
	//IsLeader  bool
	NextIndex  []int64
	MatchIndex []int64
}

func (rf *Raft) ConvertTerm(ToConvertTerm int64, ToConvertRole int) bool {
	if ToConvertTerm < rf.GeneralState.CurrentTerm {
		return false
	}
	rf.GeneralState.CurrentTerm = ToConvertTerm
	rf.GeneralState.Role = ToConvertRole
	return true
}

func (rf *Raft) Vote(CandidateTerm int64, CandidateID int64) bool {
	rf.Votemu.Lock()
	defer rf.Votemu.Unlock()
	if CandidateTerm > rf.VoteInfo.VotedTerm {
		rf.VoteInfo.VotedFor = CandidateID
		rf.VoteInfo.VotedTerm = CandidateTerm
		return true
	} else if CandidateTerm == rf.VoteInfo.VotedTerm && CandidateID == rf.VoteInfo.VotedFor {
		return true
	}
	return false
}

func (rf *Raft) ResetElectionTimer() {
	rf.PreviousPingTime = time.Now().Add(time.Duration(600+rand.Int31n(650)) * time.Millisecond).UnixMilli()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.GeneralState.CurrentTerm)
	isleader = rf.GeneralState.Role == Leader
	//fmt.Printf("GETS - %d - %s - %d\n", rf.me, name_mapping[rf.Role], rf.CurrentTerm)
	rf.mu.Unlock()
	return term, isleader
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
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.GeneralState)
	encoder.Encode(rf.VoteInfo)
	encoder.Encode(rf.Log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
	rf.prettyprint("presist")
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
	rf.prettyprint("reboot")
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var GeneralState generalServerState
	var VoteInfo voteinfo
	var Log []raftLog
	if decoder.Decode(&GeneralState) != nil ||
		decoder.Decode(&VoteInfo) != nil ||
		decoder.Decode(&Log) != nil {
		fmt.Printf("%v , %v , %v", GeneralState, VoteInfo, Log)
	} else {
		//fmt.Printf("%v , %v , %v", GeneralState, VoteInfo, Log)
		rf.GeneralState = GeneralState
		rf.VoteInfo = VoteInfo
		rf.Log = Log
		//fmt.Printf("%v , %v , %v", rf.GeneralState, rf.VoteInfo, rf.Log)
	}

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int64
	CandidateId   int64
	LastLogIndex  int64 // for elect restriction
	LastLogTerm   int64 // for elect restriction
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	IsGrantVote bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.GeneralState.CurrentTerm
	reply.IsGrantVote = false
	if args.CandidateTerm >= rf.GeneralState.CurrentTerm {
		// election restriction here
		if (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 > int(args.LastLogIndex)) || rf.Log[len(rf.Log)-1].Term > args.LastLogTerm {
			rf.prettyprint(fmt.Sprintf("Reject vote because of election restriction , candidate id : %d , term %d", args.CandidateId, args.CandidateTerm))
			return
		}
		is_vote := rf.Vote(args.CandidateTerm, args.CandidateId)
		if is_vote {
			reply.IsGrantVote = true
			rf.ConvertTerm(args.CandidateTerm, Follower)
			rf.ResetElectionTimer()
		} else {
			reply.IsGrantVote = false
		}
		rf.persist()
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

// AppendEntries RPC definition here:

type AppendEntriesArgs struct {
	LeaderTerm        int64
	LeaderID          int64
	PrevLogIndex      int64
	PrevLogTerm       int64
	Entry             raftLog
	LeaderCommitIndex int64
	//Entries      []Log # to be defined
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.GeneralState.CurrentTerm

	if args.LeaderTerm < rf.GeneralState.CurrentTerm {
		reply.Success = false
		return
	}

	rf.ResetElectionTimer()

	if args.LeaderTerm > rf.GeneralState.CurrentTerm {
		rf.ConvertTerm(args.LeaderTerm, Follower)
		rf.persist()
	}

	rf.prettyprint(fmt.Sprintf("get append %v , LeaderCommit %d , log index : %d, term %d , Log : %v", args, args.LeaderCommitIndex, len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term, rf.Log))
	if !(len(rf.Log)-1 >= int(args.PrevLogIndex) && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm) || len(rf.Log)-1 < int(args.PrevLogIndex) {
		reply.Success = false
		return
	}
	rf.prettyprint(fmt.Sprintf("Before insert, the Log is %v", rf.Log))
	if args.Entry.Term != -1 {
		if len(rf.Log)-1 >= int(args.PrevLogIndex)+1 && rf.Log[int(args.PrevLogIndex)+1].Term != args.Entry.Term {
			rf.Log = rf.Log[:args.PrevLogIndex+1]
		}
		if len(rf.Log)-1 == int(args.PrevLogIndex) {
			rf.Log = append(rf.Log, args.Entry)
		}

	}

	if args.LeaderCommitIndex > rf.CommitIndex {
		rf.prettyprint(fmt.Sprintf("Change commit index from %d to %d", rf.CommitIndex, min(args.LeaderCommitIndex, int64(len(rf.Log)-1))))
		rf.CommitIndex = min(args.LeaderCommitIndex, int64(len(rf.Log)-1))
	}

	rf.prettyprint(fmt.Sprintf("Now the Log is %v", rf.Log))

	reply.Success = true
	rf.persist()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.GeneralState.Role == Leader {
		term = int(rf.GeneralState.CurrentTerm)
		index = len(rf.Log)
		rf.prettyprint(fmt.Sprintf("get cmd with term %d, index %d , content %v", term, index, command))
		rf.Log = append(rf.Log, raftLog{int64(term), command})
		rf.persist()
		isLeader = true
		rf.prettyprint(fmt.Sprintf("now the Log is like %v", rf.Log))
	}

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.ConvertTerm(rf.GeneralState.CurrentTerm+1, Candidate)
	wg := sync.WaitGroup{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.Vote(rf.GeneralState.CurrentTerm, int64(rf.me))
			rf.Votemu.Lock()
			Ballot := ballot{rf.GeneralState.CurrentTerm, true, i}
			rf.VoteQueue = append(rf.VoteQueue, Ballot)
			rf.Votemu.Unlock()
			continue
		}
		wg.Add(1)
		go func(j int) {
			args := RequestVoteArgs{rf.GeneralState.CurrentTerm, int64(rf.me), int64(len(rf.Log) - 1), int64(rf.Log[len(rf.Log)-1].Term)}
			reply := RequestVoteReply{-1, false}
			term := rf.GeneralState.CurrentTerm
			wg.Done()
			success := rf.sendRequestVote(j, &args, &reply)
			if success {
				rf.Votemu.Lock()
				Ballot := ballot{term, false, j}
				if reply.IsGrantVote {
					Ballot.IsVote = true
				}
				rf.VoteQueue = append(rf.VoteQueue, Ballot)
				rf.Votemu.Unlock()
				rf.MessageQueueMu.Lock()
				rf.MessageQueue = append(rf.MessageQueue, message{reply.Term, TYPE_RV, -1, -1, false})
				rf.MessageQueueMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) HandleVote() {
	for {
		rf.Votemu.Lock()
		if len(rf.VoteQueue) == 0 {
			rf.Votemu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		//rf.prettyprint(fmt.Sprintf("q :%v", rf.VoteQueue))

		ballot := rf.VoteQueue[0]
		rf.VoteQueue = rf.VoteQueue[1:]
		if ballot.IsVote && ballot.Term > rf.VoteHandler.votingTerm {
			rf.VoteHandler.count = 1
			rf.VoteHandler.votingTerm = ballot.Term
		} else if ballot.IsVote && ballot.Term == rf.VoteInfo.VotedTerm {
			rf.VoteHandler.count++
		}
		rf.persist()
		rf.mu.Unlock()
		rf.Votemu.Unlock()
		if rf.VoteHandler.count >= len(rf.peers)/2+1 {
			rf.mu.Lock()
			rf.Votemu.Lock()
			rf.prettyprint(fmt.Sprintf("elected as TERM %d Leader , get vote %d , queue len : %d , %v", rf.VoteHandler.votingTerm, rf.VoteHandler.count, len(rf.VoteQueue), rf.VoteQueue))
			rf.Votemu.Unlock()
			rf.VoteHandler.count = 0 // reset the count to avoid trigger again
			rf.MatchIndex = make([]int64, len(rf.peers))
			rf.NextIndex = make([]int64, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.MatchIndex[i] = 0
				rf.NextIndex[i] = int64(len(rf.Log))
			}
			rf.ConvertTerm(rf.VoteHandler.votingTerm, Leader)
			rf.persist()
			go rf.heartbeat()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		is_leader := rf.GeneralState.Role == Leader
		if !is_leader {
			rf.mu.Unlock()
			break
		}
		wg := sync.WaitGroup{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(j int) {
				NextIndex := rf.NextIndex[j]
				args := AppendEntriesArgs{rf.GeneralState.CurrentTerm, int64(rf.me), int64(NextIndex - 1), rf.Log[NextIndex-1].Term, raftLog{-1, nil}, rf.CommitIndex}
				reply := AppendEntriesReply{-1, false}
				wg.Done()
				success := rf.sendAppendEntries(j, &args, &reply)
				if success {
					rf.MessageQueueMu.Lock()
					rf.MessageQueue = append(rf.MessageQueue, message{reply.Term, TYPE_HB, -1, -1, false})
					rf.MessageQueueMu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) HandleMessage() {
	for rf.killed() == false {
		rf.MessageQueueMu.Lock()
		if len(rf.MessageQueue) == 0 {
			time.Sleep(10 * time.Millisecond)
			rf.MessageQueueMu.Unlock()
			continue
		}
		message := rf.MessageQueue[0]
		rf.MessageQueue = rf.MessageQueue[1:]
		rf.MessageQueueMu.Unlock()
		rf.mu.Lock()
		if message.term > rf.GeneralState.CurrentTerm {
			rf.ConvertTerm(message.term, Follower)
			rf.persist()
			//go rf.CheckLeaderState()
		} else {

			if message.MessageType == TYPE_AE {
				rf.prettyprint(fmt.Sprintf("get AE received message , message sentindex %d , %d , id %d , %t", message.SentIndex, rf.NextIndex[message.SentPeer], message.SentPeer, message.IsAEsuccess))
				if message.SentIndex == rf.NextIndex[message.SentPeer] {
					if message.IsAEsuccess {
						rf.MatchIndex[message.SentPeer] = rf.NextIndex[message.SentPeer]
						rf.NextIndex[message.SentPeer]++
					} else {
						rf.NextIndex[message.SentPeer]--
					}
				}
			}
		}
		//rf.prettyprint(fmt.Sprintf("the match index array %v", rf.MatchIndex))
		rf.mu.Unlock()
	}
}

func (rf *Raft) CheckLeaderState() {
	for rf.killed() == false {
		rf.mu.Lock()
		is_leader := rf.GeneralState.Role == Leader
		if is_leader {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if time.Now().UnixMilli() > rf.PreviousPingTime {
			rf.ResetElectionTimer()
			go rf.StartElection()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}

	}
}

func (rf *Raft) ApplyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.CommitIndex > rf.ApplyHandler.CurApplyCommitIndex {
			rf.ApplyHandler.CurApplyCommitIndex++
			to_apply := ApplyMsg{}
			to_apply.Command = rf.Log[rf.ApplyHandler.CurApplyCommitIndex].CMD
			to_apply.CommandIndex = int(rf.ApplyHandler.CurApplyCommitIndex)
			to_apply.CommandValid = true
			rf.mu.Unlock()
			rf.applych <- to_apply
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

	}
}

func (rf *Raft) ReplicateLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		is_leader := rf.GeneralState.Role == Leader
		if !is_leader {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		//rf.prettyprint("REPLICATELOG Here!")
		var wg sync.WaitGroup

		commit_map := make(map[int64]int)
		for i := 0; i < len(rf.peers); i++ {
			if _, ok := commit_map[rf.MatchIndex[i]]; ok {
				commit_map[rf.MatchIndex[i]]++
			} else {
				commit_map[rf.MatchIndex[i]] = 1
			}

			if i == rf.me {
				rf.MatchIndex[i] = int64(len(rf.Log) - 1)
				continue
			}
			//rf.prettyprint(fmt.Sprintf("ReplicateLog , preindex %d , peer %d nextindex %d", len(rf.Log)-1, i, int(rf.NextIndex[i])))
			if len(rf.Log)-1 >= int(rf.NextIndex[i]) {
				// AE here
				wg.Add(1)
				go func(j int) {
					NextIndex := rf.NextIndex[j]
					args := AppendEntriesArgs{
						rf.GeneralState.CurrentTerm,
						int64(rf.me),
						int64(NextIndex - 1),
						rf.Log[NextIndex-1].Term,
						rf.Log[NextIndex],
						rf.CommitIndex}
					reply := AppendEntriesReply{-1, false}
					rf.prettyprint(fmt.Sprintf("Send AE with prevIndex and prevTerm %d , %d ", int64(NextIndex-1), rf.Log[NextIndex-1].Term))
					wg.Done()
					success := rf.sendAppendEntries(j, &args, &reply)
					if success {
						rf.MessageQueueMu.Lock()
						rf.MessageQueue = append(rf.MessageQueue, message{reply.Term, TYPE_AE, j, NextIndex, reply.Success})
						rf.MessageQueueMu.Unlock()
					}
				}(i)
			}
		}
		wg.Wait()

		// iterate through the commimt_map
		type pair struct {
			k int64
			v int
		}
		keys := make([]pair, 0)
		for k, v := range commit_map {
			keys = append(keys, pair{k, v})
		}

		sort.Slice(keys, func(i, j int) bool { return keys[i].k < keys[j].k })
		rf.prettyprint(fmt.Sprintf("key %v", keys))
		count := 0
		for i := len(keys) - 1; i >= 0; i-- {
			count += keys[i].v
			if count >= len(rf.peers)/2+1 && rf.Log[keys[i].k].Term == rf.GeneralState.CurrentTerm {
				rf.prettyprint(fmt.Sprintf("Leader rf.CommitIndex %d change to %d , commit_map %v", rf.CommitIndex, keys[i].k, commit_map))
				rf.CommitIndex = keys[i].k
				break
			}
		}
		rf.prettyprint((fmt.Sprintf("Now the log is after count commit %v", rf.Log)))
		rf.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
}

func (rf *Raft) prettyprint(msg string) {
	prefix := fmt.Sprintf("%d id (TERM %d) %s", rf.me, rf.GeneralState.CurrentTerm, name_mapping[rf.GeneralState.Role])
	fmt.Printf("%s : %s \n", prefix, msg)
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
	rf.applych = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.GeneralState = generalServerState{0, Follower}
	rf.VoteInfo = voteinfo{-1, -1}
	rf.LogIndex = 0
	rf.Log = []raftLog{{-1, nil}} // 0 index is always -1 to prevent edge case
	rf.CommitIndex = 0
	rf.ApplyHandler = applyhandler{0}
	// initialize from state persisted before a crash
	rf.ResetElectionTimer()
	rf.readPersist(persister.ReadRaftState())
	if rf.GeneralState.Role == Leader {
		rf.MatchIndex = make([]int64, len(rf.peers))
		rf.NextIndex = make([]int64, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.MatchIndex[i] = 0
			rf.NextIndex[i] = int64(len(rf.Log))
		}
	}
	// start ticker goroutine to start elections

	go rf.HandleVote()
	go rf.HandleMessage()
	go rf.CheckLeaderState()
	go rf.ReplicateLog()
	go rf.ApplyLog()
	// start heartbeat (heartbeat may contain logic tocheck wheather it self is leader)

	return rf
}
