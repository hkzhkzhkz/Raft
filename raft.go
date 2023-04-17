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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader    int = 1
	Candidate int = 2
	Follower  int = 3
)

// random election time out   //200ms ~ 350ms
func electionTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

// heartbeat peroid   //100ms
func heartbeatTimeout() time.Duration {
	return time.Second / 10
}

type entry struct {
	Command interface{}
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For Snapshot:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	//Persistent state on all servers  //Updated on stable storage before responding to RPCs
	currentTerm int
	voteFor     int
	log         []entry

	//Volatile state
	state       int
	commitIndex int
	lastApplied int

	//on leader  //Reinitialized after election
	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	//Snapshot       should be persisted
	lastIncludedIndex int
	lastIncludedTerm  int
	data              []byte //snapshot data

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

//persistence

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// pass nil as the second argument to persister.Save()  if there's not yet a snapshot.
// after you've implemented snapshots, pass the current snapshot
func (rf *Raft) persist() {

	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	var buffer bytes.Buffer
	e := labgob.NewEncoder(&buffer)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	for i := 0; i < len(rf.log); i++ {
		e.Encode(rf.log[i])
	}

	raftstate := buffer.Bytes()
	rf.persister.Save(raftstate, rf.data) // persist state and snapshot

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var term int
	var voteFor int
	var log []entry
	//restore data
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&term) != nil || d.Decode(&voteFor) != nil {

		Debug(dPersist, "S%d readPersist error", rf.me)

	} else {
		rf.lastIncludedIndex, rf.lastIncludedTerm = lastIncludedIndex, lastIncludedTerm
		rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
		rf.currentTerm, rf.voteFor = term, voteFor

		//restore log[]
		var tmp entry
		for {
			if d.Decode(&tmp) == nil {
				log = append(log, tmp)
			} else {
				break
			}
		}
		rf.log = log
		rf.log[0].Term = rf.lastIncludedTerm //Sentinel node saves the lastIncludedTerm of snapshot

		// get snapshot persisted previously
		rf.data = rf.persister.ReadSnapshot()

		//don't need to send snapshot to service in here
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var tmp []entry
	tmp = append(tmp, entry{0, 0}) // create Sentinel node

	// delete all log entries up through the last included index
	for i := index + 1; i <= rf.LastLogIndex(); i++ {
		tmp = append(tmp, rf.log[rf.LogIndex(i)])
	}

	rf.lastIncludedTerm = rf.log[rf.LogIndex(index)].Term
	rf.lastIncludedIndex = index
	tmp[0].Term = rf.lastIncludedTerm
	rf.log = tmp
	rf.data = snapshot
	Debug(dSnap, "S%d Service call Snapshot LII=%d LIT=%d lastlogIndex=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.LastLogIndex())
	rf.persist()

}

// 当我们使用snapshot功能时，本地的rf.log[]将会缩短，但我们与其他Raft实例交流时使用的Index不能改变 只能单调增大，
// 所以在索引rf.log[]时，要根据last included index 进行index转换
func (rf *Raft) LogIndex(index int) int { // index转换成在rf.log[]中的实际Index
	return index - rf.lastIncludedIndex
}
func (rf *Raft) LastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

type InstallSnapshotArgs struct { //Send the entire snapshot in a single InstallSnapshot RPC
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC hander
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.state == Follower && args.Term == rf.currentTerm && args.LastIncludedIndex > rf.lastIncludedIndex {

		rf.data = args.Data //discard old snapshot

		var tmp []entry
		tmp = append(tmp, entry{0, args.LastIncludedTerm}) //Sentinel node

		//如果Follower有snapshot中的内容，保留后面的log
		if args.LastIncludedIndex < rf.LastLogIndex() {
			if args.Term == rf.log[rf.LogIndex(args.LastIncludedIndex)].Term {
				for i := args.LastIncludedIndex + 1; i <= rf.LastLogIndex(); i++ {
					tmp = append(tmp, rf.log[rf.LogIndex(i)])
				}
			}
		}

		//如果没有则丢弃整个log
		rf.log = tmp

		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm

		msg := ApplyMsg{false, 0, 0, true, rf.data, rf.lastIncludedTerm, rf.lastIncludedIndex}

		rf.applyCh <- msg

		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex

		rf.electionTimer.Reset(electionTimeout())
		Debug(dSnap, "S%d Receive Snapshot from S%d LII=%d LIT=%d Term=%d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.currentTerm)
		rf.persist()

	}

}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		Debug(dVote, "S%d Refuse Vote to S%d, C:T:%d llT:%d llI:%d, me:T:%d llT:%d llI:%d", rf.me, args.CandidateId,
			args.Term, args.LastLogTerm, args.LastLogIndex, rf.currentTerm, rf.log[len(rf.log)-1].Term, rf.LastLogIndex())
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	if (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.LastLogIndex()) {

		Debug(dVote, "S%d Granting Vote to S%d at T=%d", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.electionTimer.Reset(electionTimeout())
	}

	rf.persist()

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//Fast Backup
	XTerm  int //term of conflicting entry ,  -1: no entry
	XIndex int //index of first entry with XTerm
	XLen   int //length of log
}

// AppendEntries RPC handler    //First log Index=1，preLogIndex=0
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { //过期AE RPC，直接不做任何处理，reply false
		reply.Success = false
		return
	}

	//处理 Term 引起的state转变 和 重置election timer
	if rf.state == Leader { //当身份为Leader时
		if args.Term > rf.currentTerm {
			Debug(dClient, "S%d convert to follower in T%d", rf.me, args.Term)
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.voteFor = args.LeaderId
			rf.electionTimer.Reset(electionTimeout())
		}
	} else if rf.state == Candidate { //当身份为Candidate
		if args.Term >= rf.currentTerm {
			Debug(dClient, "S%d convert to follower in T%d", rf.me, args.Term)
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.voteFor = args.LeaderId
			rf.electionTimer.Reset(electionTimeout())
		}
	} else if rf.state == Follower { //当身份为Follower
		if args.Term > rf.currentTerm {
			Debug(dInfo, "S%d update Term to T%d", rf.me, args.Term)
			rf.currentTerm = args.Term
			rf.voteFor = args.LeaderId
		}
		rf.electionTimer.Reset(electionTimeout())
	}

	reply.Success = false

	//处理从Leader发来的的日志
	if args.PrevLogIndex <= rf.LastLogIndex() && rf.LogIndex(args.PrevLogIndex) >= 0 { //preLogIndex上有日志
		if rf.log[rf.LogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm { //日志的Term也相同
			Debug(dInfo, "S%d Receive AE from S%d, loglength=%d, T%d", rf.me, args.LeaderId, len(args.Entries), args.Term)

			// preLog匹配时:     // 双指针法
			p1 := args.PrevLogIndex + 1                //log中对应于Entries[0]的index
			p2 := 0                                    // Entries[0]的index
			if rf.LastLogIndex() > args.PrevLogIndex { //将要放置Entries日志的位置上已经有日志了
				for p1 <= rf.LastLogIndex() && p2 <= len(args.Entries)-1 {
					if rf.log[rf.LogIndex(p1)].Term != args.Entries[p2].Term {
						//truncate
						Debug(dLog2, "S%d truncate logs after Index=%d", rf.me, p1-1)
						rf.log = rf.log[:rf.LogIndex(p1)]
						break
					}
					p1++
					p2++
				}
			}

			//append剩余的Entries
			for ; p2 <= len(args.Entries)-1; p2++ {
				rf.log = append(rf.log, args.Entries[p2])
			}

			//设置commitIndex(只在成功的时候设置)  commitIndex = min(LeaderCommit,index of last log)
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit >= rf.LastLogIndex() { //go没有min()函数  %>_<%
					rf.commitIndex = rf.LastLogIndex()
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				Debug(dCommit, "S%d update commitIndex to %d", rf.me, rf.commitIndex)
			}

			reply.Success = true

			//preLog不匹配
		} else { //preLogIndex上有Log 但Term不同。 Follower中哨兵节点一定index和term都相同(因为已经被Leader提交)

			reply.XTerm = rf.log[rf.LogIndex(args.PrevLogIndex)].Term
			reply.XLen = rf.LastLogIndex() + 1
			for i := args.PrevLogIndex - 1; rf.LogIndex(i) >= 0; i-- {
				if rf.log[rf.LogIndex(i)].Term != rf.log[rf.LogIndex(args.PrevLogIndex)].Term {
					reply.XIndex = i + 1
					break
				}
			}
		}

	} else { // preLogIndex上没有Log, 超前log或者是落后于snapshot ,都把下一次AE重定位在最后一条log
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.LastLogIndex() + 1
	}

}

// Send a  RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) { //把从客户端来的命令放进log中

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		logs := entry{command, rf.currentTerm}
		rf.log = append(rf.log, logs)
		rf.matchIndex[rf.me]++
		rf.persist()
		Debug(dClient, "S%d Get Command from Client, put in index=%d", rf.me, rf.matchIndex[rf.me])
	}

	index := rf.matchIndex[rf.me]
	term := rf.currentTerm
	isLeader := rf.state == Leader

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() { //kill掉一个Raft实例
	atomic.StoreInt32(&rf.dead, 1)

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		select {
		case <-rf.electionTimer.C: // Election Timeout
			rf.mu.Lock()
			if !rf.killed() {

				if rf.state == Follower {
					Debug(dTimer, "S%d Election timeout", rf.me)
					rf.state = Candidate
					go rf.election()
					rf.electionTimer.Reset(electionTimeout())
					Debug(dClient, "S%d Converting to Candidate,calling election in T%d", rf.me, rf.currentTerm+1)

				} else if rf.state == Candidate {
					Debug(dTimer, "S%d Election timeout", rf.me)
					go rf.election()
					rf.electionTimer.Reset(electionTimeout())
					Debug(dClient, "S%d ReStart election in T%d", rf.me, rf.currentTerm+1)

				} else { //rf.state == Leader
					rf.electionTimer.Reset(electionTimeout())
				}

			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C: // Heartbeat Timeout
			rf.mu.Lock()
			if rf.state == Leader && !rf.killed() {
				rf.heartbeat()
				rf.heartbeatTimer.Reset(heartbeatTimeout())
			}
			rf.mu.Unlock()

		}

	}
}

// 给所有其他的服务器发送的请求投票RPC只发送一次(一个任期内)
// RPC消息往返期间发生故障，就不管了，当作不能投票
func (rf *Raft) election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state == Candidate {
		rf.currentTerm++
		rf.voteFor = rf.me
		voteNum := 1

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int) { // 为每一个server创建一个发送请求投票RPC的线程
				rf.mu.Lock()
				if rf.state != Candidate { //如果已经不是Candidate，就不再发送RequestVote RPC
					rf.mu.Unlock()
					return
				}
				args := RequestVoteArgs{rf.currentTerm, rf.me, rf.LastLogIndex(), rf.log[len(rf.log)-1].Term}
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				if rf.sendRequestVote(index, &args, &reply) { //发送请求投票RPC并等待回复
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//处理关于请求投票RPC的回复
					if rf.state == Candidate && rf.currentTerm == args.Term { //过期回复不予理睬
						if reply.Term > rf.currentTerm {
							Debug(dTerm, "S%d Candidate Convert to Follower T%d", rf.me, reply.Term)
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.voteFor = -1
							rf.persist()
							//rf.electionTimer.Reset(electionTimeout())
						} else if reply.VoteGranted {
							voteNum++
							Debug(dVote, "S%d <- S%d Get vote", rf.me, index)

							if voteNum >= len(rf.peers)/2+1 {
								Debug(dLeader, "S%d Achieved Majority for T%d(%d),converting to Leader", rf.me, rf.currentTerm, voteNum)

								rf.state = Leader
								Debug(dLeader, "S%d initiation nextIndex=%d commitIndex=%d lastApplied=%d", rf.me, rf.LastLogIndex()+1, rf.commitIndex, rf.lastApplied)
								for i := 0; i < len(rf.peers); i++ { //重新初始化nextIndex和matchIndex
									rf.nextIndex[i] = rf.LastLogIndex() + 1
									rf.matchIndex[i] = 0
								}
								//其他server的matchIndex通过AERPC得知，Leader自己的初始化时确定
								rf.matchIndex[rf.me] = rf.LastLogIndex()

								rf.heartbeat() //立即发送一次心跳
								rf.heartbeatTimer.Reset(heartbeatTimeout())
							}
						}
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) heartbeat() { //send AERPC to all Followers

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AE(i)
	}
}

func (rf *Raft) GetAEArgs(index int) (*AppendEntriesArgs, *AppendEntriesReply) {
	lastLogIndex := rf.LastLogIndex()
	nextIndex := rf.nextIndex[index]

	if nextIndex <= rf.lastIncludedIndex { // Follower需要的nextIndex日志已经被Leader丢弃
		return nil, nil
	}

	var logs []entry
	for i := nextIndex; i <= lastLogIndex && rf.LogIndex(i) > 0; i++ {
		logs = append(logs, rf.log[rf.LogIndex(i)])
	}
	Debug(dLeader, "S%d send AERPC to S%d: nextIndex=%d , loglenth=%d", rf.me, index, nextIndex, len(logs))
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1, rf.log[rf.LogIndex(nextIndex-1)].Term, logs, rf.commitIndex}
	reply := &AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) AE(index int) { // send AppendEntries and handle with reply
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args, reply := rf.GetAEArgs(index) // 得到AE的参数
	if args == nil && reply == nil {   // 发送Snapshot
		go rf.SS(index)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if rf.sendAppendEntries(index, args, reply) { //发送AppendEntries并等待回复
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//处理回复
		if rf.state == Leader && rf.currentTerm == args.Term { //不理睬过期消息
			if reply.Term > rf.currentTerm { //发现更大Term,转化为Follower
				Debug(dLeader, "S%d AEresponse's(S%d T%d) Term>currentTerm convert to Follower", rf.me, index, reply.Term)
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.persist()

			} else {
				if reply.Success { // AERPC成功
					rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)

					//commitIndex的增加
					var tmp []int
					for i := range rf.matchIndex {
						tmp = append(tmp, rf.matchIndex[i])
					}
					for i := 0; i < len(tmp)-1; i++ {
						for j := 0; j < len(tmp)-1-i; j++ {
							if tmp[j] > tmp[j+1] {
								t := tmp[j]
								tmp[j] = tmp[j+1]
								tmp[j+1] = t
							}
						}

					}
					mid := tmp[len(tmp)/2]
					//Debug(dCommit, "S%d matchIndex=%d %d %d %d %d tmp=%d %d %d %d %d", rf.me, rf.matchIndex[0], rf.matchIndex[1],
					//	rf.matchIndex[2], rf.matchIndex[3], rf.matchIndex[4], tmp[0], tmp[1], tmp[2], tmp[3], tmp[4])

					if mid > rf.commitIndex && rf.log[rf.LogIndex(mid)].Term == rf.currentTerm {
						Debug(dCommit, "S%d update commitIndex to %d", rf.me, mid)
						rf.commitIndex = mid
					}

				} else { // AERPC 失败，日志不匹配

					//rf.nextIndex[index]--
					//decrement nextIndex and retry (Fast Backup)
					Debug(dLog2, "S%d : S%d's log conflict to Leader, rollback", rf.me, index)

					if reply.XTerm == -1 { //preLogIndex位置为空
						rf.nextIndex[index] = reply.XLen
					} else {

						i := args.PrevLogIndex
						for ; rf.LogIndex(i) >= 0; i-- {
							if rf.log[rf.LogIndex(i)].Term > reply.XTerm {
								continue
							} else if rf.log[rf.LogIndex(i)].Term == reply.XTerm {
								rf.nextIndex[index] = i + 1 //如果Leader有XTerm,设置nextIndex为
								break
							} else { //rf.log[rf.LogIndex(i)].Term < reply.XTerm
								rf.nextIndex[index] = reply.XIndex //如果Leader没有XTerm
								break
							}
						}
						if rf.LogIndex(i) < 0 {
							rf.nextIndex[index] = rf.lastIncludedIndex
						}

					}

					go rf.AE(index)

				}
			}
		}
	}
}

// send Snapshot to Follower and handle with reply
func (rf *Raft) SS(index int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.data}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	if rf.sendInstallSnapshot(index, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == Leader && rf.currentTerm == args.Term {
			if reply.Term > rf.currentTerm {
				Debug(dLeader, "S%d SSresponse's(S%d T%d) Term>currentTerm convert to Follower", rf.me, index, reply.Term)
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.persist()
			} else {
				Debug(dSnap, "S%d install Snapshot to S%d success", rf.me, index)
				rf.nextIndex[index] = rf.lastIncludedIndex + 1
				rf.matchIndex[index] = rf.lastIncludedIndex
				go rf.AE(index)
			}
		}

	}

}

// goroutine apply log to state mechine
func (rf *Raft) applier(appch chan ApplyMsg) {

	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{true, rf.log[rf.LogIndex(rf.lastApplied+1)].Command, rf.lastApplied + 1, false, nil, 0, 0}
			appch <- msg
			rf.lastApplied++
			Debug(dLog, "S%d apply command index=%d", rf.me, rf.lastApplied)
		}
		time.Sleep(time.Millisecond)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialization
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, entry{0, 0}) //Sentinel node 0，first log index is 1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.electionTimer = time.NewTimer(electionTimeout())
	rf.heartbeatTimer = time.NewTimer(heartbeatTimeout())

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier(applyCh)

	Debug(dClient, "S%d Started at T=%d Votefor=%d LII=%d LIT=%d lastLogIndex=%d snapshotData=nil:%t", rf.me, rf.currentTerm, rf.voteFor, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.LastLogIndex(), rf.data == nil)

	return rf
}
