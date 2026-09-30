use std::collections::{HashMap, HashSet};
use crate::config::RaftConfig;
use crate::log::RaftLog;
use crate::message::{Message, MessagePayload};
use crate::types::{HardState, Index, LogEntry, NodeId, Role, Term};

/// Pure deterministic Raft consensus state machine.
pub struct RaftEngine {
    /// Configuration of this node.
    pub config: RaftConfig,
    /// Current consensus role.
    pub role: Role,
    /// Current consensus term.
    pub current_term: Term,
    /// Candidate voted for in current term.
    pub voted_for: Option<NodeId>,
    /// Sequential log.
    pub log: RaftLog,
    /// Highest log index known to be committed.
    pub commit_index: Index,
    /// Highest log index applied to state machine.
    pub last_applied: Index,
    /// Known current leader identifier.
    pub leader_id: Option<NodeId>,
    /// Election timer ticks elapsed.
    pub election_elapsed: u64,
    /// Heartbeat timer ticks elapsed.
    pub heartbeat_elapsed: u64,
    /// Randomized election timeout for current cycle.
    pub randomized_election_timeout: u64,
    /// Tracked next log index to send to each peer (leader only).
    pub next_index: HashMap<NodeId, Index>,
    /// Tracked highest log index known to be replicated on each peer (leader only).
    pub match_index: HashMap<NodeId, Index>,
    /// Pre-votes granted during speculative election.
    pub pre_votes: HashSet<NodeId>,
    /// Formal votes granted during election.
    pub votes: HashSet<NodeId>,
    /// Peers that communicated with leader during current election period.
    pub active_peers: HashSet<NodeId>,
    /// Target node for leadership transfer in progress.
    pub lead_transferee: Option<NodeId>,
    /// Messages queued for network transmission.
    pub outbox: Vec<Message>,
}

impl RaftEngine {
    /// Creates a new Raft state machine engine.
    pub fn new(config: RaftConfig) -> Self {
        let is_learner = config.learners.contains(&config.id);
        let election_timeout = config.election_tick + (config.id * 2) % (config.election_tick.max(2));

        let mut engine = Self {
            config,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: election_timeout,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            pre_votes: HashSet::new(),
            votes: HashSet::new(),
            active_peers: HashSet::new(),
            lead_transferee: None,
            outbox: Vec::new(),
        };

        if is_learner {
            engine.role = Role::Follower;
        }

        engine
    }

    /// Advances internal timers by one logical tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_election_timeout {
                    self.election_elapsed = 0;
                    if !self.config.learners.contains(&self.config.id) {
                        if self.config.pre_vote_enabled {
                            self.campaign_pre_vote();
                        } else {
                            self.campaign_formal_vote();
                        }
                    }
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_tick {
                    self.heartbeat_elapsed = 0;
                    self.bcast_heartbeat();
                }

                if self.config.check_quorum {
                    self.election_elapsed += 1;
                    if self.election_elapsed >= self.config.election_tick {
                        self.election_elapsed = 0;
                        if !self.has_active_quorum() {
                            self.become_follower(self.current_term, None);
                        }
                        self.active_peers.clear();
                        self.active_peers.insert(self.config.id);
                    }
                }
            }
        }
    }

    /// Processes an incoming consensus message.
    pub fn step(&mut self, msg: Message) {
        if msg.term > self.current_term {
            match msg.payload {
                MessagePayload::PreVoteRequest { .. } | MessagePayload::PreVoteResponse { .. } => {}
                _ => {
                    self.become_follower(msg.term, None);
                }
            }
        }

        match msg.payload {
            MessagePayload::PreVoteRequest {
                last_log_index,
                last_log_term,
            } => {
                self.handle_pre_vote_request(msg.from, msg.term, last_log_index, last_log_term);
            }
            MessagePayload::PreVoteResponse { vote_granted } => {
                if self.role == Role::PreCandidate && msg.term == self.current_term + 1 {
                    self.handle_pre_vote_response(msg.from, vote_granted);
                }
            }
            MessagePayload::VoteRequest {
                last_log_index,
                last_log_term,
            } => {
                self.handle_vote_request(msg.from, msg.term, last_log_index, last_log_term);
            }
            MessagePayload::VoteResponse { vote_granted } => {
                if self.role == Role::Candidate && msg.term == self.current_term {
                    self.handle_vote_response(msg.from, vote_granted);
                }
            }
            MessagePayload::AppendEntriesRequest {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    msg.from,
                    msg.term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                );
            }
            MessagePayload::AppendEntriesResponse {
                success,
                match_index,
            } => {
                if self.role == Role::Leader && msg.term == self.current_term {
                    self.handle_append_response(msg.from, success, match_index);
                }
            }
            MessagePayload::HeartbeatRequest { commit_index } => {
                self.handle_heartbeat_request(msg.from, msg.term, commit_index);
            }
            MessagePayload::HeartbeatResponse => {
                if self.role == Role::Leader {
                    self.active_peers.insert(msg.from);
                }
            }
            MessagePayload::TimeoutNow => {
                if !self.config.learners.contains(&self.config.id) {
                    self.campaign_formal_vote();
                }
            }
        }
    }

    /// Submits a new normal entry proposal to the leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, &'static str> {
        if self.role != Role::Leader {
            return Err("not leader");
        }

        let new_index = self.log.last_index() + 1;
        let entry = LogEntry::new_normal(self.current_term, new_index, data);
        let _ = self.log.append(vec![entry]);

        self.match_index.insert(self.config.id, new_index);
        self.next_index.insert(self.config.id, new_index + 1);

        if self.config.peers.len() == 1 {
            self.commit_index = new_index;
        } else {
            self.bcast_append();
        }

        Ok(new_index)
    }

    /// Initiates a leadership transfer to the target node.
    pub fn transfer_leadership(&mut self, target: NodeId) -> Result<(), &'static str> {
        if self.role != Role::Leader {
            return Err("not leader");
        }
        if !self.config.peers.contains(&target) || target == self.config.id {
            return Err("invalid target");
        }

        self.lead_transferee = Some(target);
        let target_match = self.match_index.get(&target).copied().unwrap_or(0);
        if target_match >= self.log.last_index() {
            self.send_timeout_now(target);
        } else {
            self.send_append_to(target);
        }
        Ok(())
    }

    /// Returns the current durable consensus state.
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index,
        }
    }

    /// Drains and returns all queued outbound messages.
    pub fn take_outbox(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.outbox)
    }

    /// Advances `last_applied` to `commit_index` and returns all newly committed entries.
    pub fn drain_committed_entries(&mut self) -> Vec<LogEntry> {
        if self.commit_index > self.last_applied {
            let start = self.last_applied + 1;
            let count = (self.commit_index - self.last_applied) as usize;
            let entries = self.log.slice(start, count);
            self.last_applied = self.commit_index;
            entries
        } else {
            Vec::new()
        }
    }

    fn campaign_pre_vote(&mut self) {
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.pre_votes.clear();
        self.pre_votes.insert(self.config.id);

        let pre_term = self.current_term + 1;
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        let peers: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&id| id != self.config.id)
            .collect();

        if self.config.quorum_model.is_election_quorum(&self.pre_votes, &self.config.peers) {
            self.campaign_formal_vote();
            return;
        }

        for peer in peers {
            self.outbox.push(Message {
                from: self.config.id,
                to: peer,
                term: pre_term,
                payload: MessagePayload::PreVoteRequest {
                    last_log_index,
                    last_log_term,
                },
            });
        }
    }

    fn campaign_formal_vote(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.config.id);
        self.leader_id = None;
        self.election_elapsed = 0;
        self.votes.clear();
        self.votes.insert(self.config.id);

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        let peers: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&id| id != self.config.id)
            .collect();

        if self.config.quorum_model.is_election_quorum(&self.votes, &self.config.peers) {
            self.become_leader();
            return;
        }

        for peer in peers {
            self.outbox.push(Message {
                from: self.config.id,
                to: peer,
                term: self.current_term,
                payload: MessagePayload::VoteRequest {
                    last_log_index,
                    last_log_term,
                },
            });
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.active_peers.clear();
        self.active_peers.insert(self.config.id);

        let last_index = self.log.last_index();
        for &peer in &self.config.peers {
            self.next_index.insert(peer, last_index + 1);
            self.match_index.insert(peer, 0);
        }
        for &learner in &self.config.learners {
            self.next_index.insert(learner, last_index + 1);
            self.match_index.insert(learner, 0);
        }

        self.match_index.insert(self.config.id, last_index);

        let noop_index = last_index + 1;
        let noop = LogEntry::new_noop(self.current_term, noop_index);
        let _ = self.log.append(vec![noop]);
        self.match_index.insert(self.config.id, noop_index);
        self.next_index.insert(self.config.id, noop_index + 1);

        if self.config.peers.len() == 1 {
            self.commit_index = noop_index;
        } else {
            self.bcast_append();
        }
    }

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        self.role = Role::Follower;
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.leader_id = leader_id;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.randomized_election_timeout =
            self.config.election_tick + (self.config.id * 2) % (self.config.election_tick.max(2));
        self.lead_transferee = None;
        self.pre_votes.clear();
        self.votes.clear();
    }

    fn handle_pre_vote_request(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_last_index: Index,
        candidate_last_term: Term,
    ) {
        let is_up_to_date = self.is_log_up_to_date(candidate_last_index, candidate_last_term);
        let can_vote = term > self.current_term && is_up_to_date;

        self.outbox.push(Message {
            from: self.config.id,
            to: from,
            term,
            payload: MessagePayload::PreVoteResponse {
                vote_granted: can_vote,
            },
        });
    }

    fn handle_pre_vote_response(&mut self, from: NodeId, granted: bool) {
        if granted {
            self.pre_votes.insert(from);
            if self.config.quorum_model.is_election_quorum(&self.pre_votes, &self.config.peers) {
                self.campaign_formal_vote();
            }
        }
    }

    fn handle_vote_request(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_last_index: Index,
        candidate_last_term: Term,
    ) {
        let is_up_to_date = self.is_log_up_to_date(candidate_last_index, candidate_last_term);
        let can_vote = (self.voted_for.is_none() || self.voted_for == Some(from))
            && term == self.current_term
            && is_up_to_date;

        if can_vote {
            self.voted_for = Some(from);
            self.election_elapsed = 0;
        }

        self.outbox.push(Message {
            from: self.config.id,
            to: from,
            term: self.current_term,
            payload: MessagePayload::VoteResponse {
                vote_granted: can_vote,
            },
        });
    }

    fn handle_vote_response(&mut self, from: NodeId, granted: bool) {
        if granted {
            self.votes.insert(from);
            if self.config.quorum_model.is_election_quorum(&self.votes, &self.config.peers) {
                self.become_leader();
            }
        }
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) {
        if term < self.current_term {
            self.outbox.push(Message {
                from: self.config.id,
                to: from,
                term: self.current_term,
                payload: MessagePayload::AppendEntriesResponse {
                    success: false,
                    match_index: self.log.last_index(),
                },
            });
            return;
        }

        self.become_follower(term, Some(from));
        self.election_elapsed = 0;

        if prev_log_index > 0 {
            match self.log.term_at(prev_log_index) {
                Some(t) if t == prev_log_term => {}
                _ => {
                    self.outbox.push(Message {
                        from: self.config.id,
                        to: from,
                        term: self.current_term,
                        payload: MessagePayload::AppendEntriesResponse {
                            success: false,
                            match_index: self.log.last_index(),
                        },
                    });
                    return;
                }
            }
        }

        let is_witness = self.config.witnesses.contains(&self.config.id);

        for mut entry in entries {
            if is_witness && entry.entry_type == crate::types::EntryType::Normal {
                entry.data.clear();
            }

            if let Some(existing_term) = self.log.term_at(entry.index) {
                if existing_term != entry.term {
                    self.log.truncate_after(entry.index - 1);
                    let _ = self.log.append(vec![entry]);
                }
            } else {
                let _ = self.log.append(vec![entry]);
            }
        }

        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.log.last_index());
        }

        self.outbox.push(Message {
            from: self.config.id,
            to: from,
            term: self.current_term,
            payload: MessagePayload::AppendEntriesResponse {
                success: true,
                match_index: self.log.last_index(),
            },
        });
    }

    fn handle_append_response(&mut self, from: NodeId, success: bool, match_index: Index) {
        self.active_peers.insert(from);

        if success {
            self.match_index.insert(from, match_index);
            self.next_index.insert(from, match_index + 1);
            self.maybe_advance_commit_index();

            if let Some(target) = self.lead_transferee {
                if target == from && match_index >= self.log.last_index() {
                    self.send_timeout_now(target);
                }
            }
        } else {
            let next = self.next_index.entry(from).or_insert(1);
            if *next > 1 {
                *next -= 1;
            }
            self.send_append_to(from);
        }
    }

    fn handle_heartbeat_request(&mut self, from: NodeId, term: Term, commit_index: Index) {
        if term >= self.current_term {
            self.become_follower(term, Some(from));
            self.election_elapsed = 0;
            if commit_index > self.commit_index {
                self.commit_index = commit_index.min(self.log.last_index());
            }
        }

        self.outbox.push(Message {
            from: self.config.id,
            to: from,
            term: self.current_term,
            payload: MessagePayload::HeartbeatResponse,
        });
    }

    fn maybe_advance_commit_index(&mut self) {
        let last_log_index = self.log.last_index();
        for candidate_idx in ((self.commit_index + 1)..=last_log_index).rev() {
            if let Some(t) = self.log.term_at(candidate_idx) {
                if t == self.current_term {
                    let mut acks = HashSet::new();
                    for &peer in &self.config.peers {
                        if self.match_index.get(&peer).copied().unwrap_or(0) >= candidate_idx {
                            acks.insert(peer);
                        }
                    }
                    if self.config.quorum_model.is_write_quorum(&acks, &self.config.peers) {
                        self.commit_index = candidate_idx;
                        self.bcast_append();
                        break;
                    }
                }
            }
        }
    }

    fn bcast_append(&mut self) {
        let peers: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .filter(|&id| id != self.config.id)
            .collect();

        for peer in peers {
            self.send_append_to(peer);
        }
    }

    fn send_append_to(&mut self, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(1);
        let prev_log_index = next - 1;
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        let entries = self.log.slice(next, 100);

        self.outbox.push(Message {
            from: self.config.id,
            to,
            term: self.current_term,
            payload: MessagePayload::AppendEntriesRequest {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        });
    }

    fn bcast_heartbeat(&mut self) {
        let peers: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .filter(|&id| id != self.config.id)
            .collect();

        for peer in peers {
            self.outbox.push(Message {
                from: self.config.id,
                to: peer,
                term: self.current_term,
                payload: MessagePayload::HeartbeatRequest {
                    commit_index: self.commit_index,
                },
            });
        }
    }

    fn send_timeout_now(&mut self, to: NodeId) {
        self.outbox.push(Message {
            from: self.config.id,
            to,
            term: self.current_term,
            payload: MessagePayload::TimeoutNow,
        });
        self.lead_transferee = None;
    }

    fn is_log_up_to_date(&self, candidate_last_index: Index, candidate_last_term: Term) -> bool {
        let my_last_index = self.log.last_index();
        let my_last_term = self.log.last_term();

        candidate_last_term > my_last_term
            || (candidate_last_term == my_last_term && candidate_last_index >= my_last_index)
    }

    fn has_active_quorum(&self) -> bool {
        self.config
            .quorum_model
            .is_election_quorum(&self.active_peers, &self.config.peers)
    }
}
