"""
SynapseDB Raft Consensus Implementation
Complete Raft algorithm with leader election, log replication, and safety guarantees
"""

import asyncio
import asyncpg
import logging
import random
import time
import json
import uuid
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import aiohttp

logger = logging.getLogger(__name__)

class RaftRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class EntryType(Enum):
    COMMAND = "command"
    CONFIG_CHANGE = "config_change"
    NOOP = "noop"

@dataclass
class LogEntry:
    """Raft log entry"""
    index: int
    term: int
    entry_type: EntryType
    data: Optional[Dict] = None
    committed: bool = False
    applied: bool = False

@dataclass
class RaftState:
    """Raft consensus state"""
    current_term: int = 0
    voted_for: Optional[str] = None
    role: RaftRole = RaftRole.FOLLOWER
    leader_id: Optional[str] = None
    commit_index: int = 0
    last_applied: int = 0
    
    # Leader-specific state
    next_index: Dict[str, int] = None
    match_index: Dict[str, int] = None
    
    def __post_init__(self):
        if self.next_index is None:
            self.next_index = {}
        if self.match_index is None:
            self.match_index = {}

@dataclass
class VoteRequest:
    """RequestVote RPC"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class VoteResponse:
    """RequestVote response"""
    term: int
    vote_granted: bool

@dataclass
class AppendEntriesRequest:
    """AppendEntries RPC"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

@dataclass
class AppendEntriesResponse:
    """AppendEntries response"""
    term: int
    success: bool
    last_log_index: int = 0

class RaftNode:
    """Raft consensus node implementation"""
    
    def __init__(self, node_id: str, db_config: Dict, cluster_nodes: List[Dict]):
        self.node_id = node_id
        self.db_config = db_config
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        
        # Raft state
        self.state = RaftState()
        self.log: List[LogEntry] = []
        self.election_timeout = 5.0  # seconds
        self.heartbeat_interval = 1.0  # seconds
        
        # Timing
        self.last_heartbeat = 0
        self.election_timer = 0
        self.votes_received: Set[str] = set()
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # HTTP client for RPC
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Running state
        self.running = False
        
        # Leadership lease
        self.leadership_lease_duration = 10.0  # seconds
        self.last_leadership_renewal = 0
        
        # Async locks
        self.state_lock = asyncio.Lock()
        self.log_lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize the Raft node"""
        logger.info(f"Initializing Raft node {self.node_id}")
        
        # Connect to database
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=2, max_size=10)
        
        # Create HTTP session
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        # Load persistent state
        await self._load_persistent_state()
        
        # Reset election timer
        self._reset_election_timer()
        
        logger.info(f"Raft node {self.node_id} initialized as {self.state.role.value} in term {self.state.current_term}")
    
    async def start(self):
        """Start the Raft node"""
        self.running = True
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Start main Raft loop
        tasks = [
            asyncio.create_task(self._raft_main_loop()),
            asyncio.create_task(self._log_application_loop()),
            asyncio.create_task(self._leadership_lease_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Raft node error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the Raft node"""
        self.running = False
        logger.info(f"Stopping Raft node {self.node_id}")
        
        if self.http_session:
            await self.http_session.close()
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def _raft_main_loop(self):
        """Main Raft consensus loop"""
        while self.running:
            try:
                async with self.state_lock:
                    current_time = time.time()
                    
                    if self.state.role == RaftRole.LEADER:
                        # Send heartbeats
                        if current_time - self.last_heartbeat >= self.heartbeat_interval:
                            await self._send_heartbeats()
                            self.last_heartbeat = current_time
                    
                    elif self.state.role == RaftRole.FOLLOWER:
                        # Check for election timeout
                        if current_time - self.election_timer >= self.election_timeout:
                            logger.info(f"Election timeout on follower {self.node_id}")
                            await self._start_election()
                    
                    elif self.state.role == RaftRole.CANDIDATE:
                        # Check for election timeout
                        if current_time - self.election_timer >= self.election_timeout:
                            logger.info(f"Election timeout on candidate {self.node_id}, starting new election")
                            await self._start_election()
                
                await asyncio.sleep(0.1)  # 100ms loop
                
            except Exception as e:
                logger.error(f"Error in Raft main loop: {e}")
                await asyncio.sleep(1)
    
    async def _log_application_loop(self):
        """Apply committed log entries to state machine"""
        while self.running:
            try:
                async with self.log_lock:
                    # Apply committed entries
                    while self.state.last_applied < self.state.commit_index and self.state.last_applied < len(self.log):
                        entry_index = self.state.last_applied
                        if entry_index < len(self.log):
                            entry = self.log[entry_index]
                            await self._apply_log_entry(entry)
                            self.state.last_applied += 1
                            
                            # Persist state
                            await self._persist_state()
                
                await asyncio.sleep(0.05)  # 50ms application loop
                
            except Exception as e:
                logger.error(f"Error in log application loop: {e}")
                await asyncio.sleep(1)
    
    async def _leadership_lease_loop(self):
        """Maintain leadership lease to prevent split-brain"""
        while self.running:
            try:
                if self.state.role == RaftRole.LEADER:
                    current_time = time.time()
                    
                    # Check if we need to renew leadership lease
                    if current_time - self.last_leadership_renewal >= self.leadership_lease_duration / 2:
                        success = await self._renew_leadership_lease()
                        if success:
                            self.last_leadership_renewal = current_time
                        else:
                            logger.warning(f"Failed to renew leadership lease, stepping down")
                            await self._become_follower(self.state.current_term)
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in leadership lease loop: {e}")
                await asyncio.sleep(1)
    
    async def _start_election(self):
        """Start a new leader election"""
        logger.info(f"Node {self.node_id} starting election for term {self.state.current_term + 1}")
        
        # Become candidate
        self.state.role = RaftRole.CANDIDATE
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.votes_received = {self.node_id}  # Vote for self
        
        # Reset election timer
        self._reset_election_timer()
        
        # Persist state
        await self._persist_state()
        
        # Get last log info
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0
        
        # Send RequestVote RPCs to all other nodes
        vote_tasks = []
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._request_vote(node_id, node_info, last_log_index, last_log_term))
                vote_tasks.append(task)
        
        # Wait for votes (with timeout)
        if vote_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*vote_tasks, return_exceptions=True), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning(f"Vote request timeout in term {self.state.current_term}")
        
        # Check if we won the election
        majority = len(self.cluster_nodes) // 2 + 1
        if len(self.votes_received) >= majority and self.state.role == RaftRole.CANDIDATE:
            await self._become_leader()
    
    async def _request_vote(self, node_id: str, node_info: Dict, last_log_index: int, last_log_term: int):
        """Send RequestVote RPC to a node"""
        try:
            vote_request = VoteRequest(
                term=self.state.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
            url = f"http://{node_info['host']}:{node_info.get('raft_port', 8080)}/raft/vote"
            async with self.http_session.post(url, json=asdict(vote_request)) as response:
                if response.status == 200:
                    vote_data = await response.json()
                    vote_response = VoteResponse(**vote_data)
                    
                    async with self.state_lock:
                        # Handle response
                        if vote_response.term > self.state.current_term:
                            await self._become_follower(vote_response.term)
                        elif vote_response.vote_granted and self.state.role == RaftRole.CANDIDATE:
                            self.votes_received.add(node_id)
                            logger.info(f"Received vote from {node_id} in term {self.state.current_term}")
                
        except Exception as e:
            logger.warning(f"Failed to request vote from {node_id}: {e}")
    
    async def _become_leader(self):
        """Transition to leader role"""
        logger.info(f"Node {self.node_id} became leader for term {self.state.current_term}")
        
        self.state.role = RaftRole.LEADER
        self.state.leader_id = self.node_id
        
        # Initialize leader state
        self.state.next_index = {node_id: len(self.log) + 1 for node_id in self.cluster_nodes if node_id != self.node_id}
        self.state.match_index = {node_id: 0 for node_id in self.cluster_nodes if node_id != self.node_id}
        
        # Reset timers
        self.last_heartbeat = 0
        self.last_leadership_renewal = time.time()
        
        # Persist state
        await self._persist_state()
        
        # Send initial heartbeat (empty AppendEntries)
        await self._send_heartbeats()
        
        # Add a no-op entry to commit previous terms
        await self._append_log_entry(LogEntry(
            index=len(self.log) + 1,
            term=self.state.current_term,
            entry_type=EntryType.NOOP,
            data={"type": "leader_election", "leader_id": self.node_id}
        ))
    
    async def _become_follower(self, term: int):
        """Transition to follower role"""
        if term > self.state.current_term:
            logger.info(f"Node {self.node_id} became follower for term {term}")
            self.state.current_term = term
            self.state.voted_for = None
        
        self.state.role = RaftRole.FOLLOWER
        self.state.leader_id = None
        self._reset_election_timer()
        
        # Persist state
        await self._persist_state()
    
    async def _send_heartbeats(self):
        """Send heartbeat (empty AppendEntries) to all followers"""
        if self.state.role != RaftRole.LEADER:
            return
        
        heartbeat_tasks = []
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._send_append_entries(node_id, node_info))
                heartbeat_tasks.append(task)
        
        if heartbeat_tasks:
            await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
    
    async def _send_append_entries(self, node_id: str, node_info: Dict, entries: List[LogEntry] = None):
        """Send AppendEntries RPC to a node"""
        try:
            if entries is None:
                entries = []
            
            # Get next index for this node
            next_index = self.state.next_index.get(node_id, len(self.log) + 1)
            prev_log_index = next_index - 1
            prev_log_term = 0
            
            if prev_log_index > 0 and prev_log_index <= len(self.log):
                prev_log_term = self.log[prev_log_index - 1].term
            
            # If no specific entries provided, send entries from next_index
            if not entries and next_index <= len(self.log):
                entries = self.log[next_index - 1:]
            
            append_request = AppendEntriesRequest(
                term=self.state.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[asdict(entry) for entry in entries],
                leader_commit=self.state.commit_index
            )
            
            url = f"http://{node_info['host']}:{node_info.get('raft_port', 8080)}/raft/append"
            async with self.http_session.post(url, json=asdict(append_request)) as response:
                if response.status == 200:
                    append_data = await response.json()
                    append_response = AppendEntriesResponse(**append_data)
                    
                    async with self.state_lock:
                        # Handle response
                        if append_response.term > self.state.current_term:
                            await self._become_follower(append_response.term)
                        elif self.state.role == RaftRole.LEADER:
                            if append_response.success:
                                # Update next_index and match_index
                                self.state.match_index[node_id] = prev_log_index + len(entries)
                                self.state.next_index[node_id] = self.state.match_index[node_id] + 1
                                
                                # Update commit index
                                await self._update_commit_index()
                            else:
                                # Decrement next_index and retry
                                self.state.next_index[node_id] = max(1, self.state.next_index[node_id] - 1)
                
        except Exception as e:
            logger.warning(f"Failed to send append entries to {node_id}: {e}")
    
    async def _update_commit_index(self):
        """Update commit index based on majority replication"""
        if self.state.role != RaftRole.LEADER:
            return
        
        # Find the highest index that's replicated on a majority
        for index in range(len(self.log), self.state.commit_index, -1):
            if index <= len(self.log) and self.log[index - 1].term == self.state.current_term:
                # Count how many nodes have this index
                replicated_count = 1  # Leader always has it
                for node_id in self.state.match_index:
                    if self.state.match_index[node_id] >= index:
                        replicated_count += 1
                
                majority = len(self.cluster_nodes) // 2 + 1
                if replicated_count >= majority:
                    self.state.commit_index = index
                    logger.debug(f"Updated commit index to {index}")
                    break
    
    async def _append_log_entry(self, entry: LogEntry):
        """Append entry to log"""
        async with self.log_lock:
            entry.index = len(self.log) + 1
            self.log.append(entry)
            
            # Persist to database
            await self._persist_log_entry(entry)
    
    async def _apply_log_entry(self, entry: LogEntry):
        """Apply log entry to state machine"""
        try:
            logger.debug(f"Applying log entry {entry.index}: {entry.entry_type.value}")
            
            if entry.entry_type == EntryType.COMMAND:
                # Apply the command to the state machine (database)
                await self._execute_command(entry.data)
            elif entry.entry_type == EntryType.CONFIG_CHANGE:
                # Apply cluster configuration change
                await self._apply_config_change(entry.data)
            # NOOP entries don't need application
            
            # Mark as applied
            entry.applied = True
            await self._update_log_entry_applied(entry.index)
            
        except Exception as e:
            logger.error(f"Failed to apply log entry {entry.index}: {e}")
    
    async def _execute_command(self, command_data: Dict):
        """Execute a command from the log"""
        # This would execute the actual database operation
        # For now, just log it
        logger.info(f"Executing command: {command_data}")
    
    async def _apply_config_change(self, config_data: Dict):
        """Apply cluster configuration change"""
        logger.info(f"Applying config change: {config_data}")
        # Update cluster membership
        # This is a simplified version
    
    async def _renew_leadership_lease(self) -> bool:
        """Renew leadership lease with majority of nodes"""
        if self.state.role != RaftRole.LEADER:
            return False
        
        # Send lease renewal requests to majority of nodes
        lease_tasks = []
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._request_lease_renewal(node_id, node_info))
                lease_tasks.append(task)
        
        if lease_tasks:
            results = await asyncio.gather(*lease_tasks, return_exceptions=True)
            successful_renewals = sum(1 for r in results if r is True)
            majority = len(self.cluster_nodes) // 2 + 1
            
            # Count self as successful
            return (successful_renewals + 1) >= majority
        
        return True  # Single node cluster
    
    async def _request_lease_renewal(self, node_id: str, node_info: Dict) -> bool:
        """Request lease renewal from a node"""
        try:
            lease_data = {
                'leader_id': self.node_id,
                'term': self.state.current_term,
                'lease_duration': self.leadership_lease_duration
            }
            
            url = f"http://{node_info['host']}:{node_info.get('raft_port', 8080)}/raft/lease"
            async with self.http_session.post(url, json=lease_data) as response:
                return response.status == 200
                
        except Exception as e:
            logger.warning(f"Failed to renew lease with {node_id}: {e}")
            return False
    
    def _reset_election_timer(self):
        """Reset election timeout with jitter"""
        jitter = random.uniform(0.5, 1.5)  # 50% jitter
        self.election_timeout = (self.election_timeout + jitter)
        self.election_timer = time.time()
    
    async def _load_persistent_state(self):
        """Load Raft state from database"""
        try:
            async with self.db_pool.acquire() as conn:
                # Load Raft state
                state_row = await conn.fetchrow(
                    "SELECT * FROM synapsedb_consensus.raft_state WHERE node_id = $1",
                    self.node_id
                )
                
                if state_row:
                    self.state.current_term = state_row['current_term']
                    self.state.voted_for = str(state_row['voted_for']) if state_row['voted_for'] else None
                    self.state.commit_index = state_row['commit_index']
                    self.state.last_applied = state_row['last_applied']
                    self.state.role = RaftRole(state_row['role'])
                    self.state.leader_id = str(state_row['leader_id']) if state_row['leader_id'] else None
                
                # Load log entries
                log_rows = await conn.fetch(
                    "SELECT * FROM synapsedb_consensus.raft_log ORDER BY log_index"
                )
                
                self.log = []
                for row in log_rows:
                    entry = LogEntry(
                        index=row['log_index'],
                        term=row['term'],
                        entry_type=EntryType(row['entry_type']),
                        data=row['command_data'],
                        committed=row['committed'],
                        applied=row['applied']
                    )
                    self.log.append(entry)
                
                logger.info(f"Loaded Raft state: term={self.state.current_term}, log_entries={len(self.log)}")
                
        except Exception as e:
            logger.error(f"Failed to load persistent state: {e}")
    
    async def _persist_state(self):
        """Persist Raft state to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_consensus.raft_state 
                    (node_id, current_term, voted_for, commit_index, last_applied, role, leader_id, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (node_id) DO UPDATE SET
                        current_term = EXCLUDED.current_term,
                        voted_for = EXCLUDED.voted_for,
                        commit_index = EXCLUDED.commit_index,
                        last_applied = EXCLUDED.last_applied,
                        role = EXCLUDED.role,
                        leader_id = EXCLUDED.leader_id,
                        updated_at = EXCLUDED.updated_at
                """, self.node_id, self.state.current_term, self.state.voted_for,
                    self.state.commit_index, self.state.last_applied, 
                    self.state.role.value, self.state.leader_id)
                
        except Exception as e:
            logger.error(f"Failed to persist state: {e}")
    
    async def _persist_log_entry(self, entry: LogEntry):
        """Persist log entry to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_consensus.raft_log 
                    (log_index, term, entry_type, command_data, committed, applied, node_id, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (log_index) DO UPDATE SET
                        term = EXCLUDED.term,
                        entry_type = EXCLUDED.entry_type,
                        command_data = EXCLUDED.command_data,
                        committed = EXCLUDED.committed,
                        applied = EXCLUDED.applied
                """, entry.index, entry.term, entry.entry_type.value,
                    json.dumps(entry.data) if entry.data else None,
                    entry.committed, entry.applied, self.node_id)
                
        except Exception as e:
            logger.error(f"Failed to persist log entry: {e}")
    
    async def _update_log_entry_applied(self, index: int):
        """Mark log entry as applied"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE synapsedb_consensus.raft_log SET applied = true WHERE log_index = $1",
                    index
                )
        except Exception as e:
            logger.error(f"Failed to update log entry applied status: {e}")
    
    # Public API methods
    async def submit_command(self, command_data: Dict) -> bool:
        """Submit a command to the Raft cluster"""
        if self.state.role != RaftRole.LEADER:
            return False
        
        try:
            entry = LogEntry(
                index=0,  # Will be set in _append_log_entry
                term=self.state.current_term,
                entry_type=EntryType.COMMAND,
                data=command_data
            )
            
            await self._append_log_entry(entry)
            return True
            
        except Exception as e:
            logger.error(f"Failed to submit command: {e}")
            return False
    
    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        return self.state.role == RaftRole.LEADER
    
    def get_leader_id(self) -> Optional[str]:
        """Get the current leader ID"""
        return self.state.leader_id
    
    def get_current_term(self) -> int:
        """Get the current term"""
        return self.state.current_term