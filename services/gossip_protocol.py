"""
SynapseDB Gossip Protocol
Implementation of SWIM (Scalable Weakly-consistent Infection-style process group Membership) protocol
for failure detection and cluster membership management
"""

import asyncio
import asyncpg
import logging
import random
import time
import json
import hashlib
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import aiohttp
import socket

logger = logging.getLogger(__name__)

class NodeState(Enum):
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    LEFT = "left"

class GossipMessageType(Enum):
    PING = "ping"
    PONG = "pong"  
    PING_REQ = "ping_req"
    SUSPECT = "suspect"
    ALIVE = "alive"
    DEAD = "dead"
    JOIN = "join"
    LEAVE = "leave"

@dataclass
class NodeInfo:
    node_id: str
    name: str
    host: str
    port: int
    gossip_port: int
    state: NodeState
    incarnation: int  # Lamport timestamp for conflict resolution
    last_seen: float
    suspect_timeout: float = 0
    
class GossipMember:
    """Represents a member in the gossip protocol"""
    def __init__(self, node_info: NodeInfo):
        self.node_info = node_info
        self.last_ping_time = 0
        self.ping_count = 0
        self.indirect_pings: Set[str] = set()  # Nodes that have been asked to ping this member
        
@dataclass
class GossipMessage:
    msg_type: GossipMessageType
    sender_id: str
    target_id: str
    incarnation: int
    payload: Optional[Dict] = None
    members: Optional[List[Dict]] = None  # Member state updates

class GossipProtocol:
    """SWIM-based gossip protocol for failure detection and membership"""
    
    def __init__(self, node_id: str, node_name: str, host: str, port: int, gossip_port: int,
                 db_config: Dict, initial_members: List[Dict] = None):
        self.node_id = node_id
        self.node_name = node_name
        self.host = host
        self.port = port
        self.gossip_port = gossip_port
        self.db_config = db_config
        
        # Local node info
        self.local_node = NodeInfo(
            node_id=node_id,
            name=node_name,
            host=host,
            port=port,
            gossip_port=gossip_port,
            state=NodeState.ALIVE,
            incarnation=int(time.time() * 1000),  # Millisecond precision
            last_seen=time.time()
        )
        
        # Cluster members
        self.members: Dict[str, GossipMember] = {}
        self.member_list_version = 0
        
        # Protocol parameters (tunable)
        self.protocol_period = 1.0  # seconds between gossip rounds
        self.failure_timeout = 5.0  # seconds to suspect failure
        self.suspect_timeout = 3.0  # seconds before marking suspect as dead
        self.gossip_fanout = 3  # number of nodes to gossip with per round
        self.indirect_ping_count = 3  # number of nodes to ask for indirect ping
        
        # Network partition detection
        self.partition_threshold = 0.5  # If we can't reach >50% of nodes, suspect partition
        self.last_partition_check = 0
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Running state
        self.running = False
        self.server_task: Optional[asyncio.Task] = None
        
        # HTTP session for gossip
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize members from provided list
        if initial_members:
            for member_info in initial_members:
                if member_info['id'] != self.node_id:
                    node_info = NodeInfo(**member_info, state=NodeState.ALIVE, last_seen=time.time())
                    self.members[member_info['id']] = GossipMember(node_info)
    
    async def initialize(self):
        """Initialize gossip protocol"""
        logger.info(f"Initializing gossip protocol for node {self.node_id}")
        
        # Connect to database
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=2, max_size=5)
        
        # Create HTTP session
        connector = aiohttp.TCPConnector(limit=50)
        timeout = aiohttp.ClientTimeout(total=2, connect=1)
        self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        # Load existing members from database
        await self._load_members_from_db()
        
        logger.info(f"Gossip protocol initialized with {len(self.members)} members")
    
    async def start(self):
        """Start gossip protocol"""
        self.running = True
        logger.info(f"Starting gossip protocol on {self.host}:{self.gossip_port}")
        
        # Start HTTP server for receiving gossip messages
        self.server_task = asyncio.create_task(self._start_gossip_server())
        
        # Start gossip loops
        tasks = [
            self.server_task,
            asyncio.create_task(self._gossip_loop()),
            asyncio.create_task(self._failure_detection_loop()),
            asyncio.create_task(self._partition_detection_loop()),
            asyncio.create_task(self._member_sync_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Gossip protocol error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop gossip protocol"""
        self.running = False
        logger.info("Stopping gossip protocol")
        
        # Send leave message
        await self._broadcast_leave()
        
        if self.server_task:
            self.server_task.cancel()
        
        if self.http_session:
            await self.http_session.close()
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def _start_gossip_server(self):
        """Start HTTP server for gossip messages"""
        app = aiohttp.web.Application()
        app.router.add_post('/gossip', self._handle_gossip_message)
        
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        
        site = aiohttp.web.TCPSite(runner, self.host, self.gossip_port)
        await site.start()
        
        logger.info(f"Gossip server started on {self.host}:{self.gossip_port}")
        
        # Keep server running
        try:
            while self.running:
                await asyncio.sleep(1)
        finally:
            await runner.cleanup()
    
    async def _handle_gossip_message(self, request):
        """Handle incoming gossip message"""
        try:
            data = await request.json()
            message = GossipMessage(**data)
            
            # Update member list based on gossip
            if message.members:
                await self._merge_member_list(message.members)
            
            # Handle specific message types
            if message.msg_type == GossipMessageType.PING:
                await self._handle_ping(message)
            elif message.msg_type == GossipMessageType.PING_REQ:
                await self._handle_ping_req(message)
            elif message.msg_type == GossipMessageType.SUSPECT:
                await self._handle_suspect(message)
            elif message.msg_type == GossipMessageType.ALIVE:
                await self._handle_alive(message)
            elif message.msg_type == GossipMessageType.DEAD:
                await self._handle_dead(message)
            elif message.msg_type == GossipMessageType.JOIN:
                await self._handle_join(message)
            elif message.msg_type == GossipMessageType.LEAVE:
                await self._handle_leave(message)
            
            # Send response based on message type
            if message.msg_type == GossipMessageType.PING:
                response_msg = GossipMessage(
                    msg_type=GossipMessageType.PONG,
                    sender_id=self.node_id,
                    target_id=message.sender_id,
                    incarnation=self.local_node.incarnation,
                    members=self._get_member_updates()
                )
                return aiohttp.web.json_response(asdict(response_msg))
            
            return aiohttp.web.json_response({'status': 'ok'})
            
        except Exception as e:
            logger.error(f"Error handling gossip message: {e}")
            return aiohttp.web.json_response({'error': str(e)}, status=500)
    
    async def _gossip_loop(self):
        """Main gossip loop - sends periodic gossip messages"""
        while self.running:
            try:
                # Select random nodes to gossip with
                gossip_targets = self._select_gossip_targets()
                
                # Send gossip to selected targets
                for target_id in gossip_targets:
                    if target_id in self.members:
                        member = self.members[target_id]
                        if member.node_info.state in [NodeState.ALIVE, NodeState.SUSPECT]:
                            await self._send_ping(member.node_info)
                
                await asyncio.sleep(self.protocol_period)
                
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")
                await asyncio.sleep(1)
    
    async def _failure_detection_loop(self):
        """Detect failed nodes and update their status"""
        while self.running:
            try:
                current_time = time.time()
                
                for member_id, member in list(self.members.items()):
                    node_info = member.node_info
                    
                    if node_info.state == NodeState.ALIVE:
                        # Check if node should be suspected
                        if current_time - node_info.last_seen > self.failure_timeout:
                            logger.warning(f"Suspecting node {node_info.name} - no response for {current_time - node_info.last_seen:.1f}s")
                            await self._mark_suspect(member_id, current_time + self.suspect_timeout)
                    
                    elif node_info.state == NodeState.SUSPECT:
                        # Check if suspect timeout has passed
                        if current_time > node_info.suspect_timeout:
                            logger.warning(f"Marking node {node_info.name} as dead - suspect timeout exceeded")
                            await self._mark_dead(member_id)
                    
                    elif node_info.state == NodeState.DEAD:
                        # Clean up dead nodes after some time
                        if current_time - node_info.last_seen > 60:  # 60 seconds
                            logger.info(f"Removing dead node {node_info.name} from member list")
                            del self.members[member_id]
                            await self._update_member_in_db(node_info)
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in failure detection loop: {e}")
                await asyncio.sleep(5)
    
    async def _partition_detection_loop(self):
        """Detect network partitions"""
        while self.running:
            try:
                current_time = time.time()
                
                # Check every 30 seconds
                if current_time - self.last_partition_check > 30:
                    await self._check_network_partition()
                    self.last_partition_check = current_time
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in partition detection loop: {e}")
                await asyncio.sleep(30)
    
    async def _member_sync_loop(self):
        """Sync member list with database"""
        while self.running:
            try:
                await self._sync_members_to_db()
                await asyncio.sleep(30)  # Sync every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in member sync loop: {e}")
                await asyncio.sleep(10)
    
    def _select_gossip_targets(self) -> List[str]:
        """Select random nodes to gossip with"""
        alive_members = [
            mid for mid, member in self.members.items()
            if member.node_info.state in [NodeState.ALIVE, NodeState.SUSPECT]
        ]
        
        # Select random subset
        count = min(self.gossip_fanout, len(alive_members))
        return random.sample(alive_members, count) if alive_members else []
    
    async def _send_ping(self, target_node: NodeInfo):
        """Send ping to a target node"""
        try:
            message = GossipMessage(
                msg_type=GossipMessageType.PING,
                sender_id=self.node_id,
                target_id=target_node.node_id,
                incarnation=self.local_node.incarnation,
                members=self._get_member_updates()
            )
            
            url = f"http://{target_node.host}:{target_node.gossip_port}/gossip"
            async with self.http_session.post(url, json=asdict(message)) as response:
                if response.status == 200:
                    # Update last seen time
                    if target_node.node_id in self.members:
                        self.members[target_node.node_id].node_info.last_seen = time.time()
                        
                        # If node was suspected, mark as alive
                        if self.members[target_node.node_id].node_info.state == NodeState.SUSPECT:
                            await self._mark_alive(target_node.node_id)
                    
                    # Process response
                    response_data = await response.json()
                    if 'members' in response_data:
                        await self._merge_member_list(response_data['members'])
                else:
                    logger.debug(f"Ping to {target_node.name} failed with status {response.status}")
                    
        except Exception as e:
            logger.debug(f"Failed to ping {target_node.name}: {e}")
    
    async def _send_indirect_ping(self, suspect_node_id: str):
        """Send indirect ping requests for a suspected node"""
        if suspect_node_id not in self.members:
            return
        
        suspect_member = self.members[suspect_node_id]
        
        # Select random nodes to perform indirect ping
        candidates = [
            mid for mid, member in self.members.items()
            if mid != suspect_node_id and mid not in suspect_member.indirect_pings
            and member.node_info.state == NodeState.ALIVE
        ]
        
        indirect_targets = random.sample(candidates, min(self.indirect_ping_count, len(candidates)))
        
        for target_id in indirect_targets:
            try:
                target_node = self.members[target_id].node_info
                
                message = GossipMessage(
                    msg_type=GossipMessageType.PING_REQ,
                    sender_id=self.node_id,
                    target_id=suspect_node_id,
                    incarnation=self.local_node.incarnation,
                    payload={'indirect_target': target_node.node_id}
                )
                
                url = f"http://{target_node.host}:{target_node.gossip_port}/gossip"
                async with self.http_session.post(url, json=asdict(message)) as response:
                    if response.status == 200:
                        suspect_member.indirect_pings.add(target_id)
                        
            except Exception as e:
                logger.debug(f"Failed to send indirect ping request to {target_id}: {e}")
    
    async def _mark_suspect(self, node_id: str, suspect_timeout: float):
        """Mark a node as suspected"""
        if node_id in self.members:
            member = self.members[node_id]
            member.node_info.state = NodeState.SUSPECT
            member.node_info.suspect_timeout = suspect_timeout
            member.node_info.incarnation += 1  # Increment incarnation
            
            # Send indirect pings
            await self._send_indirect_ping(node_id)
            
            # Broadcast suspect message
            await self._broadcast_suspect(node_id)
            
            # Update database
            await self._update_member_in_db(member.node_info)
    
    async def _mark_alive(self, node_id: str):
        """Mark a node as alive"""
        if node_id in self.members:
            member = self.members[node_id]
            member.node_info.state = NodeState.ALIVE
            member.node_info.last_seen = time.time()
            member.node_info.incarnation += 1
            member.indirect_pings.clear()
            
            # Broadcast alive message
            await self._broadcast_alive(node_id)
            
            # Update database
            await self._update_member_in_db(member.node_info)
            
            logger.info(f"Node {member.node_info.name} is alive again")
    
    async def _mark_dead(self, node_id: str):
        """Mark a node as dead"""
        if node_id in self.members:
            member = self.members[node_id]
            member.node_info.state = NodeState.DEAD
            member.node_info.incarnation += 1
            
            # Broadcast dead message
            await self._broadcast_dead(node_id)
            
            # Update database
            await self._update_member_in_db(member.node_info)
            
            logger.error(f"Node {member.node_info.name} marked as dead")
    
    async def _broadcast_suspect(self, suspect_node_id: str):
        """Broadcast suspect message"""
        await self._broadcast_message(GossipMessageType.SUSPECT, suspect_node_id)
    
    async def _broadcast_alive(self, alive_node_id: str):
        """Broadcast alive message"""
        await self._broadcast_message(GossipMessageType.ALIVE, alive_node_id)
    
    async def _broadcast_dead(self, dead_node_id: str):
        """Broadcast dead message"""
        await self._broadcast_message(GossipMessageType.DEAD, dead_node_id)
    
    async def _broadcast_leave(self):
        """Broadcast leave message"""
        await self._broadcast_message(GossipMessageType.LEAVE, self.node_id)
    
    async def _broadcast_message(self, msg_type: GossipMessageType, target_node_id: str):
        """Broadcast a message to all alive members"""
        message = GossipMessage(
            msg_type=msg_type,
            sender_id=self.node_id,
            target_id=target_node_id,
            incarnation=self.local_node.incarnation
        )
        
        alive_members = [
            member for member in self.members.values()
            if member.node_info.state == NodeState.ALIVE
        ]
        
        # Send to random subset to avoid message explosion
        targets = random.sample(alive_members, min(self.gossip_fanout, len(alive_members)))
        
        for member in targets:
            try:
                url = f"http://{member.node_info.host}:{member.node_info.gossip_port}/gossip"
                async with self.http_session.post(url, json=asdict(message)) as response:
                    pass  # Fire and forget
            except Exception as e:
                logger.debug(f"Failed to broadcast to {member.node_info.name}: {e}")
    
    def _get_member_updates(self) -> List[Dict]:
        """Get recent member updates to gossip"""
        # Return a subset of member info (piggyback on messages)
        members_to_share = random.sample(
            list(self.members.values()), 
            min(5, len(self.members))  # Share up to 5 member updates
        )
        
        return [asdict(member.node_info) for member in members_to_share]
    
    async def _merge_member_list(self, member_updates: List[Dict]):
        """Merge received member updates with local state"""
        for member_data in member_updates:
            node_id = member_data['node_id']
            
            if node_id == self.node_id:
                continue  # Skip self
            
            if node_id in self.members:
                existing_member = self.members[node_id]
                
                # Use incarnation number to resolve conflicts
                if member_data['incarnation'] > existing_member.node_info.incarnation:
                    # Update with newer information
                    existing_member.node_info.incarnation = member_data['incarnation']
                    existing_member.node_info.state = NodeState(member_data['state'])
                    
                    if member_data['state'] == NodeState.ALIVE.value:
                        existing_member.node_info.last_seen = time.time()
                        
                    await self._update_member_in_db(existing_member.node_info)
            else:
                # New member
                node_info = NodeInfo(**member_data)
                self.members[node_id] = GossipMember(node_info)
                await self._update_member_in_db(node_info)
                
                logger.info(f"Discovered new member: {node_info.name}")
    
    async def _handle_ping(self, message: GossipMessage):
        """Handle incoming ping message"""
        # Ping is handled by returning pong in the HTTP response
        # Update sender's last seen time
        if message.sender_id in self.members:
            self.members[message.sender_id].node_info.last_seen = time.time()
    
    async def _handle_ping_req(self, message: GossipMessage):
        """Handle indirect ping request"""
        target_id = message.target_id
        if target_id in self.members:
            target_node = self.members[target_id].node_info
            
            # Try to ping the target
            try:
                ping_msg = GossipMessage(
                    msg_type=GossipMessageType.PING,
                    sender_id=self.node_id,
                    target_id=target_id,
                    incarnation=self.local_node.incarnation
                )
                
                url = f"http://{target_node.host}:{target_node.gossip_port}/gossip"
                async with self.http_session.post(url, json=asdict(ping_msg)) as response:
                    if response.status == 200:
                        # Target responded, it's alive
                        await self._mark_alive(target_id)
                        
            except Exception:
                # Target didn't respond to indirect ping
                pass
    
    async def _handle_suspect(self, message: GossipMessage):
        """Handle suspect message"""
        suspect_id = message.target_id
        
        if suspect_id == self.node_id:
            # We're being suspected, increase incarnation and broadcast alive
            self.local_node.incarnation += 1
            await self._broadcast_alive(self.node_id)
        elif suspect_id in self.members:
            member = self.members[suspect_id]
            if message.incarnation >= member.node_info.incarnation:
                await self._mark_suspect(suspect_id, time.time() + self.suspect_timeout)
    
    async def _handle_alive(self, message: GossipMessage):
        """Handle alive message"""
        alive_id = message.target_id
        if alive_id in self.members:
            member = self.members[alive_id]
            if message.incarnation >= member.node_info.incarnation:
                await self._mark_alive(alive_id)
    
    async def _handle_dead(self, message: GossipMessage):
        """Handle dead message"""
        dead_id = message.target_id
        if dead_id in self.members:
            member = self.members[dead_id]
            if message.incarnation >= member.node_info.incarnation:
                await self._mark_dead(dead_id)
    
    async def _handle_join(self, message: GossipMessage):
        """Handle join message"""
        # Add new member to cluster
        if message.payload and 'node_info' in message.payload:
            node_info = NodeInfo(**message.payload['node_info'])
            self.members[node_info.node_id] = GossipMember(node_info)
            await self._update_member_in_db(node_info)
            logger.info(f"Node {node_info.name} joined the cluster")
    
    async def _handle_leave(self, message: GossipMessage):
        """Handle leave message"""
        leaving_id = message.target_id
        if leaving_id in self.members:
            member = self.members[leaving_id]
            member.node_info.state = NodeState.LEFT
            await self._update_member_in_db(member.node_info)
            logger.info(f"Node {member.node_info.name} left the cluster")
    
    async def _check_network_partition(self):
        """Check for potential network partition"""
        try:
            total_members = len(self.members)
            if total_members == 0:
                return
            
            alive_members = sum(1 for m in self.members.values() if m.node_info.state == NodeState.ALIVE)
            alive_ratio = alive_members / total_members
            
            if alive_ratio < self.partition_threshold:
                logger.warning(f"Potential network partition detected: {alive_members}/{total_members} "
                             f"({alive_ratio:.1%}) nodes reachable")
                
                # Store partition event in database
                await self._record_partition_event(alive_members, total_members)
            else:
                logger.debug(f"Cluster health check: {alive_members}/{total_members} ({alive_ratio:.1%}) nodes alive")
                
        except Exception as e:
            logger.error(f"Error checking network partition: {e}")
    
    async def _load_members_from_db(self):
        """Load member list from database"""
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT node_id, node_id as node_name, host, port, status,
                           last_heartbeat, 0 as raft_term
                    FROM synapsedb_consensus.cluster_nodes
                    WHERE node_id != $1
                """, self.node_id)
                
                for row in rows:
                    node_info = NodeInfo(
                        node_id=str(row['node_id']),
                        name=row['node_name'],
                        host=row['host'],
                        port=row['port'],
                        gossip_port=row['port'] + 1000,  # Assume gossip port = db port + 1000
                        state=NodeState.ALIVE if row['status'] == 'active' else NodeState.SUSPECT,
                        incarnation=row.get('raft_term', 0),
                        last_seen=row['last_heartbeat'].timestamp() if row['last_heartbeat'] else time.time()
                    )
                    
                    self.members[node_info.node_id] = GossipMember(node_info)
                
        except Exception as e:
            logger.error(f"Failed to load members from database: {e}")
    
    async def _sync_members_to_db(self):
        """Sync member list to database"""
        try:
            async with self.db_pool.acquire() as conn:
                for member in self.members.values():
                    node_info = member.node_info
                    status = 'active' if node_info.state == NodeState.ALIVE else 'inactive'
                    
                    await conn.execute("""
                        INSERT INTO synapsedb_replication.node_status
                        (node_id, node_name, host_address, port, status, last_heartbeat, updated_at)
                        VALUES ($1, $2, $3, $4, $5, to_timestamp($6), NOW())
                        ON CONFLICT (node_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            last_heartbeat = EXCLUDED.last_heartbeat,
                            updated_at = NOW()
                    """, node_info.node_id, node_info.name, node_info.host, node_info.port,
                        status, node_info.last_seen)
                        
        except Exception as e:
            logger.error(f"Failed to sync members to database: {e}")
    
    async def _update_member_in_db(self, node_info: NodeInfo):
        """Update a single member in database"""
        try:
            async with self.db_pool.acquire() as conn:
                status = 'active' if node_info.state == NodeState.ALIVE else 'inactive'
                
                await conn.execute("""
                    INSERT INTO synapsedb_replication.node_status
                    (node_id, node_name, host_address, port, status, last_heartbeat, updated_at)
                    VALUES ($1, $2, $3, $4, $5, to_timestamp($6), NOW())
                    ON CONFLICT (node_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        last_heartbeat = EXCLUDED.last_heartbeat,
                        updated_at = NOW()
                """, node_info.node_id, node_info.name, node_info.host, node_info.port,
                    status, node_info.last_seen)
                    
        except Exception as e:
            logger.error(f"Failed to update member {node_info.name} in database: {e}")
    
    async def _record_partition_event(self, alive_count: int, total_count: int):
        """Record network partition event"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_replication.partition_events
                    (detected_by_node, alive_nodes, total_nodes, detected_at)
                    VALUES ($1, $2, $3, NOW())
                """, self.node_id, alive_count, total_count)
                
        except Exception as e:
            logger.error(f"Failed to record partition event: {e}")
    
    async def join_cluster(self, seed_nodes: List[Dict]):
        """Join an existing cluster using seed nodes"""
        for seed in seed_nodes:
            try:
                join_message = GossipMessage(
                    msg_type=GossipMessageType.JOIN,
                    sender_id=self.node_id,
                    target_id=seed['id'],
                    incarnation=self.local_node.incarnation,
                    payload={'node_info': asdict(self.local_node)}
                )
                
                url = f"http://{seed['host']}:{seed['gossip_port']}/gossip"
                async with self.http_session.post(url, json=asdict(join_message)) as response:
                    if response.status == 200:
                        logger.info(f"Successfully joined cluster via {seed['name']}")
                        return True
                        
            except Exception as e:
                logger.warning(f"Failed to join via {seed.get('name', 'unknown')}: {e}")
        
        logger.error("Failed to join cluster - no seed nodes responded")
        return False
    
    def get_cluster_members(self) -> List[Dict]:
        """Get current cluster members"""
        members_info = []
        
        # Include self
        members_info.append({
            'node_id': self.local_node.node_id,
            'name': self.local_node.name,
            'host': self.local_node.host,
            'port': self.local_node.port,
            'state': self.local_node.state.value,
            'incarnation': self.local_node.incarnation,
            'is_self': True
        })
        
        # Include other members
        for member in self.members.values():
            members_info.append({
                'node_id': member.node_info.node_id,
                'name': member.node_info.name,
                'host': member.node_info.host,
                'port': member.node_info.port,
                'state': member.node_info.state.value,
                'incarnation': member.node_info.incarnation,
                'last_seen': member.node_info.last_seen,
                'is_self': False
            })
        
        return sorted(members_info, key=lambda x: x['name'])
    
    def is_majority_reachable(self) -> bool:
        """Check if majority of cluster is reachable"""
        total_members = len(self.members) + 1  # +1 for self
        alive_members = 1 + sum(1 for m in self.members.values() if m.node_info.state == NodeState.ALIVE)
        
        return alive_members > total_members / 2