"""
SynapseDB Distributed Deadlock Detection System
Implements global deadlock detection using wait-for graphs across distributed nodes
"""

import asyncio
import asyncpg
import logging
import time
import json
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict
import networkx as nx

logger = logging.getLogger(__name__)

class TransactionState(Enum):
    ACTIVE = "active"
    WAITING = "waiting"
    COMMITTED = "committed"
    ABORTED = "aborted"

class EdgeType(Enum):
    LOCAL_WAIT = "local_wait"
    REMOTE_WAIT = "remote_wait"
    GLOBAL_WAIT = "global_wait"

@dataclass
class WaitForEdge:
    """Represents a wait-for relationship between transactions"""
    waiting_txn: str
    waiting_node: str
    blocking_txn: str
    blocking_node: str
    resource: str
    edge_type: EdgeType
    created_at: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'waiting_txn': self.waiting_txn,
            'waiting_node': self.waiting_node,
            'blocking_txn': self.blocking_txn,
            'blocking_node': self.blocking_node,
            'resource': self.resource,
            'edge_type': self.edge_type.value,
            'created_at': self.created_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WaitForEdge':
        return cls(
            waiting_txn=data['waiting_txn'],
            waiting_node=data['waiting_node'],
            blocking_txn=data['blocking_txn'],
            blocking_node=data['blocking_node'],
            resource=data['resource'],
            edge_type=EdgeType(data['edge_type']),
            created_at=data['created_at']
        )

@dataclass
class DistributedTransaction:
    """Represents a transaction in the distributed system"""
    txn_id: str
    node_id: str
    state: TransactionState
    start_time: float
    last_activity: float
    held_locks: Set[str]
    waiting_for: Optional[str] = None  # Resource being waited for
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'txn_id': self.txn_id,
            'node_id': self.node_id,
            'state': self.state.value,
            'start_time': self.start_time,
            'last_activity': self.last_activity,
            'held_locks': list(self.held_locks),
            'waiting_for': self.waiting_for
        }

class DeadlockVictimSelector:
    """Selects which transaction to abort in case of deadlock"""
    
    def select_victim(self, cycle: List[str], transactions: Dict[str, DistributedTransaction]) -> str:
        """Select victim transaction to break deadlock cycle"""
        if not cycle:
            return None
        
        # Strategy: Select transaction with least work done (youngest transaction)
        youngest_txn = None
        youngest_start_time = float('inf')
        
        for txn_id in cycle:
            if txn_id in transactions:
                txn = transactions[txn_id]
                if txn.start_time < youngest_start_time:
                    youngest_start_time = txn.start_time
                    youngest_txn = txn_id
        
        # Fallback: select first transaction in cycle
        return youngest_txn or cycle[0]

class WaitForGraph:
    """Manages the wait-for graph for deadlock detection"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.edges: Dict[Tuple[str, str], WaitForEdge] = {}
        self.transactions: Dict[str, DistributedTransaction] = {}
    
    def add_transaction(self, transaction: DistributedTransaction):
        """Add a transaction to the graph"""
        self.transactions[transaction.txn_id] = transaction
        if transaction.txn_id not in self.graph:
            self.graph.add_node(transaction.txn_id)
    
    def remove_transaction(self, txn_id: str):
        """Remove a transaction and all its edges"""
        if txn_id in self.transactions:
            del self.transactions[txn_id]
        
        if txn_id in self.graph:
            # Remove all edges involving this transaction
            edges_to_remove = []
            for edge_key in self.edges.keys():
                if edge_key[0] == txn_id or edge_key[1] == txn_id:
                    edges_to_remove.append(edge_key)
            
            for edge_key in edges_to_remove:
                del self.edges[edge_key]
            
            self.graph.remove_node(txn_id)
    
    def add_wait_edge(self, edge: WaitForEdge):
        """Add a wait-for edge to the graph"""
        edge_key = (edge.waiting_txn, edge.blocking_txn)
        self.edges[edge_key] = edge
        
        # Add nodes if they don't exist
        if edge.waiting_txn not in self.graph:
            self.graph.add_node(edge.waiting_txn)
        if edge.blocking_txn not in self.graph:
            self.graph.add_node(edge.blocking_txn)
        
        # Add edge to NetworkX graph
        self.graph.add_edge(edge.waiting_txn, edge.blocking_txn, 
                          weight=1, edge_data=edge)
    
    def remove_wait_edge(self, waiting_txn: str, blocking_txn: str):
        """Remove a wait-for edge"""
        edge_key = (waiting_txn, blocking_txn)
        if edge_key in self.edges:
            del self.edges[edge_key]
        
        if self.graph.has_edge(waiting_txn, blocking_txn):
            self.graph.remove_edge(waiting_txn, blocking_txn)
    
    def detect_cycles(self) -> List[List[str]]:
        """Detect all cycles in the wait-for graph"""
        try:
            cycles = list(nx.simple_cycles(self.graph))
            return cycles
        except nx.NetworkXError:
            logger.error("Error detecting cycles in wait-for graph")
            return []
    
    def get_shortest_cycle(self) -> Optional[List[str]]:
        """Get the shortest cycle in the graph"""
        cycles = self.detect_cycles()
        if not cycles:
            return None
        
        return min(cycles, key=len)
    
    def get_graph_stats(self) -> Dict[str, int]:
        """Get statistics about the wait-for graph"""
        return {
            'nodes': self.graph.number_of_nodes(),
            'edges': self.graph.number_of_edges(),
            'cycles': len(self.detect_cycles()),
            'transactions': len(self.transactions)
        }
    
    def export_graph(self) -> Dict[str, Any]:
        """Export graph for visualization or debugging"""
        return {
            'nodes': list(self.graph.nodes()),
            'edges': [
                {
                    'from': edge_key[0],
                    'to': edge_key[1],
                    'data': edge.to_dict()
                }
                for edge_key, edge in self.edges.items()
            ],
            'transactions': {
                txn_id: txn.to_dict() 
                for txn_id, txn in self.transactions.items()
            }
        }

class DistributedDeadlockDetector:
    """Main deadlock detection coordinator"""
    
    def __init__(self, node_id: str, db_config: Dict, cluster_nodes: List[Dict]):
        self.node_id = node_id
        self.db_config = db_config
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Wait-for graph
        self.wait_graph = WaitForGraph()
        
        # Deadlock detection settings
        self.detection_interval = 10  # seconds
        self.detection_enabled = True
        
        # Victim selection
        self.victim_selector = DeadlockVictimSelector()
        
        # Statistics
        self.deadlocks_detected = 0
        self.transactions_aborted = 0
        
        # Running state
        self.running = False
    
    async def initialize(self):
        """Initialize deadlock detector"""
        logger.info(f"Initializing deadlock detector for node {self.node_id}")
        
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=2, max_size=10)
        
        # Create deadlock detection tables
        await self._create_tables()
        
        logger.info("Deadlock detector initialized")
    
    async def start(self):
        """Start deadlock detection"""
        self.running = True
        logger.info("Starting distributed deadlock detector")
        
        tasks = [
            asyncio.create_task(self._local_detection_loop()),
            asyncio.create_task(self._global_detection_loop()),
            asyncio.create_task(self._cleanup_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Deadlock detector error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop deadlock detection"""
        self.running = False
        logger.info("Stopping deadlock detector")
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def register_transaction(self, txn_id: str, held_locks: Set[str]):
        """Register a new transaction"""
        transaction = DistributedTransaction(
            txn_id=txn_id,
            node_id=self.node_id,
            state=TransactionState.ACTIVE,
            start_time=time.time(),
            last_activity=time.time(),
            held_locks=held_locks
        )
        
        self.wait_graph.add_transaction(transaction)
        await self._persist_transaction_state(transaction)
        
        logger.debug(f"Registered transaction {txn_id} with locks: {held_locks}")
    
    async def register_wait(self, waiting_txn: str, blocking_txn: str, resource: str, 
                           blocking_node: str = None):
        """Register a wait-for relationship"""
        if blocking_node is None:
            blocking_node = self.node_id
        
        edge = WaitForEdge(
            waiting_txn=waiting_txn,
            waiting_node=self.node_id,
            blocking_txn=blocking_txn,
            blocking_node=blocking_node,
            resource=resource,
            edge_type=EdgeType.LOCAL_WAIT if blocking_node == self.node_id else EdgeType.REMOTE_WAIT,
            created_at=time.time()
        )
        
        self.wait_graph.add_wait_edge(edge)
        await self._persist_wait_edge(edge)
        
        # Update transaction state
        if waiting_txn in self.wait_graph.transactions:
            self.wait_graph.transactions[waiting_txn].state = TransactionState.WAITING
            self.wait_graph.transactions[waiting_txn].waiting_for = resource
            await self._persist_transaction_state(self.wait_graph.transactions[waiting_txn])
        
        logger.debug(f"Registered wait: {waiting_txn} -> {blocking_txn} for resource {resource}")
    
    async def unregister_wait(self, waiting_txn: str, blocking_txn: str):
        """Remove a wait-for relationship"""
        self.wait_graph.remove_wait_edge(waiting_txn, blocking_txn)
        await self._remove_wait_edge(waiting_txn, blocking_txn)
        
        # Update transaction state
        if waiting_txn in self.wait_graph.transactions:
            self.wait_graph.transactions[waiting_txn].state = TransactionState.ACTIVE
            self.wait_graph.transactions[waiting_txn].waiting_for = None
            await self._persist_transaction_state(self.wait_graph.transactions[waiting_txn])
        
        logger.debug(f"Removed wait: {waiting_txn} -> {blocking_txn}")
    
    async def unregister_transaction(self, txn_id: str, state: TransactionState):
        """Unregister a transaction"""
        self.wait_graph.remove_transaction(txn_id)
        await self._remove_transaction_state(txn_id, state)
        
        logger.debug(f"Unregistered transaction {txn_id} with state {state.value}")
    
    async def _local_detection_loop(self):
        """Local deadlock detection loop"""
        while self.running:
            try:
                await self._detect_local_deadlocks()
                await asyncio.sleep(self.detection_interval / 2)
            except Exception as e:
                logger.error(f"Error in local detection loop: {e}")
                await asyncio.sleep(5)
    
    async def _global_detection_loop(self):
        """Global deadlock detection loop"""
        while self.running:
            try:
                await self._detect_global_deadlocks()
                await asyncio.sleep(self.detection_interval)
            except Exception as e:
                logger.error(f"Error in global detection loop: {e}")
                await asyncio.sleep(10)
    
    async def _cleanup_loop(self):
        """Cleanup old entries"""
        while self.running:
            try:
                await self._cleanup_old_entries()
                await asyncio.sleep(300)  # Cleanup every 5 minutes
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(60)
    
    async def _detect_local_deadlocks(self):
        """Detect deadlocks within this node"""
        cycles = self.wait_graph.detect_cycles()
        
        for cycle in cycles:
            # Check if it's a purely local cycle
            is_local = all(
                self.wait_graph.transactions.get(txn_id, DistributedTransaction('', self.node_id, TransactionState.ACTIVE, 0, 0, set())).node_id == self.node_id
                for txn_id in cycle
            )
            
            if is_local:
                logger.warning(f"Local deadlock detected: {' -> '.join(cycle)}")
                await self._resolve_deadlock(cycle)
    
    async def _detect_global_deadlocks(self):
        """Detect deadlocks across the distributed system"""
        try:
            # Collect wait-for graphs from all nodes
            global_graph = await self._build_global_wait_graph()
            
            # Detect cycles in the global graph
            cycles = global_graph.detect_cycles()
            
            for cycle in cycles:
                logger.warning(f"Global deadlock detected: {' -> '.join(cycle)}")
                self.deadlocks_detected += 1
                await self._resolve_global_deadlock(cycle, global_graph)
                
        except Exception as e:
            logger.error(f"Error in global deadlock detection: {e}")
    
    async def _build_global_wait_graph(self) -> WaitForGraph:
        """Build global wait-for graph from all nodes"""
        global_graph = WaitForGraph()
        
        # Add local graph
        for txn_id, txn in self.wait_graph.transactions.items():
            global_graph.add_transaction(txn)
        
        for edge in self.wait_graph.edges.values():
            global_graph.add_wait_edge(edge)
        
        # Collect from other nodes
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                try:
                    remote_graph = await self._fetch_remote_wait_graph(node_id, node_info)
                    
                    # Merge remote transactions
                    for txn_id, txn in remote_graph.get('transactions', {}).items():
                        remote_txn = DistributedTransaction(
                            txn_id=txn['txn_id'],
                            node_id=txn['node_id'],
                            state=TransactionState(txn['state']),
                            start_time=txn['start_time'],
                            last_activity=txn['last_activity'],
                            held_locks=set(txn['held_locks']),
                            waiting_for=txn.get('waiting_for')
                        )
                        global_graph.add_transaction(remote_txn)
                    
                    # Merge remote edges
                    for edge_data in remote_graph.get('edges', []):
                        edge = WaitForEdge.from_dict(edge_data['data'])
                        edge.edge_type = EdgeType.GLOBAL_WAIT
                        global_graph.add_wait_edge(edge)
                        
                except Exception as e:
                    logger.warning(f"Failed to fetch wait graph from node {node_id}: {e}")
        
        return global_graph
    
    async def _fetch_remote_wait_graph(self, node_id: str, node_info: Dict) -> Dict:
        """Fetch wait-for graph from remote node"""
        import aiohttp
        
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"http://{node_info['host']}:{node_info.get('deadlock_port', 8082)}/deadlock/graph"
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"HTTP {response.status}")
    
    async def _resolve_deadlock(self, cycle: List[str]):
        """Resolve a local deadlock"""
        victim_txn = self.victim_selector.select_victim(cycle, self.wait_graph.transactions)
        
        if victim_txn:
            logger.info(f"Aborting transaction {victim_txn} to resolve deadlock")
            await self._abort_transaction(victim_txn)
            self.transactions_aborted += 1
    
    async def _resolve_global_deadlock(self, cycle: List[str], global_graph: WaitForGraph):
        """Resolve a global deadlock"""
        victim_txn = self.victim_selector.select_victim(cycle, global_graph.transactions)
        
        if victim_txn:
            # Determine which node owns the victim transaction
            victim_node = None
            if victim_txn in global_graph.transactions:
                victim_node = global_graph.transactions[victim_txn].node_id
            
            if victim_node == self.node_id:
                # We own the victim, abort it locally
                logger.info(f"Aborting local transaction {victim_txn} to resolve global deadlock")
                await self._abort_transaction(victim_txn)
            else:
                # Send abort request to remote node
                logger.info(f"Requesting abort of remote transaction {victim_txn} on node {victim_node}")
                await self._request_remote_abort(victim_txn, victim_node)
            
            self.transactions_aborted += 1
    
    async def _abort_transaction(self, txn_id: str):
        """Abort a local transaction"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%' || $1 || '%'", txn_id)
            
            # Update our local state
            if txn_id in self.wait_graph.transactions:
                self.wait_graph.transactions[txn_id].state = TransactionState.ABORTED
                await self._persist_transaction_state(self.wait_graph.transactions[txn_id])
            
            # Remove from wait graph after a delay to allow cleanup
            await asyncio.sleep(1)
            self.wait_graph.remove_transaction(txn_id)
            
        except Exception as e:
            logger.error(f"Failed to abort transaction {txn_id}: {e}")
    
    async def _request_remote_abort(self, txn_id: str, node_id: str):
        """Request remote node to abort a transaction"""
        try:
            node_info = self.cluster_nodes[node_id]
            
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{node_info['host']}:{node_info.get('deadlock_port', 8082)}/deadlock/abort"
                payload = {'txn_id': txn_id, 'reason': 'deadlock_victim'}
                
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        logger.error(f"Failed to request abort from node {node_id}: HTTP {response.status}")
                        
        except Exception as e:
            logger.error(f"Error requesting remote abort: {e}")
    
    async def _create_tables(self):
        """Create deadlock detection tables"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS synapsedb_deadlock;
                
                CREATE TABLE IF NOT EXISTS synapsedb_deadlock.transactions (
                    txn_id VARCHAR(64) PRIMARY KEY,
                    node_id VARCHAR(64) NOT NULL,
                    state VARCHAR(32) NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    last_activity TIMESTAMP NOT NULL,
                    held_locks TEXT[],
                    waiting_for VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE TABLE IF NOT EXISTS synapsedb_deadlock.wait_edges (
                    waiting_txn VARCHAR(64) NOT NULL,
                    blocking_txn VARCHAR(64) NOT NULL,
                    waiting_node VARCHAR(64) NOT NULL,
                    blocking_node VARCHAR(64) NOT NULL,
                    resource VARCHAR(255) NOT NULL,
                    edge_type VARCHAR(32) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (waiting_txn, blocking_txn)
                );
                
                CREATE INDEX IF NOT EXISTS idx_transactions_node_state 
                ON synapsedb_deadlock.transactions (node_id, state);
                
                CREATE INDEX IF NOT EXISTS idx_wait_edges_nodes 
                ON synapsedb_deadlock.wait_edges (waiting_node, blocking_node);
            """)
    
    async def _persist_transaction_state(self, transaction: DistributedTransaction):
        """Persist transaction state to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_deadlock.transactions
                    (txn_id, node_id, state, start_time, last_activity, held_locks, waiting_for, updated_at)
                    VALUES ($1, $2, $3, to_timestamp($4), to_timestamp($5), $6, $7, NOW())
                    ON CONFLICT (txn_id) DO UPDATE SET
                        state = EXCLUDED.state,
                        last_activity = EXCLUDED.last_activity,
                        held_locks = EXCLUDED.held_locks,
                        waiting_for = EXCLUDED.waiting_for,
                        updated_at = NOW()
                """, transaction.txn_id, transaction.node_id, transaction.state.value,
                    transaction.start_time, transaction.last_activity, 
                    list(transaction.held_locks), transaction.waiting_for)
        except Exception as e:
            logger.error(f"Failed to persist transaction state {transaction.txn_id}: {e}")
    
    async def _persist_wait_edge(self, edge: WaitForEdge):
        """Persist wait edge to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_deadlock.wait_edges
                    (waiting_txn, blocking_txn, waiting_node, blocking_node, resource, edge_type)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (waiting_txn, blocking_txn) DO UPDATE SET
                        waiting_node = EXCLUDED.waiting_node,
                        blocking_node = EXCLUDED.blocking_node,
                        resource = EXCLUDED.resource,
                        edge_type = EXCLUDED.edge_type
                """, edge.waiting_txn, edge.blocking_txn, edge.waiting_node,
                    edge.blocking_node, edge.resource, edge.edge_type.value)
        except Exception as e:
            logger.error(f"Failed to persist wait edge: {e}")
    
    async def _remove_wait_edge(self, waiting_txn: str, blocking_txn: str):
        """Remove wait edge from database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM synapsedb_deadlock.wait_edges
                    WHERE waiting_txn = $1 AND blocking_txn = $2
                """, waiting_txn, blocking_txn)
        except Exception as e:
            logger.error(f"Failed to remove wait edge: {e}")
    
    async def _remove_transaction_state(self, txn_id: str, final_state: TransactionState):
        """Remove transaction from database"""
        try:
            async with self.db_pool.acquire() as conn:
                # Update final state before removal
                await conn.execute("""
                    UPDATE synapsedb_deadlock.transactions
                    SET state = $2, updated_at = NOW()
                    WHERE txn_id = $1
                """, txn_id, final_state.value)
                
                # Remove wait edges
                await conn.execute("""
                    DELETE FROM synapsedb_deadlock.wait_edges
                    WHERE waiting_txn = $1 OR blocking_txn = $1
                """, txn_id)
        except Exception as e:
            logger.error(f"Failed to remove transaction state {txn_id}: {e}")
    
    async def _cleanup_old_entries(self):
        """Clean up old database entries"""
        try:
            cutoff_time = time.time() - 3600  # 1 hour old
            
            async with self.db_pool.acquire() as conn:
                # Clean up completed transactions
                await conn.execute("""
                    DELETE FROM synapsedb_deadlock.transactions
                    WHERE state IN ('committed', 'aborted') 
                    AND updated_at < to_timestamp($1)
                """, cutoff_time)
                
                # Clean up orphaned wait edges
                await conn.execute("""
                    DELETE FROM synapsedb_deadlock.wait_edges
                    WHERE created_at < to_timestamp($1)
                    AND (waiting_txn NOT IN (SELECT txn_id FROM synapsedb_deadlock.transactions)
                         OR blocking_txn NOT IN (SELECT txn_id FROM synapsedb_deadlock.transactions))
                """, cutoff_time)
        except Exception as e:
            logger.error(f"Failed to cleanup old entries: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get deadlock detection statistics"""
        graph_stats = self.wait_graph.get_graph_stats()
        
        return {
            'node_id': self.node_id,
            'deadlocks_detected': self.deadlocks_detected,
            'transactions_aborted': self.transactions_aborted,
            'detection_enabled': self.detection_enabled,
            'wait_graph': graph_stats,
            'active_transactions': len([
                txn for txn in self.wait_graph.transactions.values()
                if txn.state == TransactionState.ACTIVE
            ]),
            'waiting_transactions': len([
                txn for txn in self.wait_graph.transactions.values()
                if txn.state == TransactionState.WAITING
            ])
        }
    
    async def export_wait_graph(self) -> Dict[str, Any]:
        """Export current wait-for graph"""
        return self.wait_graph.export_graph()