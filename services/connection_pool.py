"""
SynapseDB Cluster-Aware Connection Pool
Provides intelligent connection routing with load balancing and failover
"""

import asyncio
import asyncpg
import logging
import time
import random
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import hashlib

logger = logging.getLogger(__name__)

class NodeRole(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    UNAVAILABLE = "unavailable"

class QueryType(Enum):
    READ = "read"
    WRITE = "write"
    DDL = "ddl"
    ADMIN = "admin"

class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_RANDOM = "weighted_random"
    CONSISTENT_HASH = "consistent_hash"

@dataclass
class NodeHealth:
    """Health metrics for a database node"""
    node_id: str
    host: str
    port: int
    role: NodeRole
    is_available: bool = True
    last_check: float = 0
    response_time: float = 0
    active_connections: int = 0
    max_connections: int = 100
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    replication_lag: float = 0.0
    error_count: int = 0
    last_error: Optional[str] = None
    weight: float = 1.0  # For weighted load balancing
    
    @property
    def connection_utilization(self) -> float:
        """Get connection utilization percentage"""
        if self.max_connections == 0:
            return 0.0
        return (self.active_connections / self.max_connections) * 100
    
    @property
    def health_score(self) -> float:
        """Calculate overall health score (0.0 to 1.0)"""
        if not self.is_available:
            return 0.0
        
        # Factor in response time, connection utilization, and system resources
        response_score = max(0, 1.0 - (self.response_time / 1000))  # Cap at 1 second
        connection_score = max(0, 1.0 - (self.connection_utilization / 100))
        cpu_score = max(0, 1.0 - (self.cpu_usage / 100))
        memory_score = max(0, 1.0 - (self.memory_usage / 100))
        replication_score = max(0, 1.0 - (self.replication_lag / 10))  # Cap at 10 seconds
        error_score = max(0, 1.0 - (self.error_count / 10))  # Cap at 10 errors
        
        return (response_score + connection_score + cpu_score + 
                memory_score + replication_score + error_score) / 6

@dataclass
class ConnectionConfig:
    """Configuration for database connections"""
    host: str
    port: int = 5432
    database: str = "synapsedb"
    user: str = "synapsedb"
    password: str = "synapsedb"
    ssl: str = "prefer"
    min_size: int = 5
    max_size: int = 20
    command_timeout: float = 60.0
    server_settings: Dict[str, str] = field(default_factory=dict)

class QueryRouter:
    """Routes queries to appropriate nodes based on type and load"""
    
    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.LEAST_CONNECTIONS):
        self.strategy = strategy
        self.round_robin_index = 0
    
    def select_node(self, nodes: List[NodeHealth], query_type: QueryType, 
                   query_hash: Optional[str] = None) -> Optional[NodeHealth]:
        """Select the best node for a query"""
        if not nodes:
            return None
        
        # Filter available nodes based on query type
        available_nodes = self._filter_nodes_by_query_type(nodes, query_type)
        if not available_nodes:
            return None
        
        # Apply load balancing strategy
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_select(available_nodes)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            return self._random_select(available_nodes)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_select(available_nodes)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED_RANDOM:
            return self._weighted_random_select(available_nodes)
        elif self.strategy == LoadBalancingStrategy.CONSISTENT_HASH:
            return self._consistent_hash_select(available_nodes, query_hash)
        else:
            return available_nodes[0]
    
    def _filter_nodes_by_query_type(self, nodes: List[NodeHealth], 
                                   query_type: QueryType) -> List[NodeHealth]:
        """Filter nodes based on query type requirements"""
        available = [n for n in nodes if n.is_available]
        
        if query_type in [QueryType.WRITE, QueryType.DDL]:
            # Write operations go to leader only
            leaders = [n for n in available if n.role == NodeRole.LEADER]
            return leaders if leaders else []
        elif query_type == QueryType.READ:
            # Read operations can go to any available node, prefer followers for load distribution
            followers = [n for n in available if n.role == NodeRole.FOLLOWER]
            leaders = [n for n in available if n.role == NodeRole.LEADER]
            # Prefer followers, but fall back to leaders if no followers available
            return followers if followers else leaders
        else:  # ADMIN queries
            # Admin queries go to leader
            leaders = [n for n in available if n.role == NodeRole.LEADER]
            return leaders if leaders else available
    
    def _round_robin_select(self, nodes: List[NodeHealth]) -> NodeHealth:
        """Round-robin selection"""
        if not nodes:
            return None
        node = nodes[self.round_robin_index % len(nodes)]
        self.round_robin_index += 1
        return node
    
    def _random_select(self, nodes: List[NodeHealth]) -> NodeHealth:
        """Random selection"""
        return random.choice(nodes)
    
    def _least_connections_select(self, nodes: List[NodeHealth]) -> NodeHealth:
        """Select node with least active connections"""
        return min(nodes, key=lambda n: n.active_connections)
    
    def _weighted_random_select(self, nodes: List[NodeHealth]) -> NodeHealth:
        """Weighted random selection based on health scores"""
        weights = [n.health_score * n.weight for n in nodes]
        total_weight = sum(weights)
        
        if total_weight == 0:
            return self._random_select(nodes)
        
        r = random.uniform(0, total_weight)
        cumulative = 0
        for i, weight in enumerate(weights):
            cumulative += weight
            if r <= cumulative:
                return nodes[i]
        
        return nodes[-1]  # Fallback
    
    def _consistent_hash_select(self, nodes: List[NodeHealth], 
                               query_hash: Optional[str]) -> NodeHealth:
        """Consistent hash-based selection"""
        if not query_hash:
            return self._random_select(nodes)
        
        # Create hash ring
        hash_ring = {}
        for node in nodes:
            # Create multiple virtual nodes for better distribution
            for i in range(100):
                virtual_key = f"{node.node_id}:{i}"
                hash_value = int(hashlib.md5(virtual_key.encode()).hexdigest(), 16)
                hash_ring[hash_value] = node
        
        if not hash_ring:
            return self._random_select(nodes)
        
        # Find the node for this query hash
        query_hash_value = int(hashlib.md5(query_hash.encode()).hexdigest(), 16)
        
        # Find the first node clockwise from the query hash
        sorted_keys = sorted(hash_ring.keys())
        for key in sorted_keys:
            if key >= query_hash_value:
                return hash_ring[key]
        
        # Wrap around to the first node
        return hash_ring[sorted_keys[0]]

class HealthChecker:
    """Monitors node health and updates status"""
    
    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.running = False
    
    async def start_monitoring(self, nodes: Dict[str, NodeHealth], 
                             connection_configs: Dict[str, ConnectionConfig]):
        """Start health monitoring for all nodes"""
        self.running = True
        
        while self.running:
            try:
                tasks = []
                for node_id, node in nodes.items():
                    if node_id in connection_configs:
                        task = asyncio.create_task(
                            self._check_node_health(node, connection_configs[node_id])
                        )
                        tasks.append(task)
                
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                await asyncio.sleep(5)
    
    async def stop_monitoring(self):
        """Stop health monitoring"""
        self.running = False
    
    async def _check_node_health(self, node: NodeHealth, config: ConnectionConfig):
        """Check health of a single node"""
        start_time = time.time()
        
        try:
            # Attempt connection and simple query
            conn = await asyncpg.connect(
                host=config.host,
                port=config.port,
                database=config.database,
                user=config.user,
                password=config.password,
                ssl=config.ssl,
                command_timeout=5.0
            )
            
            # Simple health check query
            await conn.fetchval("SELECT 1")
            
            # Get connection stats
            stats = await conn.fetch("""
                SELECT setting::int as max_connections
                FROM pg_settings WHERE name = 'max_connections'
            """)
            
            if stats:
                node.max_connections = stats[0]['max_connections']
            
            # Get active connections
            active_conn_result = await conn.fetchval("""
                SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
            """)
            node.active_connections = active_conn_result or 0
            
            # Get replication lag (if replica)
            try:
                lag_result = await conn.fetchval("""
                    SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
                """)
                node.replication_lag = lag_result or 0.0
            except:
                node.replication_lag = 0.0
            
            await conn.close()
            
            # Update health metrics
            node.response_time = (time.time() - start_time) * 1000  # Convert to ms
            node.is_available = True
            node.last_check = time.time()
            node.error_count = max(0, node.error_count - 1)  # Decrease error count on success
            node.last_error = None
            
            logger.debug(f"Node {node.node_id} health check successful: "
                        f"{node.response_time:.1f}ms, {node.active_connections} connections")
            
        except Exception as e:
            node.is_available = False
            node.response_time = (time.time() - start_time) * 1000
            node.last_check = time.time()
            node.error_count += 1
            node.last_error = str(e)
            
            logger.warning(f"Node {node.node_id} health check failed: {e}")

class ClusterAwareConnectionPool:
    """Main connection pool with cluster awareness"""
    
    def __init__(self, cluster_config: Dict[str, ConnectionConfig], 
                 strategy: LoadBalancingStrategy = LoadBalancingStrategy.LEAST_CONNECTIONS):
        self.cluster_config = cluster_config
        self.strategy = strategy
        
        # Node health tracking
        self.nodes: Dict[str, NodeHealth] = {}
        self.connection_pools: Dict[str, asyncpg.Pool] = {}
        
        # Query routing
        self.query_router = QueryRouter(strategy)
        self.health_checker = HealthChecker()
        
        # Circuit breaker state
        self.circuit_breakers: Dict[str, Dict] = defaultdict(lambda: {
            'failures': 0,
            'last_failure': 0,
            'state': 'closed'  # closed, open, half_open
        })
        
        # Statistics
        self.query_stats = defaultdict(lambda: {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'avg_response_time': 0.0
        })
        
        self.running = False
    
    async def initialize(self):
        """Initialize connection pools for all nodes"""
        logger.info("Initializing cluster-aware connection pool")
        
        # Initialize node health tracking
        for node_id, config in self.cluster_config.items():
            self.nodes[node_id] = NodeHealth(
                node_id=node_id,
                host=config.host,
                port=config.port,
                role=NodeRole.FOLLOWER,  # Will be updated by health checker
                max_connections=config.max_size
            )
        
        # Create connection pools
        for node_id, config in self.cluster_config.items():
            try:
                pool = await asyncpg.create_pool(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    user=config.user,
                    password=config.password,
                    ssl=config.ssl,
                    min_size=config.min_size,
                    max_size=config.max_size,
                    command_timeout=config.command_timeout,
                    server_settings=config.server_settings
                )
                
                self.connection_pools[node_id] = pool
                logger.info(f"Created connection pool for node {node_id} ({config.host}:{config.port})")
                
            except Exception as e:
                logger.error(f"Failed to create connection pool for node {node_id}: {e}")
                self.nodes[node_id].is_available = False
        
        logger.info(f"Connection pool initialized with {len(self.connection_pools)} nodes")
    
    async def start(self):
        """Start the connection pool and health monitoring"""
        self.running = True
        
        # Start health monitoring
        health_task = asyncio.create_task(
            self.health_checker.start_monitoring(self.nodes, self.cluster_config)
        )
        
        # Start role detection
        role_task = asyncio.create_task(self._role_detection_loop())
        
        try:
            await asyncio.gather(health_task, role_task)
        except Exception as e:
            logger.error(f"Connection pool error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the connection pool"""
        self.running = False
        logger.info("Stopping cluster-aware connection pool")
        
        await self.health_checker.stop_monitoring()
        
        # Close all connection pools
        for node_id, pool in self.connection_pools.items():
            try:
                await pool.close()
                logger.info(f"Closed connection pool for node {node_id}")
            except Exception as e:
                logger.error(f"Error closing pool for node {node_id}: {e}")
        
        self.connection_pools.clear()
    
    async def execute_query(self, query: str, *args, query_type: QueryType = QueryType.READ,
                           query_hash: Optional[str] = None, timeout: float = 30.0) -> Any:
        """Execute a query with intelligent routing"""
        start_time = time.time()
        selected_node = None
        
        try:
            # Select appropriate node
            available_nodes = list(self.nodes.values())
            selected_node = self.query_router.select_node(available_nodes, query_type, query_hash)
            
            if not selected_node:
                raise Exception("No available nodes for query execution")
            
            # Check circuit breaker
            if not self._check_circuit_breaker(selected_node.node_id):
                # Find alternative node
                alternative_nodes = [n for n in available_nodes if n.node_id != selected_node.node_id]
                selected_node = self.query_router.select_node(alternative_nodes, query_type, query_hash)
                
                if not selected_node:
                    raise Exception("All nodes are circuit-broken")
            
            # Get connection from pool
            pool = self.connection_pools.get(selected_node.node_id)
            if not pool:
                raise Exception(f"No connection pool for node {selected_node.node_id}")
            
            # Execute query
            async with pool.acquire() as conn:
                if args:
                    result = await asyncio.wait_for(conn.fetch(query, *args), timeout=timeout)
                else:
                    result = await asyncio.wait_for(conn.fetch(query), timeout=timeout)
            
            # Update statistics
            execution_time = time.time() - start_time
            self._update_query_stats(selected_node.node_id, True, execution_time)
            self._reset_circuit_breaker(selected_node.node_id)
            
            logger.debug(f"Query executed on node {selected_node.node_id} in {execution_time:.3f}s")
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            if selected_node:
                self._update_query_stats(selected_node.node_id, False, execution_time)
                self._update_circuit_breaker(selected_node.node_id)
            
            logger.error(f"Query execution failed on node {selected_node.node_id if selected_node else 'unknown'}: {e}")
            raise
    
    async def execute_transaction(self, queries: List[tuple], query_type: QueryType = QueryType.WRITE,
                                timeout: float = 60.0) -> List[Any]:
        """Execute multiple queries in a transaction"""
        start_time = time.time()
        selected_node = None
        
        try:
            # Select appropriate node (transactions must go to leader for writes)
            available_nodes = list(self.nodes.values())
            selected_node = self.query_router.select_node(available_nodes, query_type)
            
            if not selected_node:
                raise Exception("No available nodes for transaction execution")
            
            # Get connection from pool
            pool = self.connection_pools.get(selected_node.node_id)
            if not pool:
                raise Exception(f"No connection pool for node {selected_node.node_id}")
            
            results = []
            
            # Execute transaction
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for query, args in queries:
                        if args:
                            result = await asyncio.wait_for(conn.fetch(query, *args), timeout=timeout)
                        else:
                            result = await asyncio.wait_for(conn.fetch(query), timeout=timeout)
                        results.append(result)
            
            # Update statistics
            execution_time = time.time() - start_time
            self._update_query_stats(selected_node.node_id, True, execution_time)
            self._reset_circuit_breaker(selected_node.node_id)
            
            logger.debug(f"Transaction with {len(queries)} queries executed on node {selected_node.node_id} in {execution_time:.3f}s")
            return results
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            if selected_node:
                self._update_query_stats(selected_node.node_id, False, execution_time)
                self._update_circuit_breaker(selected_node.node_id)
            
            logger.error(f"Transaction execution failed on node {selected_node.node_id if selected_node else 'unknown'}: {e}")
            raise
    
    async def _role_detection_loop(self):
        """Detect node roles (leader/follower)"""
        while self.running:
            try:
                for node_id, pool in self.connection_pools.items():
                    try:
                        async with pool.acquire() as conn:
                            # Check if this is a primary (leader) or replica (follower)
                            is_primary = await conn.fetchval("SELECT NOT pg_is_in_recovery()")
                            
                            if is_primary:
                                self.nodes[node_id].role = NodeRole.LEADER
                            else:
                                self.nodes[node_id].role = NodeRole.FOLLOWER
                                
                    except Exception as e:
                        logger.warning(f"Failed to detect role for node {node_id}: {e}")
                        self.nodes[node_id].role = NodeRole.UNAVAILABLE
                
                await asyncio.sleep(30)  # Check roles every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in role detection loop: {e}")
                await asyncio.sleep(10)
    
    def _check_circuit_breaker(self, node_id: str) -> bool:
        """Check if circuit breaker allows requests to this node"""
        breaker = self.circuit_breakers[node_id]
        current_time = time.time()
        
        if breaker['state'] == 'closed':
            return True
        elif breaker['state'] == 'open':
            # Check if we should try half-open
            if current_time - breaker['last_failure'] > 60:  # 60 second timeout
                breaker['state'] = 'half_open'
                return True
            return False
        else:  # half_open
            return True
    
    def _update_circuit_breaker(self, node_id: str):
        """Update circuit breaker on failure"""
        breaker = self.circuit_breakers[node_id]
        breaker['failures'] += 1
        breaker['last_failure'] = time.time()
        
        if breaker['failures'] >= 5:  # Trip after 5 failures
            breaker['state'] = 'open'
            logger.warning(f"Circuit breaker opened for node {node_id}")
    
    def _reset_circuit_breaker(self, node_id: str):
        """Reset circuit breaker on success"""
        breaker = self.circuit_breakers[node_id]
        if breaker['state'] in ['half_open', 'open']:
            breaker['state'] = 'closed'
            breaker['failures'] = 0
            logger.info(f"Circuit breaker closed for node {node_id}")
    
    def _update_query_stats(self, node_id: str, success: bool, execution_time: float):
        """Update query statistics"""
        stats = self.query_stats[node_id]
        stats['total_queries'] += 1
        
        if success:
            stats['successful_queries'] += 1
        else:
            stats['failed_queries'] += 1
        
        # Update average response time
        if stats['total_queries'] == 1:
            stats['avg_response_time'] = execution_time
        else:
            stats['avg_response_time'] = (
                (stats['avg_response_time'] * (stats['total_queries'] - 1) + execution_time) /
                stats['total_queries']
            )
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get overall cluster status"""
        total_nodes = len(self.nodes)
        available_nodes = sum(1 for node in self.nodes.values() if node.is_available)
        leaders = [node for node in self.nodes.values() if node.role == NodeRole.LEADER]
        followers = [node for node in self.nodes.values() if node.role == NodeRole.FOLLOWER]
        
        return {
            'total_nodes': total_nodes,
            'available_nodes': available_nodes,
            'unavailable_nodes': total_nodes - available_nodes,
            'leaders': len(leaders),
            'followers': len(followers),
            'leader_nodes': [n.node_id for n in leaders],
            'strategy': self.strategy.value,
            'nodes': {
                node_id: {
                    'host': f"{node.host}:{node.port}",
                    'role': node.role.value,
                    'available': node.is_available,
                    'health_score': node.health_score,
                    'active_connections': node.active_connections,
                    'max_connections': node.max_connections,
                    'response_time': node.response_time,
                    'replication_lag': node.replication_lag,
                    'circuit_breaker': self.circuit_breakers[node_id]['state'],
                    'query_stats': dict(self.query_stats[node_id])
                }
                for node_id, node in self.nodes.items()
            }
        }
    
    def get_node_by_id(self, node_id: str) -> Optional[NodeHealth]:
        """Get node health by ID"""
        return self.nodes.get(node_id)
    
    def get_pool_by_node_id(self, node_id: str) -> Optional[asyncpg.Pool]:
        """Get connection pool by node ID"""
        return self.connection_pools.get(node_id)