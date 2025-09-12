"""
SynapseDB Query Router
Routes queries to appropriate nodes based on load, health, and consistency requirements
"""

import asyncio
import asyncpg
import logging
import json
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import aiohttp
from aiohttp import web
import yaml

logger = logging.getLogger(__name__)

class QueryType(Enum):
    READ = "read"
    WRITE = "write"
    DDL = "ddl"
    VECTOR_SEARCH = "vector_search"

class ConsistencyLevel(Enum):
    STRONG = "strong"          # Must read from writer
    EVENTUAL = "eventual"      # Can read from any node
    BOUNDED_STALENESS = "bounded"  # Read from node with recent data

@dataclass
class QueryContext:
    """Context for query routing decisions"""
    query_type: QueryType
    consistency_level: ConsistencyLevel
    max_staleness_ms: int = 1000
    preferred_node: Optional[str] = None
    vector_dimensions: Optional[int] = None

@dataclass
class NodeMetrics:
    """Performance metrics for a node"""
    node_name: str
    cpu_usage: float
    memory_usage: float
    active_connections: int
    avg_query_time_ms: float
    replication_lag_ms: int
    last_updated: float

class QueryRouter:
    """Routes queries to appropriate database nodes"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.nodes: Dict[str, dict] = {}
        self.node_metrics: Dict[str, NodeMetrics] = {}
        self.connection_pools: Dict[str, asyncpg.Pool] = {}
        self.running = False
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    async def initialize(self):
        """Initialize query router"""
        logger.info("Initializing query router")
        
        # Initialize connection pools for all nodes
        for node_config in self.config['cluster']['nodes']:
            node_name = node_config['name']
            self.nodes[node_name] = node_config
            
            try:
                pool = await asyncpg.create_pool(
                    host=node_config['host'],
                    port=node_config['port'],
                    database=self.config['database']['name'],
                    user=self.config['database']['user'],
                    password=self.config['database']['password'],
                    min_size=5,
                    max_size=20,
                    command_timeout=30
                )
                self.connection_pools[node_name] = pool
                logger.info(f"Connected to node {node_name}")
            except Exception as e:
                logger.error(f"Failed to connect to node {node_name}: {e}")
    
    async def start(self):
        """Start query router services"""
        self.running = True
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._metrics_collector_loop()),
            asyncio.create_task(self._start_http_server()),
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Query router error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop query router"""
        self.running = False
        logger.info("Stopping query router")
        
        # Close all connection pools
        for pool in self.connection_pools.values():
            await pool.close()
    
    async def _metrics_collector_loop(self):
        """Collect metrics from all nodes"""
        while self.running:
            try:
                await self._collect_node_metrics()
                await asyncio.sleep(10)  # Collect metrics every 10 seconds
            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
                await asyncio.sleep(5)
    
    async def _collect_node_metrics(self):
        """Collect performance metrics from all nodes"""
        for node_name, pool in self.connection_pools.items():
            try:
                async with pool.acquire() as conn:
                    # Collect basic metrics
                    metrics_query = """
                        SELECT 
                            -- Connection stats
                            (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                            
                            -- Replication lag
                            COALESCE((
                                SELECT lag_seconds 
                                FROM replication_status 
                                WHERE target_node_id = (
                                    SELECT node_id FROM cluster_nodes WHERE node_name = $1
                                ) 
                                ORDER BY last_sync_time DESC 
                                LIMIT 1
                            ), 0) as replication_lag_seconds,
                            
                            -- Query performance (simplified)
                            COALESCE((
                                SELECT avg(mean_exec_time)::float 
                                FROM pg_stat_statements 
                                WHERE calls > 10
                            ), 50.0) as avg_query_time_ms,
                            
                            -- Node status
                            (SELECT status FROM cluster_nodes WHERE node_name = $1) as node_status
                    """
                    
                    result = await conn.fetchrow(metrics_query, node_name)
                    
                    if result:
                        self.node_metrics[node_name] = NodeMetrics(
                            node_name=node_name,
                            cpu_usage=50.0,  # Would need system metrics collection
                            memory_usage=60.0,  # Would need system metrics collection
                            active_connections=result['active_connections'],
                            avg_query_time_ms=float(result['avg_query_time_ms']),
                            replication_lag_ms=result['replication_lag_seconds'] * 1000,
                            last_updated=time.time()
                        )
                        
            except Exception as e:
                logger.warning(f"Failed to collect metrics from {node_name}: {e}")
                # Mark node as potentially problematic
                if node_name in self.node_metrics:
                    self.node_metrics[node_name].avg_query_time_ms = float('inf')
    
    def _analyze_query(self, query: str) -> QueryContext:
        """Analyze query to determine routing strategy"""
        query_lower = query.lower().strip()
        
        # Determine query type
        if query_lower.startswith(('select', 'with')):
            if '<->' in query_lower or 'vector' in query_lower or 'embedding' in query_lower:
                query_type = QueryType.VECTOR_SEARCH
            else:
                query_type = QueryType.READ
        elif query_lower.startswith(('insert', 'update', 'delete')):
            query_type = QueryType.WRITE
        elif query_lower.startswith(('create', 'alter', 'drop', 'grant', 'revoke')):
            query_type = QueryType.DDL
        else:
            query_type = QueryType.READ  # Default to read
        
        # Determine consistency requirements
        if query_type in (QueryType.WRITE, QueryType.DDL):
            consistency_level = ConsistencyLevel.STRONG
        elif 'FOR UPDATE' in query.upper() or 'LOCK' in query.upper():
            consistency_level = ConsistencyLevel.STRONG
        else:
            consistency_level = ConsistencyLevel.EVENTUAL
        
        return QueryContext(
            query_type=query_type,
            consistency_level=consistency_level,
            max_staleness_ms=1000
        )
    
    async def _get_cluster_status(self) -> dict:
        """Get current cluster status"""
        # Try to get status from any available node
        for node_name, pool in self.connection_pools.items():
            try:
                async with pool.acquire() as conn:
                    result = await conn.fetch("""
                        SELECT 
                            node_name, host, port, status, is_writer, last_heartbeat
                        FROM cluster_nodes 
                        ORDER BY is_writer DESC, last_heartbeat DESC
                    """)
                    
                    nodes = []
                    writer_node = None
                    active_count = 0
                    
                    for row in result:
                        node_info = dict(row)
                        nodes.append(node_info)
                        
                        if node_info['is_writer'] and node_info['status'] == 'active':
                            writer_node = node_info
                        
                        if node_info['status'] == 'active':
                            active_count += 1
                    
                    # Check quorum
                    quorum_result = await conn.fetchval("SELECT check_quorum()")
                    
                    return {
                        'nodes': nodes,
                        'writer_node': writer_node,
                        'active_nodes': active_count,
                        'total_nodes': len(nodes),
                        'has_quorum': quorum_result
                    }
            except Exception as e:
                logger.warning(f"Failed to get cluster status from {node_name}: {e}")
                continue
        
        return {'nodes': [], 'has_quorum': False, 'active_nodes': 0}
    
    def _select_node_for_read(self, context: QueryContext) -> Optional[str]:
        """Select best node for read query"""
        if context.consistency_level == ConsistencyLevel.STRONG:
            # Must read from writer
            cluster_status = asyncio.run(self._get_cluster_status())
            writer = cluster_status.get('writer_node')
            return writer['node_name'] if writer else None
        
        # For eventual consistency, find best available node
        available_nodes = []
        
        for node_name, metrics in self.node_metrics.items():
            # Skip nodes that are too stale
            if (context.consistency_level == ConsistencyLevel.BOUNDED_STALENESS and 
                metrics.replication_lag_ms > context.max_staleness_ms):
                continue
            
            # Skip overloaded nodes
            if metrics.avg_query_time_ms == float('inf'):
                continue
            
            available_nodes.append((node_name, metrics))
        
        if not available_nodes:
            # Fallback to any available node
            available_nodes = [(name, metrics) for name, metrics in self.node_metrics.items()]
        
        if not available_nodes:
            return None
        
        # Select node with lowest load score
        def load_score(metrics: NodeMetrics) -> float:
            return (
                metrics.cpu_usage * 0.3 +
                metrics.memory_usage * 0.2 +
                (metrics.active_connections / 20) * 0.2 +  # Normalize connections
                (metrics.avg_query_time_ms / 100) * 0.3  # Normalize query time
            )
        
        best_node = min(available_nodes, key=lambda x: load_score(x[1]))
        return best_node[0]
    
    async def _select_node_for_write(self) -> Optional[str]:
        """Select node for write query (must be writer with quorum)"""
        cluster_status = await self._get_cluster_status()
        
        if not cluster_status['has_quorum']:
            logger.error("Cannot execute write - no quorum")
            return None
        
        writer = cluster_status.get('writer_node')
        if not writer or writer['status'] != 'active':
            logger.error("No active writer node available")
            return None
        
        return writer['node_name']
    
    async def execute_query(self, query: str, params: List[Any] = None, 
                          context: Optional[QueryContext] = None) -> Tuple[bool, Any]:
        """Execute query on appropriate node"""
        if params is None:
            params = []
        
        if context is None:
            context = self._analyze_query(query)
        
        # Select appropriate node
        if context.query_type in (QueryType.WRITE, QueryType.DDL):
            node_name = await self._select_node_for_write()
        else:
            node_name = self._select_node_for_read(context)
        
        if not node_name:
            return False, "No suitable node available"
        
        # Execute query on selected node
        pool = self.connection_pools.get(node_name)
        if not pool:
            return False, f"No connection pool for node {node_name}"
        
        try:
            async with pool.acquire() as conn:
                start_time = time.time()
                
                if context.query_type == QueryType.WRITE:
                    # For writes, check quorum before executing
                    has_quorum = await conn.fetchval("SELECT check_quorum()")
                    if not has_quorum:
                        return False, "Write failed - no quorum"
                
                # Execute the query
                if query.strip().upper().startswith('SELECT'):
                    result = await conn.fetch(query, *params)
                    result = [dict(row) for row in result]  # Convert to dict list
                else:
                    await conn.execute(query, *params)
                    result = "Query executed successfully"
                
                # Update metrics
                execution_time = (time.time() - start_time) * 1000
                logger.debug(f"Query executed on {node_name} in {execution_time:.2f}ms")
                
                return True, result
                
        except Exception as e:
            logger.error(f"Query execution failed on {node_name}: {e}")
            return False, str(e)
    
    async def _start_http_server(self):
        """Start HTTP API server"""
        app = web.Application()
        
        # Add routes
        app.router.add_post('/execute', self._handle_execute_query)
        app.router.add_get('/status', self._handle_status)
        app.router.add_get('/metrics', self._handle_metrics)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', self.config.get('router_port', 8080))
        await site.start()
        
        logger.info(f"Query router HTTP server started on port {self.config.get('router_port', 8080)}")
        
        # Keep server running
        while self.running:
            await asyncio.sleep(1)
        
        await runner.cleanup()
    
    async def _handle_execute_query(self, request):
        """Handle query execution via HTTP"""
        try:
            data = await request.json()
            
            query = data.get('query', '')
            params = data.get('params', [])
            
            # Parse context if provided
            context = None
            if 'context' in data:
                ctx_data = data['context']
                context = QueryContext(
                    query_type=QueryType(ctx_data.get('query_type', 'read')),
                    consistency_level=ConsistencyLevel(ctx_data.get('consistency_level', 'eventual')),
                    max_staleness_ms=ctx_data.get('max_staleness_ms', 1000),
                    preferred_node=ctx_data.get('preferred_node')
                )
            
            success, result = await self.execute_query(query, params, context)
            
            return web.json_response({
                'success': success,
                'result': result,
                'timestamp': time.time()
            })
            
        except Exception as e:
            return web.json_response({
                'success': False,
                'error': str(e)
            }, status=500)
    
    async def _handle_status(self, request):
        """Handle status request"""
        cluster_status = await self._get_cluster_status()
        
        return web.json_response({
            'cluster_status': cluster_status,
            'router_metrics': {
                name: {
                    'cpu_usage': metrics.cpu_usage,
                    'memory_usage': metrics.memory_usage,
                    'active_connections': metrics.active_connections,
                    'avg_query_time_ms': metrics.avg_query_time_ms,
                    'replication_lag_ms': metrics.replication_lag_ms
                }
                for name, metrics in self.node_metrics.items()
            }
        })
    
    async def _handle_metrics(self, request):
        """Handle metrics request (Prometheus format)"""
        metrics_text = []
        
        for node_name, metrics in self.node_metrics.items():
            metrics_text.extend([
                f'synapsedb_cpu_usage{{node="{node_name}"}} {metrics.cpu_usage}',
                f'synapsedb_memory_usage{{node="{node_name}"}} {metrics.memory_usage}',
                f'synapsedb_active_connections{{node="{node_name}"}} {metrics.active_connections}',
                f'synapsedb_avg_query_time_ms{{node="{node_name}"}} {metrics.avg_query_time_ms}',
                f'synapsedb_replication_lag_ms{{node="{node_name}"}} {metrics.replication_lag_ms}'
            ])
        
        return web.Response(
            text='\n'.join(metrics_text),
            content_type='text/plain'
        )

# Main entry point
async def main():
    """Main entry point for query router"""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python query_router.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    router = QueryRouter(config_path)
    
    try:
        await router.initialize()
        await router.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await router.stop()

if __name__ == "__main__":
    asyncio.run(main())