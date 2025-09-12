"""
SynapseDB Cluster Manager
Coordinates cluster membership, health monitoring, and failover
"""

import asyncio
import asyncpg
import logging
import json
import time
import uuid
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import aiohttp
import yaml
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NodeInfo:
    """Information about a cluster node"""
    node_id: str
    node_name: str
    host: str
    port: int
    status: str
    is_writer: bool
    last_heartbeat: datetime
    version: Optional[str] = None

@dataclass
class ClusterStatus:
    """Overall cluster status"""
    total_nodes: int
    active_nodes: int
    writer_node: Optional[NodeInfo]
    has_quorum: bool
    nodes: List[NodeInfo]

class DatabaseConnection:
    """Manages database connections with retry logic"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.pool: Optional[asyncpg.Pool] = None
        
    async def connect(self) -> bool:
        """Establish connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
                command_timeout=10
            )
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}:{self.port} - {e}")
            return False
    
    async def execute(self, query: str, *args) -> Optional[List]:
        """Execute query with error handling"""
        if not self.pool:
            return None
            
        try:
            async with self.pool.acquire() as conn:
                if query.strip().upper().startswith('SELECT'):
                    return await conn.fetch(query, *args)
                else:
                    await conn.execute(query, *args)
                    return []
        except Exception as e:
            logger.error(f"Query failed on {self.host}:{self.port} - {e}")
            return None
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()

class ClusterManager:
    """Main cluster management service"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.connections: Dict[str, DatabaseConnection] = {}
        self.cluster_state: Optional[ClusterStatus] = None
        self.running = False
        self.node_name = self.config['node']['name']
        self.node_id = self.config['node']['id']
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    async def initialize(self):
        """Initialize cluster manager"""
        logger.info(f"Initializing cluster manager for node {self.node_name}")
        
        # Connect to all configured nodes
        for node_config in self.config['cluster']['nodes']:
            node_name = node_config['name']
            conn = DatabaseConnection(
                host=node_config['host'],
                port=node_config['port'],
                database=self.config['database']['name'],
                user=self.config['database']['user'],
                password=self.config['database']['password']
            )
            
            if await conn.connect():
                self.connections[node_name] = conn
                logger.info(f"Connected to node {node_name}")
            else:
                logger.error(f"Failed to connect to node {node_name}")
        
        # Register this node if not already registered
        await self._register_node()
        
    async def _register_node(self):
        """Register this node in the cluster"""
        local_conn = self.connections.get(self.node_name)
        if not local_conn:
            logger.error("Cannot register node - no local connection")
            return
        
        register_query = """
            INSERT INTO cluster_nodes (node_id, node_name, host, port, status, version)
            VALUES ($1, $2, $3, $4, 'active', $5)
            ON CONFLICT (node_name) 
            DO UPDATE SET 
                status = 'active',
                last_heartbeat = NOW(),
                updated_at = NOW(),
                version = EXCLUDED.version
        """
        
        await local_conn.execute(
            register_query,
            self.node_id,
            self.node_name,
            self.config['node']['host'],
            self.config['node']['port'],
            self.config.get('version', '1.0.0')
        )
        
        # Set node-specific settings
        await local_conn.execute("SELECT set_config('synapsedb.node_name', $1, false)", self.node_name)
        await local_conn.execute("SELECT set_config('synapsedb.node_id', $1, false)", self.node_id)
        await local_conn.execute("SELECT set_config('synapsedb.host', $1, false)", self.config['node']['host'])
        await local_conn.execute("SELECT set_config('synapsedb.port', $1, false)", str(self.config['node']['port']))
        
        logger.info(f"Node {self.node_name} registered successfully")
    
    async def start(self):
        """Start cluster management services"""
        self.running = True
        logger.info("Starting cluster manager services")
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._health_monitor_loop()),
            asyncio.create_task(self._replication_monitor_loop()),
            asyncio.create_task(self._ddl_processor_loop()),
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Cluster manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop cluster manager"""
        self.running = False
        logger.info("Stopping cluster manager")
        
        # Close all connections
        for conn in self.connections.values():
            await conn.close()
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while self.running:
            try:
                local_conn = self.connections.get(self.node_name)
                if local_conn:
                    await local_conn.execute("SELECT update_node_heartbeat($1)", self.node_name)
                
                await asyncio.sleep(10)  # Heartbeat every 10 seconds
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                await asyncio.sleep(5)
    
    async def _health_monitor_loop(self):
        """Monitor cluster health and manage failover"""
        while self.running:
            try:
                await self._update_cluster_status()
                await self._check_and_handle_failures()
                await asyncio.sleep(15)  # Health check every 15 seconds
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(10)
    
    async def _update_cluster_status(self):
        """Update cluster status from all reachable nodes"""
        nodes = []
        active_count = 0
        writer_node = None
        
        for node_name, conn in self.connections.items():
            node_query = """
                SELECT node_id, node_name, host, port, status, is_writer, 
                       last_heartbeat, version
                FROM cluster_nodes 
                WHERE node_name = $1
            """
            
            result = await conn.execute(node_query, node_name)
            if result:
                row = result[0]
                node_info = NodeInfo(
                    node_id=str(row['node_id']),
                    node_name=row['node_name'],
                    host=row['host'],
                    port=row['port'],
                    status=row['status'],
                    is_writer=row['is_writer'],
                    last_heartbeat=row['last_heartbeat'],
                    version=row['version']
                )
                
                # Check if node is actually reachable
                if time.time() - node_info.last_heartbeat.timestamp() > 30:
                    node_info.status = 'failed'
                    await self._mark_node_failed(node_name)
                else:
                    active_count += 1
                
                nodes.append(node_info)
                
                if node_info.is_writer and node_info.status == 'active':
                    writer_node = node_info
        
        # Check quorum
        has_quorum = await self._check_quorum()
        
        self.cluster_state = ClusterStatus(
            total_nodes=len(nodes),
            active_nodes=active_count,
            writer_node=writer_node,
            has_quorum=has_quorum,
            nodes=nodes
        )
        
        logger.debug(f"Cluster status: {active_count}/{len(nodes)} nodes active, quorum: {has_quorum}")
    
    async def _check_quorum(self) -> bool:
        """Check if cluster has quorum"""
        local_conn = self.connections.get(self.node_name)
        if not local_conn:
            return False
            
        result = await local_conn.execute("SELECT check_quorum()")
        return result[0]['check_quorum'] if result else False
    
    async def _mark_node_failed(self, node_name: str):
        """Mark a node as failed"""
        for conn_name, conn in self.connections.items():
            try:
                await conn.execute(
                    "UPDATE cluster_nodes SET status = 'failed', updated_at = NOW() WHERE node_name = $1",
                    node_name
                )
                break
            except:
                continue
    
    async def _check_and_handle_failures(self):
        """Check for failures and trigger failover if needed"""
        if not self.cluster_state:
            return
        
        # If no writer or writer failed, promote a new one
        if not self.cluster_state.writer_node or self.cluster_state.writer_node.status != 'active':
            await self._handle_writer_failover()
        
        # Remove failed nodes from replication
        for node in self.cluster_state.nodes:
            if node.status == 'failed':
                await self._remove_failed_node_from_replication(node.node_name)
    
    async def _handle_writer_failover(self):
        """Handle writer node failover"""
        if not self.cluster_state.has_quorum:
            logger.warning("Cannot perform failover - no quorum")
            return
        
        # Find best candidate for new writer (active node with lowest lag)
        active_nodes = [n for n in self.cluster_state.nodes if n.status == 'active']
        if not active_nodes:
            logger.error("No active nodes for failover")
            return
        
        # For now, just pick the first active node
        # In production, you'd consider replication lag, load, etc.
        new_writer = active_nodes[0]
        
        logger.info(f"Promoting {new_writer.node_name} to writer")
        
        # Promote the new writer
        writer_conn = self.connections.get(new_writer.node_name)
        if writer_conn:
            try:
                await writer_conn.execute("SELECT promote_to_writer($1)", new_writer.node_name)
                logger.info(f"Successfully promoted {new_writer.node_name} to writer")
            except Exception as e:
                logger.error(f"Failed to promote {new_writer.node_name}: {e}")
    
    async def _remove_failed_node_from_replication(self, failed_node: str):
        """Remove failed node from replication topology"""
        # This would involve removing pglogical subscriptions/publications
        # Implementation depends on the specific replication setup
        logger.info(f"Removing failed node {failed_node} from replication")
    
    async def _replication_monitor_loop(self):
        """Monitor replication status and lag"""
        while self.running:
            try:
                local_conn = self.connections.get(self.node_name)
                if local_conn:
                    await local_conn.execute("SELECT update_replication_lag()")
                
                await asyncio.sleep(30)  # Check replication every 30 seconds
            except Exception as e:
                logger.error(f"Replication monitor error: {e}")
                await asyncio.sleep(15)
    
    async def _ddl_processor_loop(self):
        """Process DDL replication queue"""
        while self.running:
            try:
                local_conn = self.connections.get(self.node_name)
                if local_conn:
                    await local_conn.execute("SELECT process_ddl_replication()")
                
                await asyncio.sleep(5)  # Process DDL every 5 seconds
            except Exception as e:
                logger.error(f"DDL processor error: {e}")
                await asyncio.sleep(10)
    
    async def get_cluster_status(self) -> Optional[ClusterStatus]:
        """Get current cluster status"""
        await self._update_cluster_status()
        return self.cluster_state
    
    async def execute_with_quorum(self, query: str, *args) -> bool:
        """Execute query only if quorum is available"""
        if not self.cluster_state or not self.cluster_state.has_quorum:
            logger.warning("Cannot execute query - no quorum")
            return False
        
        writer_conn = None
        if self.cluster_state.writer_node:
            writer_conn = self.connections.get(self.cluster_state.writer_node.node_name)
        
        if not writer_conn:
            logger.error("No writer connection available")
            return False
        
        try:
            await writer_conn.execute(query, *args)
            return True
        except Exception as e:
            logger.error(f"Quorum query failed: {e}")
            return False

# Configuration management
def generate_config(node_name: str, node_host: str, node_port: int, 
                   cluster_nodes: List[dict], output_path: str):
    """Generate configuration file for a node"""
    config = {
        'node': {
            'name': node_name,
            'id': str(uuid.uuid4()),
            'host': node_host,
            'port': node_port
        },
        'database': {
            'name': 'synapsedb',
            'user': 'synapsedb',
            'password': os.getenv('SYNAPSEDB_PASSWORD', 'changeme')
        },
        'cluster': {
            'nodes': cluster_nodes
        },
        'version': '1.0.0'
    }
    
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

# Main entry point
async def main():
    """Main entry point for cluster manager"""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python cluster_manager.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    manager = ClusterManager(config_path)
    
    try:
        await manager.initialize()
        await manager.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())