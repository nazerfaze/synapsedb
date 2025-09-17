#!/usr/bin/env python3
"""
SynapseDB Cluster Initialization Service
Initializes the distributed database cluster with all necessary components
"""

import asyncio
import asyncpg
import logging
import os
import time
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClusterInitializer:
    """Initializes the SynapseDB cluster"""
    
    def __init__(self):
        self.cluster_nodes = self._parse_cluster_nodes()
        self.db_config = {
            'database': os.getenv('DB_NAME', 'synapsedb'),
            'user': os.getenv('DB_USER', 'synapsedb'),
            'password': os.getenv('DB_PASSWORD', 'synapsedb')
        }
    
    def _parse_cluster_nodes(self) -> List[Dict]:
        """Parse cluster nodes from environment"""
        nodes_str = os.getenv('CLUSTER_NODES', '')
        nodes = []
        
        for node_str in nodes_str.split(','):
            if ':' in node_str:
                parts = node_str.split(':')
                if len(parts) >= 3:
                    nodes.append({
                        'id': parts[0],
                        'host': parts[1],
                        'port': int(parts[2])
                    })
        
        if not nodes:
            # Default configuration
            nodes = [
                {'id': 'node1', 'host': 'postgres1', 'port': 5432},
                {'id': 'node2', 'host': 'postgres2', 'port': 5432},
                {'id': 'node3', 'host': 'postgres3', 'port': 5432}
            ]
        
        return nodes
    
    async def initialize_cluster(self):
        """Initialize the complete cluster"""
        logger.info("Starting SynapseDB cluster initialization")
        
        # Wait for all nodes to be ready
        await self._wait_for_nodes()
        
        # Install extensions on all nodes
        await self._install_extensions()
        
        # Create system schemas
        await self._create_system_schemas()
        
        # Setup pglogical replication
        await self._setup_replication()
        
        # Initialize cluster metadata
        await self._initialize_metadata()
        
        # Setup demo data (optional)
        await self._setup_demo_data()
        
        logger.info("SynapseDB cluster initialization complete!")
    
    async def _wait_for_nodes(self, timeout=300):
        """Wait for all nodes to be ready"""
        logger.info("Waiting for all nodes to be ready...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            ready_nodes = 0
            
            for node in self.cluster_nodes:
                try:
                    conn = await asyncpg.connect(
                        host=node['host'],
                        port=node['port'],
                        **self.db_config,
                        command_timeout=5
                    )
                    
                    result = await conn.fetchval("SELECT 1")
                    if result == 1:
                        ready_nodes += 1
                    
                    await conn.close()
                    
                except Exception as e:
                    logger.debug(f"Node {node['id']} not ready: {e}")
            
            if ready_nodes == len(self.cluster_nodes):
                logger.info(f"All {ready_nodes} nodes are ready!")
                return
            
            logger.info(f"{ready_nodes}/{len(self.cluster_nodes)} nodes ready, waiting...")
            await asyncio.sleep(5)
        
        raise TimeoutError(f"Nodes not ready within {timeout} seconds")
    
    async def _install_extensions(self):
        """Install required PostgreSQL extensions on all nodes"""
        logger.info("Installing PostgreSQL extensions...")
        
        extensions = [
            'pglogical',
            'uuid-ossp',
            'btree_gist'
        ]
        
        # Try to install pgvector (optional)
        optional_extensions = ['vector']
        
        for node in self.cluster_nodes:
            logger.info(f"Installing extensions on {node['id']}")
            
            conn = await asyncpg.connect(
                host=node['host'],
                port=node['port'],
                **self.db_config
            )
            
            try:
                # Install required extensions
                for ext in extensions:
                    try:
                        await conn.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
                        logger.info(f"Installed {ext} on {node['id']}")
                    except Exception as e:
                        logger.error(f"Failed to install {ext} on {node['id']}: {e}")
                        raise
                
                # Install optional extensions
                for ext in optional_extensions:
                    try:
                        await conn.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
                        logger.info(f"Installed {ext} on {node['id']}")
                    except Exception as e:
                        logger.warning(f"Optional extension {ext} not available on {node['id']}: {e}")
                
            finally:
                await conn.close()
    
    async def _create_system_schemas(self):
        """Create system schemas and tables"""
        logger.info("Creating system schemas...")
        
        # Create schemas on the first node (will replicate to others)
        conn = await asyncpg.connect(
            host=self.cluster_nodes[0]['host'],
            port=self.cluster_nodes[0]['port'],
            **self.db_config
        )
        
        try:
            # Create system schemas
            schemas = [
                'synapsedb_raft',
                'synapsedb_consensus',
                'synapsedb_sharding',
                'synapsedb_schema',
                'synapsedb_deadlock',
                'synapsedb_backups',
                'synapsedb_updates'
            ]
            
            for schema in schemas:
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                logger.info(f"Created schema: {schema}")
            
            # Create cluster metadata tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS synapsedb_consensus.cluster_nodes (
                    node_id VARCHAR(64) PRIMARY KEY,
                    host VARCHAR(255) NOT NULL,
                    port INTEGER NOT NULL,
                    status VARCHAR(32) DEFAULT 'active',
                    role VARCHAR(32) DEFAULT 'follower',
                    last_heartbeat TIMESTAMP DEFAULT NOW(),
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Insert cluster node information
            for node in self.cluster_nodes:
                await conn.execute("""
                    INSERT INTO synapsedb_consensus.cluster_nodes 
                    (node_id, host, port, status, role)
                    VALUES ($1, $2, $3, 'active', 'follower')
                    ON CONFLICT (node_id) DO UPDATE SET
                        host = EXCLUDED.host,
                        port = EXCLUDED.port,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """, node['id'], node['host'], node['port'])
            
            logger.info("System schemas and tables created")
            
        finally:
            await conn.close()
    
    async def _setup_replication(self):
        """Setup pglogical replication between all nodes"""
        logger.info("Setting up pglogical replication...")
        
        # Create pglogical node on each database
        for node in self.cluster_nodes:
            logger.info(f"Setting up pglogical node on {node['id']}")
            
            conn = await asyncpg.connect(
                host=node['host'],
                port=node['port'],
                **self.db_config
            )
            
            try:
                # Create pglogical node
                node_dsn = f"host={node['host']} port={node['port']} dbname={self.db_config['database']} user={self.db_config['user']} password={self.db_config['password']}"
                
                await conn.execute(
                    "SELECT pglogical.create_node(node_name := $1, dsn := $2)",
                    f"node_{node['id']}", node_dsn
                )
                
                # Create replication set
                await conn.execute(
                    "SELECT pglogical.create_replication_set($1, $2, $3, $4, $5)",
                    'synapsedb_set', True, True, True, True
                )
                
                logger.info(f"pglogical node created on {node['id']}")
                
            except Exception as e:
                if "already exists" in str(e):
                    logger.info(f"pglogical node already exists on {node['id']}")
                else:
                    logger.error(f"Failed to create pglogical node on {node['id']}: {e}")
                    raise
            finally:
                await conn.close()
        
        # Create subscriptions between nodes (full mesh)
        await asyncio.sleep(5)  # Allow nodes to be ready
        
        for source_node in self.cluster_nodes:
            for target_node in self.cluster_nodes:
                if source_node['id'] != target_node['id']:
                    await self._create_subscription(source_node, target_node)
    
    async def _create_subscription(self, source_node: Dict, target_node: Dict):
        """Create subscription from source to target node"""
        logger.info(f"Creating subscription from {source_node['id']} to {target_node['id']}")
        
        target_conn = await asyncpg.connect(
            host=target_node['host'],
            port=target_node['port'],
            **self.db_config
        )
        
        try:
            source_dsn = f"host={source_node['host']} port={source_node['port']} dbname={self.db_config['database']} user={self.db_config['user']} password={self.db_config['password']}"
            subscription_name = f"sub_{source_node['id']}_to_{target_node['id']}"
            
            await target_conn.execute("""
                SELECT pglogical.create_subscription(
                    subscription_name := $1,
                    provider_dsn := $2,
                    replication_sets := ARRAY['synapsedb_set'],
                    synchronize_structure := false,
                    synchronize_data := false
                )
            """, subscription_name, source_dsn)
            
            logger.info(f"Subscription created: {subscription_name}")
            
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"Subscription already exists: {source_node['id']} -> {target_node['id']}")
            else:
                logger.error(f"Failed to create subscription {source_node['id']} -> {target_node['id']}: {e}")
                # Don't raise - continue with other subscriptions
        finally:
            await target_conn.close()
    
    async def _initialize_metadata(self):
        """Initialize cluster metadata"""
        logger.info("Initializing cluster metadata...")
        
        conn = await asyncpg.connect(
            host=self.cluster_nodes[0]['host'],
            port=self.cluster_nodes[0]['port'],
            **self.db_config
        )
        
        try:
            # Initialize Raft state
            await conn.execute("""
                INSERT INTO synapsedb_raft.raft_state 
                (node_id, current_term, voted_for, role, leader_id)
                VALUES ($1, 0, NULL, 'follower', NULL)
                ON CONFLICT (node_id) DO NOTHING
            """, self.cluster_nodes[0]['id'])
            
            # Initialize shard configuration
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS synapsedb_sharding.shard_config (
                    shard_id INTEGER PRIMARY KEY,
                    node_id VARCHAR(64) NOT NULL,
                    status VARCHAR(32) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Initialize basic shard distribution
            shard_count = 256
            for i in range(shard_count):
                node_index = i % len(self.cluster_nodes)
                assigned_node = self.cluster_nodes[node_index]['id']
                
                await conn.execute("""
                    INSERT INTO synapsedb_sharding.shard_config (shard_id, node_id)
                    VALUES ($1, $2)
                    ON CONFLICT (shard_id) DO NOTHING
                """, i, assigned_node)
            
            logger.info(f"Initialized {shard_count} shards across {len(self.cluster_nodes)} nodes")
            
        finally:
            await conn.close()
    
    async def _setup_demo_data(self):
        """Setup demo tables and data"""
        logger.info("Setting up demo data...")
        
        conn = await asyncpg.connect(
            host=self.cluster_nodes[0]['host'],
            port=self.cluster_nodes[0]['port'],
            **self.db_config
        )
        
        try:
            # Create demo tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    product_name VARCHAR(100) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    order_date TIMESTAMP DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    content TEXT,
                    embedding_json JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Add tables to replication set
            tables = ['users', 'orders', 'documents']
            for table in tables:
                try:
                    await conn.execute(
                        "SELECT pglogical.replication_set_add_table($1, $2)",
                        'synapsedb_set', f'public.{table}'
                    )
                    logger.info(f"Added {table} to replication set")
                except Exception as e:
                    if "already exists" not in str(e):
                        logger.error(f"Failed to add {table} to replication: {e}")
            
            # Insert some demo data
            demo_users = [
                ('alice', 'alice@example.com'),
                ('bob', 'bob@example.com'),
                ('charlie', 'charlie@example.com')
            ]
            
            for username, email in demo_users:
                try:
                    await conn.execute("""
                        INSERT INTO users (username, email) 
                        VALUES ($1, $2)
                        ON CONFLICT (username) DO NOTHING
                    """, username, email)
                except Exception as e:
                    logger.warning(f"Failed to insert demo user {username}: {e}")
            
            logger.info("Demo data setup complete")
            
        finally:
            await conn.close()

async def main():
    """Main initialization function"""
    initializer = ClusterInitializer()
    
    try:
        await initializer.initialize_cluster()
        logger.info("Cluster initialization successful!")
        return 0
    except Exception as e:
        logger.error(f"Cluster initialization failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)