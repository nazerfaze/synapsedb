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
            'host': os.getenv('DB_HOST', 'postgres1'),
            'port': int(os.getenv('DB_PORT', '5432')),
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
        
        # Setup pglogical nodes (subscriptions will be created automatically by replication manager)
        await self._setup_pglogical_nodes()
        
        # Initialize cluster metadata
        await self._initialize_metadata()
        
        # Setup demo data (optional)
        await self._setup_demo_data()
        
        logger.info("SynapseDB cluster initialization complete!")
    
    async def _wait_for_nodes(self, timeout=300):
        """Wait for local database to be ready"""
        logger.info("Waiting for local database to be ready...")

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Test connection to local database
                conn = await asyncpg.connect(**self.db_config, command_timeout=5)
                result = await conn.fetchval("SELECT 1")
                await conn.close()

                if result == 1:
                    logger.info("Local database is ready!")
                    return

            except Exception as e:
                logger.debug(f"Local database not ready: {e}")

            logger.info("Local database not ready, waiting...")
            await asyncio.sleep(5)
        
        raise TimeoutError(f"Nodes not ready within {timeout} seconds")
    
    async def _install_extensions(self):
        """Install required PostgreSQL extensions on local database"""
        logger.info("Installing PostgreSQL extensions...")

        extensions = [
            'pglogical',
            'uuid-ossp',
            'btree_gist'
        ]

        # Try to install pgvector (optional)
        optional_extensions = ['vector']

        logger.info("Installing extensions on local database")

        # Connect to local database
        conn = await asyncpg.connect(**self.db_config)

        try:
            # Install required extensions
            for ext in extensions:
                try:
                    await conn.execute(f"CREATE EXTENSION IF NOT EXISTS \"{ext}\"")
                    logger.info(f"Installed {ext} on local database")
                except Exception as e:
                    logger.error(f"Failed to install {ext} on local database: {e}")
                    raise

            # Install optional extensions
            for ext in optional_extensions:
                try:
                    await conn.execute(f"CREATE EXTENSION IF NOT EXISTS \"{ext}\"")
                    logger.info(f"Installed {ext} on local database")
                except Exception as e:
                    logger.warning(f"Optional extension {ext} not available on local database: {e}")

        finally:
            await conn.close()
    
    async def _create_system_schemas(self):
        """Create system schemas and tables"""
        logger.info("Creating system schemas...")

        # Create schemas on the local database
        conn = await asyncpg.connect(**self.db_config)
        
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

            # Create Raft state table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS synapsedb_consensus.raft_state (
                    node_id VARCHAR(64) PRIMARY KEY,
                    current_term BIGINT DEFAULT 0,
                    voted_for VARCHAR(64),
                    commit_index BIGINT DEFAULT 0,
                    last_applied BIGINT DEFAULT 0,
                    role VARCHAR(32) DEFAULT 'follower',
                    leader_id VARCHAR(64),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Create Raft log table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS synapsedb_consensus.raft_log (
                    log_index BIGINT PRIMARY KEY,
                    term BIGINT NOT NULL,
                    command_type VARCHAR(64) NOT NULL,
                    command_data JSONB NOT NULL,
                    applied BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
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
    
    async def _setup_pglogical_nodes(self):
        """Setup pglogical nodes only (subscriptions will be created automatically by replication manager)"""
        logger.info("Setting up pglogical nodes for auto-discovery...")

        # Only setup the local database node (other nodes will be discovered automatically)
        logger.info(f"Setting up pglogical node on local database")

        conn = await asyncpg.connect(**self.db_config)

        try:
            # Create pglogical node
            node_dsn = f"host={self.db_config['host']} port={self.db_config['port']} dbname={self.db_config['database']} user={self.db_config['user']} password={self.db_config['password']}"

            try:
                await conn.execute(
                    "SELECT pglogical.create_node(node_name := $1, dsn := $2)",
                    f"{self.db_config['host']}", node_dsn
                )
                logger.info(f"pglogical node created on local database")
            except Exception as e:
                if "already exists" in str(e) or "already configured" in str(e):
                    logger.info(f"pglogical node already exists on local database")
                else:
                    logger.error(f"Failed to create pglogical node on local database: {e}")
                    raise

            # Create default replication set
            try:
                await conn.execute(
                    "SELECT pglogical.create_replication_set($1, $2, $3, $4, $5)",
                    f'synapsedb_default_{self.db_config["host"]}', True, True, True, True
                )
                logger.info(f"Created replication set synapsedb_default_{self.db_config['host']} on local database")
            except Exception as e:
                if "already exists" in str(e):
                    logger.info(f"Replication set synapsedb_default_{self.db_config['host']} already exists on local database")
                else:
                    logger.error(f"Failed to create replication set on local database: {e}")
                    # Don't raise - continue

        except Exception as e:
            logger.error(f"Failed to setup pglogical on local database: {e}")
            raise
        finally:
            await conn.close()

        logger.info("Pglogical nodes setup complete. Subscriptions will be created automatically by replication manager via auto-discovery.")
    
    
    async def _initialize_metadata(self):
        """Initialize cluster metadata"""
        logger.info("Initializing cluster metadata...")

        # Initialize metadata on the local database
        conn = await asyncpg.connect(**self.db_config)
        
        try:
            # Initialize Raft state
            await conn.execute("""
                INSERT INTO synapsedb_consensus.raft_state
                (node_id, current_term, voted_for, role, leader_id)
                VALUES ($1, 0, NULL, 'follower', NULL)
                ON CONFLICT (node_id) DO NOTHING
            """, self.cluster_nodes[0]['id'])
            
            # Initialize shard configuration
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS synapsedb_sharding.shard_assignments (
                    shard_id INTEGER PRIMARY KEY,
                    primary_node_id VARCHAR(64) NOT NULL,
                    replica_node_ids TEXT DEFAULT '[]',
                    status VARCHAR(32) DEFAULT 'active',
                    key_range_start VARCHAR(64),
                    key_range_end VARCHAR(64),
                    table_name VARCHAR(128),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Initialize basic shard distribution
            shard_count = 256
            for i in range(shard_count):
                node_index = i % len(self.cluster_nodes)
                assigned_node = self.cluster_nodes[node_index]['id']
                
                await conn.execute("""
                    INSERT INTO synapsedb_sharding.shard_assignments
                    (shard_id, primary_node_id, replica_node_ids, status, key_range_start, key_range_end, table_name)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (shard_id) DO NOTHING
                """, i, assigned_node, '[]', 'active', str(i), str(i), None)
            
            logger.info(f"Initialized {shard_count} shards across {len(self.cluster_nodes)} nodes")
            
        finally:
            await conn.close()
    
    async def _setup_demo_data(self):
        """Setup demo tables and data"""
        logger.info("Setting up demo data...")

        # Setup demo data on the local database
        conn = await asyncpg.connect(**self.db_config)
        
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
                        f'synapsedb_default_{self.db_config[\"host\"]}', f'public.{table}'
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