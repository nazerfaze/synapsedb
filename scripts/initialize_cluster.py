#!/usr/bin/env python3
"""
SynapseDB Cluster Initialization Script
Bootstraps a 3-node PostgreSQL cluster with distributed capabilities
"""

import asyncio
import asyncpg
import logging
import os
import time
import yaml
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClusterInitializer:
    """Initializes the SynapseDB cluster"""
    
    def __init__(self):
        self.nodes = [
            {'name': 'node1', 'host': '172.20.0.10', 'port': 5432},
            {'name': 'node2', 'host': '172.20.0.11', 'port': 5432},
            {'name': 'node3', 'host': '172.20.0.12', 'port': 5432},
        ]
        self.db_config = {
            'database': 'synapsedb',
            'user': 'synapsedb',
            'password': 'changeme123'
        }
        self.connections = {}
    
    async def connect_to_nodes(self):
        """Establish connections to all nodes"""
        logger.info("Connecting to cluster nodes...")
        
        for node in self.nodes:
            max_retries = 30  # Wait up to 5 minutes for each node
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    conn = await asyncpg.connect(
                        host=node['host'],
                        port=node['port'],
                        **self.db_config,
                        command_timeout=10
                    )
                    
                    # Test connection
                    await conn.fetchval('SELECT 1')
                    self.connections[node['name']] = conn
                    logger.info(f"Connected to {node['name']} at {node['host']}:{node['port']}")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Failed to connect to {node['name']} (attempt {retry_count}/{max_retries}): {e}")
                    await asyncio.sleep(10)
            
            if node['name'] not in self.connections:
                raise Exception(f"Could not connect to {node['name']} after {max_retries} attempts")
    
    async def install_extensions(self):
        """Install required extensions on all nodes"""
        logger.info("Installing extensions on all nodes...")
        
        extensions = [
            'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"',
            'CREATE EXTENSION IF NOT EXISTS "pgcrypto"',
            'CREATE EXTENSION IF NOT EXISTS "dblink"',
            # Note: pglogical and vector would need to be available in the PostgreSQL image
            # 'CREATE EXTENSION IF NOT EXISTS "pglogical"',
            # 'CREATE EXTENSION IF NOT EXISTS "vector"',
        ]
        
        for node_name, conn in self.connections.items():
            logger.info(f"Installing extensions on {node_name}...")
            for ext_sql in extensions:
                try:
                    await conn.execute(ext_sql)
                    logger.debug(f"Executed on {node_name}: {ext_sql}")
                except Exception as e:
                    logger.warning(f"Extension installation failed on {node_name}: {e}")
    
    async def execute_sql_file(self, conn, file_path: str, node_name: str):
        """Execute SQL commands from a file"""
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by statements and execute
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for stmt in statements:
                if stmt and not stmt.startswith('--'):
                    try:
                        await conn.execute(stmt)
                    except Exception as e:
                        logger.warning(f"Failed to execute statement on {node_name}: {stmt[:100]}... Error: {e}")
            
            logger.info(f"Successfully executed {file_path} on {node_name}")
            
        except Exception as e:
            logger.error(f"Failed to execute {file_path} on {node_name}: {e}")
    
    async def setup_cluster_schema(self):
        """Set up cluster metadata schema on all nodes"""
        logger.info("Setting up cluster schema...")
        
        sql_files = [
            '/app/sql/cluster_setup.sql',
            '/app/sql/replication_setup.sql', 
            '/app/sql/ddl_triggers.sql',
            '/app/sql/conflict_resolution.sql',
            '/app/sql/quorum_writes.sql'
        ]
        
        for node_name, conn in self.connections.items():
            logger.info(f"Setting up schema on {node_name}...")
            
            # Set node-specific configurations
            await conn.execute("SELECT set_config('synapsedb.node_name', $1, false)", node_name)
            await conn.execute("SELECT set_config('synapsedb.node_id', $1, false)", 
                             f"00000000-0000-0000-0000-00000000000{node_name[-1]}")
            await conn.execute("SELECT set_config('synapsedb.host', $1, false)", 
                             next(n['host'] for n in self.nodes if n['name'] == node_name))
            await conn.execute("SELECT set_config('synapsedb.port', '5432', false)")
            
            for sql_file in sql_files:
                if os.path.exists(sql_file):
                    await self.execute_sql_file(conn, sql_file, node_name)
                else:
                    logger.warning(f"SQL file not found: {sql_file}")
    
    async def register_cluster_nodes(self):
        """Register all nodes in the cluster metadata"""
        logger.info("Registering cluster nodes...")
        
        # Register nodes in node1 first (it will be the initial writer)
        node1_conn = self.connections['node1']
        
        for i, node in enumerate(self.nodes, 1):
            node_id = f"00000000-0000-0000-0000-00000000000{i}"
            is_writer = (node['name'] == 'node1')  # node1 is initial writer
            
            await node1_conn.execute("""
                INSERT INTO cluster_nodes (node_id, node_name, host, port, status, is_writer, version)
                VALUES ($1, $2, $3, $4, 'active', $5, '1.0.0')
                ON CONFLICT (node_name) DO UPDATE SET
                    host = EXCLUDED.host,
                    port = EXCLUDED.port,
                    status = 'active',
                    is_writer = EXCLUDED.is_writer,
                    last_heartbeat = NOW(),
                    updated_at = NOW()
            """, node_id, node['name'], node['host'], node['port'], is_writer)
        
        logger.info("Cluster nodes registered successfully")
        
        # Replicate node registrations to other nodes
        for node_name, conn in self.connections.items():
            if node_name != 'node1':
                for i, node in enumerate(self.nodes, 1):
                    node_id = f"00000000-0000-0000-0000-00000000000{i}"
                    is_writer = (node['name'] == 'node1')
                    
                    await conn.execute("""
                        INSERT INTO cluster_nodes (node_id, node_name, host, port, status, is_writer, version)
                        VALUES ($1, $2, $3, $4, 'active', $5, '1.0.0')
                        ON CONFLICT (node_name) DO UPDATE SET
                            host = EXCLUDED.host,
                            port = EXCLUDED.port,
                            status = 'active',
                            is_writer = EXCLUDED.is_writer,
                            last_heartbeat = NOW(),
                            updated_at = NOW()
                    """, node_id, node['name'], node['host'], node['port'], is_writer)
    
    async def setup_sample_tables(self):
        """Create sample tables with vector support"""
        logger.info("Creating sample tables...")
        
        sample_tables = [
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                profile_data JSONB DEFAULT '{}',
                last_updated_at TIMESTAMPTZ DEFAULT NOW(),
                updated_by_node UUID,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                price DECIMAL(10,2),
                category TEXT,
                metadata JSONB DEFAULT '{}',
                last_updated_at TIMESTAMPTZ DEFAULT NOW(),
                updated_by_node UUID,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS documents (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                -- embedding VECTOR(1536), -- Uncomment when pgvector is available
                tags TEXT[],
                metadata JSONB DEFAULT '{}',
                last_updated_at TIMESTAMPTZ DEFAULT NOW(),
                updated_by_node UUID,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        ]
        
        # Create tables on node1 first
        node1_conn = self.connections['node1']
        for table_sql in sample_tables:
            await node1_conn.execute(table_sql)
        
        logger.info("Sample tables created on node1")
        
        # Replicate to other nodes
        for node_name, conn in self.connections.items():
            if node_name != 'node1':
                for table_sql in sample_tables:
                    await conn.execute(table_sql)
        
        logger.info("Sample tables replicated to all nodes")
    
    async def insert_sample_data(self):
        """Insert sample data for testing"""
        logger.info("Inserting sample data...")
        
        node1_conn = self.connections['node1']
        
        # Insert sample users
        await node1_conn.execute("""
            INSERT INTO users (username, email, profile_data) VALUES
            ('alice', 'alice@example.com', '{"role": "admin", "preferences": {"theme": "dark"}}'),
            ('bob', 'bob@example.com', '{"role": "user", "preferences": {"theme": "light"}}'),
            ('charlie', 'charlie@example.com', '{"role": "user", "preferences": {"theme": "auto"}}')
            ON CONFLICT (username) DO NOTHING
        """)
        
        # Insert sample products
        await node1_conn.execute("""
            INSERT INTO products (name, description, price, category, metadata) VALUES
            ('Laptop', 'High-performance laptop', 999.99, 'electronics', '{"brand": "TechCorp", "warranty": "2 years"}'),
            ('Mouse', 'Wireless mouse', 29.99, 'electronics', '{"brand": "TechCorp", "warranty": "1 year"}'),
            ('Keyboard', 'Mechanical keyboard', 79.99, 'electronics', '{"brand": "TechCorp", "warranty": "2 years"}')
            ON CONFLICT DO NOTHING
        """)
        
        # Insert sample documents
        await node1_conn.execute("""
            INSERT INTO documents (title, content, tags, metadata) VALUES
            ('Getting Started Guide', 'This guide will help you get started with SynapseDB...', 
             ARRAY['guide', 'documentation'], '{"author": "SynapseDB Team", "version": "1.0"}'),
            ('API Reference', 'Complete API reference for SynapseDB distributed operations...', 
             ARRAY['api', 'reference'], '{"author": "SynapseDB Team", "version": "1.0"}'),
            ('Troubleshooting', 'Common issues and their solutions...', 
             ARRAY['troubleshooting', 'help'], '{"author": "SynapseDB Team", "version": "1.0"}')
            ON CONFLICT DO NOTHING
        """)
        
        logger.info("Sample data inserted successfully")
    
    async def verify_cluster_health(self):
        """Verify that the cluster is healthy and operational"""
        logger.info("Verifying cluster health...")
        
        for node_name, conn in self.connections.items():
            try:
                # Check basic connectivity
                result = await conn.fetchval('SELECT 1')
                assert result == 1
                
                # Check cluster metadata
                nodes_count = await conn.fetchval('SELECT COUNT(*) FROM cluster_nodes')
                assert nodes_count == 3
                
                # Check quorum
                has_quorum = await conn.fetchval('SELECT check_quorum()')
                assert has_quorum is True
                
                # Check sample data
                users_count = await conn.fetchval('SELECT COUNT(*) FROM users')
                products_count = await conn.fetchval('SELECT COUNT(*) FROM products')
                docs_count = await conn.fetchval('SELECT COUNT(*) FROM documents')
                
                logger.info(f"{node_name}: {nodes_count} nodes, quorum: {has_quorum}, "
                          f"data: {users_count} users, {products_count} products, {docs_count} docs")
                
            except Exception as e:
                logger.error(f"Health check failed for {node_name}: {e}")
                raise
        
        logger.info("Cluster health verification completed successfully!")
    
    async def close_connections(self):
        """Close all database connections"""
        for node_name, conn in self.connections.items():
            try:
                await conn.close()
                logger.debug(f"Closed connection to {node_name}")
            except Exception as e:
                logger.warning(f"Error closing connection to {node_name}: {e}")
    
    async def initialize(self):
        """Main initialization process"""
        try:
            logger.info("Starting SynapseDB cluster initialization...")
            
            await self.connect_to_nodes()
            await self.install_extensions()
            await self.setup_cluster_schema()
            await self.register_cluster_nodes()
            await self.setup_sample_tables()
            await self.insert_sample_data()
            await self.verify_cluster_health()
            
            logger.info("✅ SynapseDB cluster initialization completed successfully!")
            logger.info("🎉 Your distributed database cluster is ready!")
            logger.info("")
            logger.info("Connection endpoints:")
            logger.info("  - Writer (HAProxy): postgres://synapsedb:changeme123@localhost:5000/synapsedb")
            logger.info("  - Reader (HAProxy): postgres://synapsedb:changeme123@localhost:5001/synapsedb")
            logger.info("  - Query Router: http://localhost:8080")
            logger.info("  - Node 1: postgres://synapsedb:changeme123@localhost:5432/synapsedb")
            logger.info("  - Node 2: postgres://synapsedb:changeme123@localhost:5433/synapsedb")
            logger.info("  - Node 3: postgres://synapsedb:changeme123@localhost:5434/synapsedb")
            logger.info("")
            logger.info("Monitoring:")
            logger.info("  - HAProxy Stats: http://localhost:8404/stats")
            logger.info("  - Grafana: http://localhost:3000 (admin/admin123)")
            logger.info("  - Prometheus: http://localhost:9090")
            
        except Exception as e:
            logger.error(f"❌ Cluster initialization failed: {e}")
            raise
        finally:
            await self.close_connections()

async def main():
    """Main entry point"""
    initializer = ClusterInitializer()
    await initializer.initialize()

if __name__ == "__main__":
    asyncio.run(main())