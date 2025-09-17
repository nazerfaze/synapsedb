#!/usr/bin/env python3
"""
SynapseDB Cluster Node Service
Main service that orchestrates all distributed database components
"""

import asyncio
import logging
import os
import signal
import sys
from typing import Dict, List

# Import all service components
from raft_consensus import RaftNode
from replication_manager import ReplicationManager  
from sharding_manager import ShardingManager
from transaction_coordinator import TransactionCoordinator
from schema_manager import SchemaManager
from gossip_protocol import GossipProtocol
from vector_clock import CausalConsistencyManager
from deadlock_detector import DistributedDeadlockDetector
from connection_pool import ClusterAwareConnectionPool
from tls_manager import TLSManager
from backup_manager import BackupCoordinator
from rolling_update_manager import RollingUpdateManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/app/logs/cluster_node.log')
    ]
)
logger = logging.getLogger(__name__)

class ClusterNode:
    """Main cluster node service orchestrating all components"""
    
    def __init__(self):
        self.node_id = os.getenv('NODE_ID', 'node1')
        self.running = False
        self.services = {}
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'synapsedb'),
            'user': os.getenv('DB_USER', 'synapsedb'),
            'password': os.getenv('DB_PASSWORD', 'synapsedb')
        }
        
        # Parse cluster nodes
        self.cluster_nodes = self._parse_cluster_nodes()
        
        # Service configurations
        self.service_configs = self._get_service_configs()
    
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
    
    def _get_service_configs(self) -> Dict:
        """Get configuration for all services"""
        return {
            'tls': {
                'cluster_nodes': self.cluster_nodes,
                'certs_dir': '/app/certs',
                'cert_validity_days': 365,
                'auto_renewal': True
            },
            'backup': {
                'type': 'local',
                'path': '/app/data/backups',
                'retention_days': 7,
                'wal_archive': True
            },
            'connection_pool': {
                'min_size': 5,
                'max_size': 20,
                'command_timeout': 60.0
            }
        }
    
    async def initialize(self):
        """Initialize all cluster services"""
        logger.info(f"Initializing cluster node {self.node_id}")
        
        try:
            # Initialize TLS Manager first (needed for secure communication)
            logger.info("Initializing TLS Manager")
            tls_manager = TLSManager(self.service_configs['tls'])
            await tls_manager.initialize()
            await tls_manager.generate_cluster_certificates()
            self.services['tls_manager'] = tls_manager
            
            # Initialize core database services
            logger.info("Initializing Raft Consensus")
            raft_node = RaftNode(self.node_id, self.cluster_nodes, self.db_config)
            await raft_node.initialize()
            self.services['raft'] = raft_node
            
            logger.info("Initializing Replication Manager")
            repl_manager = ReplicationManager(self.node_id, self.cluster_nodes, self.db_config)
            await repl_manager.initialize()
            self.services['replication'] = repl_manager
            
            logger.info("Initializing Sharding Manager")
            shard_manager = ShardingManager(self.node_id, self.cluster_nodes, self.db_config)
            await shard_manager.initialize()
            self.services['sharding'] = shard_manager
            
            logger.info("Initializing Transaction Coordinator")
            txn_coordinator = TransactionCoordinator(self.node_id, self.db_config, self.cluster_nodes)
            await txn_coordinator.initialize()
            self.services['transaction'] = txn_coordinator
            
            logger.info("Initializing Schema Manager")
            schema_manager = SchemaManager(self.node_id, self.cluster_nodes, self.db_config, raft_node)
            await schema_manager.initialize()
            self.services['schema'] = schema_manager
            
            # Initialize advanced services
            logger.info("Initializing Gossip Protocol")
            gossip = GossipProtocol(self.node_id, self.cluster_nodes)
            await gossip.initialize()
            self.services['gossip'] = gossip
            
            logger.info("Initializing Vector Clock System")
            vector_clocks = CausalConsistencyManager(self.node_id, [n['id'] for n in self.cluster_nodes])
            self.services['vector_clocks'] = vector_clocks
            
            logger.info("Initializing Deadlock Detector")
            deadlock_detector = DistributedDeadlockDetector(self.node_id, self.db_config, self.cluster_nodes)
            await deadlock_detector.initialize()
            self.services['deadlock'] = deadlock_detector
            
            # Initialize operational services
            logger.info("Initializing Backup Manager")
            backup_manager = BackupCoordinator(self.node_id, self.cluster_nodes, 
                                             self.service_configs['backup'], self.db_config)
            await backup_manager.initialize()
            self.services['backup'] = backup_manager
            
            logger.info("Initializing Rolling Update Manager")
            update_manager = RollingUpdateManager(self.node_id, self.cluster_nodes, self.db_config)
            await update_manager.initialize()
            self.services['updates'] = update_manager
            
            logger.info(f"All services initialized for node {self.node_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize cluster node: {e}")
            raise
    
    async def start(self):
        """Start all cluster services"""
        logger.info(f"Starting cluster node {self.node_id}")
        self.running = True
        
        # Start all services concurrently
        tasks = []
        
        for service_name, service in self.services.items():
            if hasattr(service, 'start'):
                logger.info(f"Starting {service_name}")
                task = asyncio.create_task(service.start())
                tasks.append(task)
        
        try:
            # Wait for all services to run
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in cluster node services: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop all cluster services"""
        logger.info(f"Stopping cluster node {self.node_id}")
        self.running = False
        
        # Stop all services
        for service_name, service in self.services.items():
            if hasattr(service, 'stop'):
                try:
                    logger.info(f"Stopping {service_name}")
                    await service.stop()
                except Exception as e:
                    logger.error(f"Error stopping {service_name}: {e}")
        
        logger.info(f"Cluster node {self.node_id} stopped")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

async def main():
    """Main entry point"""
    # Create and initialize cluster node
    node = ClusterNode()
    node.setup_signal_handlers()
    
    try:
        await node.initialize()
        await node.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Cluster node failed: {e}")
        return 1
    finally:
        await node.stop()
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)