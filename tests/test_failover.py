#!/usr/bin/env python3
"""
SynapseDB Failover Testing
Tests node failures and automatic failover mechanisms
"""

import asyncio
import asyncpg
import logging
import time
import json
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FailoverTester:
    """Tests failover scenarios for SynapseDB cluster"""
    
    def __init__(self):
        self.nodes = {
            'node1': {'host': '172.20.0.10', 'port': 5432},
            'node2': {'host': '172.20.0.11', 'port': 5432}, 
            'node3': {'host': '172.20.0.12', 'port': 5432},
        }
        self.db_config = {
            'database': 'synapsedb',
            'user': 'synapsedb',
            'password': 'changeme123'
        }
        self.connections = {}
    
    async def connect_to_nodes(self):
        """Connect to all available nodes"""
        for node_name, node_config in self.nodes.items():
            try:
                conn = await asyncpg.connect(
                    host=node_config['host'],
                    port=node_config['port'],
                    **self.db_config,
                    command_timeout=5
                )
                self.connections[node_name] = conn
                logger.info(f"Connected to {node_name}")
            except Exception as e:
                logger.warning(f"Failed to connect to {node_name}: {e}")
    
    async def get_cluster_status(self) -> Dict:
        """Get current cluster status from any available node"""
        for node_name, conn in self.connections.items():
            try:
                query = """
                    SELECT 
                        node_name, host, port, status, is_writer, 
                        last_heartbeat, 
                        (NOW() - last_heartbeat) < INTERVAL '30 seconds' as is_healthy
                    FROM cluster_nodes 
                    ORDER BY is_writer DESC, node_name
                """
                
                result = await conn.fetch(query)
                
                nodes = []
                writer_node = None
                healthy_count = 0
                
                for row in result:
                    node_info = dict(row)
                    nodes.append(node_info)
                    
                    if node_info['is_writer'] and node_info['is_healthy']:
                        writer_node = node_info
                    
                    if node_info['is_healthy']:
                        healthy_count += 1
                
                # Check quorum
                quorum_result = await conn.fetchval("SELECT check_quorum()")
                
                return {
                    'nodes': nodes,
                    'writer_node': writer_node,
                    'healthy_nodes': healthy_count,
                    'total_nodes': len(nodes),
                    'has_quorum': quorum_result,
                    'responding_node': node_name
                }
                
            except Exception as e:
                logger.warning(f"Failed to get status from {node_name}: {e}")
                continue
        
        return {'error': 'No nodes responding'}
    
    async def simulate_node_failure(self, node_name: str) -> bool:
        """Simulate node failure by marking it as failed"""
        logger.info(f"Simulating failure of {node_name}")
        
        # Try to mark the node as failed from other nodes
        for other_node, conn in self.connections.items():
            if other_node != node_name:
                try:
                    await conn.execute(
                        "UPDATE cluster_nodes SET status = 'failed', updated_at = NOW() WHERE node_name = $1",
                        node_name
                    )
                    logger.info(f"Marked {node_name} as failed from {other_node}")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to mark {node_name} as failed from {other_node}: {e}")
        
        return False
    
    async def simulate_node_recovery(self, node_name: str) -> bool:
        """Simulate node recovery by marking it as active"""
        logger.info(f"Simulating recovery of {node_name}")
        
        # Try to mark the node as active
        for other_node, conn in self.connections.items():
            try:
                await conn.execute("""
                    UPDATE cluster_nodes 
                    SET status = 'active', 
                        last_heartbeat = NOW(),
                        updated_at = NOW() 
                    WHERE node_name = $1
                """, node_name)
                logger.info(f"Marked {node_name} as recovered from {other_node}")
                return True
            except Exception as e:
                logger.warning(f"Failed to mark {node_name} as recovered from {other_node}: {e}")
        
        return False
    
    async def test_write_operation(self, description: str) -> bool:
        """Test a write operation"""
        logger.info(f"Testing write: {description}")
        
        test_data = {
            'test_id': int(time.time()),
            'description': description,
            'timestamp': time.time()
        }
        
        # Try to insert test data
        for node_name, conn in self.connections.items():
            try:
                await conn.execute(
                    "INSERT INTO users (username, email, profile_data) VALUES ($1, $2, $3)",
                    f"failover_test_{test_data['test_id']}",
                    f"test_{test_data['test_id']}@example.com",
                    json.dumps(test_data)
                )
                logger.info(f"Write succeeded on {node_name}")
                return True
            except Exception as e:
                logger.warning(f"Write failed on {node_name}: {e}")
                continue
        
        logger.error("Write failed on all nodes")
        return False
    
    async def test_read_consistency(self, test_id: int) -> Dict:
        """Test read consistency across nodes"""
        logger.info(f"Testing read consistency for test_id {test_id}")
        
        results = {}
        
        for node_name, conn in self.connections.items():
            try:
                result = await conn.fetchrow(
                    "SELECT username, email, profile_data FROM users WHERE username = $1",
                    f"failover_test_{test_id}"
                )
                results[node_name] = dict(result) if result else None
            except Exception as e:
                logger.warning(f"Read failed on {node_name}: {e}")
                results[node_name] = {'error': str(e)}
        
        return results
    
    async def test_writer_failover(self):
        """Test writer node failover"""
        logger.info("=== Testing Writer Failover ===")
        
        # Get initial status
        status = await self.get_cluster_status()
        if 'error' in status:
            logger.error("Cannot get cluster status")
            return
        
        initial_writer = status.get('writer_node')
        if not initial_writer:
            logger.error("No initial writer found")
            return
        
        logger.info(f"Initial writer: {initial_writer['node_name']}")
        
        # Perform a write operation
        write_success = await self.test_write_operation("Before failover")
        assert write_success, "Initial write should succeed"
        
        # Simulate writer failure
        await self.simulate_node_failure(initial_writer['node_name'])
        
        # Wait for failover detection
        logger.info("Waiting for failover detection...")
        await asyncio.sleep(15)
        
        # Check new cluster status
        new_status = await self.get_cluster_status()
        new_writer = new_status.get('writer_node')
        
        if new_writer and new_writer['node_name'] != initial_writer['node_name']:
            logger.info(f"Failover successful! New writer: {new_writer['node_name']}")
        else:
            logger.warning("Failover may not have completed yet")
        
        # Test write on new configuration
        write_success = await self.test_write_operation("After failover")
        if write_success:
            logger.info("Write succeeded after failover")
        else:
            logger.warning("Write failed after failover")
        
        # Simulate recovery of failed node
        await self.simulate_node_recovery(initial_writer['node_name'])
        await asyncio.sleep(5)
        
        # Final status
        final_status = await self.get_cluster_status()
        logger.info(f"Final cluster status: {final_status['healthy_nodes']}/{final_status['total_nodes']} nodes healthy")
    
    async def test_quorum_loss(self):
        """Test cluster behavior when quorum is lost"""
        logger.info("=== Testing Quorum Loss ===")
        
        status = await self.get_cluster_status()
        if status['healthy_nodes'] < 3:
            logger.warning("Not enough healthy nodes for quorum test")
            return
        
        # Fail two nodes to lose quorum
        nodes_to_fail = ['node2', 'node3']
        
        for node in nodes_to_fail:
            await self.simulate_node_failure(node)
            await asyncio.sleep(2)
        
        # Check quorum status
        status = await self.get_cluster_status()
        logger.info(f"Quorum status after failures: {status.get('has_quorum')}")
        
        # Try to perform a write (should fail without quorum)
        write_success = await self.test_write_operation("During quorum loss")
        if not write_success:
            logger.info("Write correctly failed due to quorum loss")
        else:
            logger.warning("Write succeeded despite quorum loss (unexpected)")
        
        # Recover one node to restore quorum
        await self.simulate_node_recovery('node2')
        await asyncio.sleep(10)
        
        # Check quorum restoration
        status = await self.get_cluster_status()
        logger.info(f"Quorum status after recovery: {status.get('has_quorum')}")
        
        # Try write again
        write_success = await self.test_write_operation("After quorum restoration")
        if write_success:
            logger.info("Write succeeded after quorum restoration")
        
        # Recover the other node
        await self.simulate_node_recovery('node3')
        await asyncio.sleep(5)
    
    async def test_network_partition(self):
        """Simulate network partition by failing connections"""
        logger.info("=== Testing Network Partition Simulation ===")
        
        # Close connection to one node to simulate network partition
        if 'node2' in self.connections:
            await self.connections['node2'].close()
            del self.connections['node2']
            logger.info("Simulated network partition: disconnected from node2")
        
        # Test operations with reduced connectivity
        write_success = await self.test_write_operation("During network partition")
        logger.info(f"Write during partition: {'success' if write_success else 'failed'}")
        
        # Try to reconnect (simulate network healing)
        try:
            conn = await asyncpg.connect(
                host=self.nodes['node2']['host'],
                port=self.nodes['node2']['port'],
                **self.db_config
            )
            self.connections['node2'] = conn
            logger.info("Network partition healed: reconnected to node2")
        except Exception as e:
            logger.warning(f"Failed to reconnect to node2: {e}")
    
    async def test_cascading_failures(self):
        """Test multiple sequential node failures"""
        logger.info("=== Testing Cascading Failures ===")
        
        # Fail nodes one by one
        failure_order = ['node3', 'node2']
        
        for node in failure_order:
            logger.info(f"Failing {node}")
            await self.simulate_node_failure(node)
            
            # Check cluster status after each failure
            status = await self.get_cluster_status()
            logger.info(f"After failing {node}: {status['healthy_nodes']}/{status['total_nodes']} nodes, "
                       f"quorum: {status.get('has_quorum')}")
            
            # Test write capability
            write_success = await self.test_write_operation(f"After {node} failure")
            logger.info(f"Write capability: {'available' if write_success else 'unavailable'}")
            
            await asyncio.sleep(5)
        
        # Recovery sequence
        recovery_order = ['node2', 'node3']
        
        for node in recovery_order:
            logger.info(f"Recovering {node}")
            await self.simulate_node_recovery(node)
            
            # Check cluster status after each recovery
            status = await self.get_cluster_status()
            logger.info(f"After recovering {node}: {status['healthy_nodes']}/{status['total_nodes']} nodes, "
                       f"quorum: {status.get('has_quorum')}")
            
            await asyncio.sleep(5)
    
    async def test_data_consistency_after_failover(self):
        """Test data consistency after failover events"""
        logger.info("=== Testing Data Consistency After Failover ===")
        
        # Insert test data
        test_operations = []
        for i in range(5):
            test_id = int(time.time()) + i
            success = await self.test_write_operation(f"Consistency test {i}")
            if success:
                test_operations.append(test_id)
        
        # Simulate failover
        status = await self.get_cluster_status()
        writer_node = status.get('writer_node')
        if writer_node:
            await self.simulate_node_failure(writer_node['node_name'])
            await asyncio.sleep(10)  # Wait for failover
            await self.simulate_node_recovery(writer_node['node_name'])
            await asyncio.sleep(10)  # Wait for recovery
        
        # Check data consistency across nodes
        logger.info("Checking data consistency across nodes...")
        for test_id in test_operations:
            consistency_results = await self.test_read_consistency(test_id)
            
            # Compare results across nodes
            values = [result for result in consistency_results.values() 
                     if result and 'error' not in result]
            
            if len(set(str(v) for v in values)) == 1:
                logger.info(f"Test {test_id}: Consistent across all nodes")
            else:
                logger.warning(f"Test {test_id}: Inconsistent data detected")
                for node_name, result in consistency_results.items():
                    logger.warning(f"  {node_name}: {result}")
    
    async def cleanup_test_data(self):
        """Clean up test data"""
        logger.info("Cleaning up test data...")
        
        for node_name, conn in self.connections.items():
            try:
                await conn.execute(
                    "DELETE FROM users WHERE username LIKE 'failover_test_%'"
                )
                logger.info(f"Cleaned up test data from {node_name}")
                break  # Only need to clean from one node due to replication
            except Exception as e:
                logger.warning(f"Failed to cleanup from {node_name}: {e}")
    
    async def close_connections(self):
        """Close all connections"""
        for node_name, conn in self.connections.items():
            try:
                await conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection to {node_name}: {e}")
    
    async def run_all_tests(self):
        """Run all failover tests"""
        try:
            logger.info("Starting SynapseDB Failover Tests")
            
            await self.connect_to_nodes()
            
            # Run test suites
            await self.test_writer_failover()
            await self.test_quorum_loss()
            await self.test_network_partition()
            await self.test_cascading_failures()
            await self.test_data_consistency_after_failover()
            
            logger.info("✅ All failover tests completed!")
            
        except Exception as e:
            logger.error(f"❌ Failover test failed: {e}")
            raise
        finally:
            await self.cleanup_test_data()
            await self.close_connections()

async def main():
    """Main entry point"""
    tester = FailoverTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main())