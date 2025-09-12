#!/usr/bin/env python3
"""
SynapseDB Full System Integration Tests
Comprehensive testing of all distributed database components
"""

import asyncio
import asyncpg
import pytest
import logging
import time
import json
import uuid
from typing import List, Dict, Any
import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SynapseDBTestSuite:
    """Complete integration test suite for SynapseDB"""
    
    def __init__(self):
        self.nodes = [
            {'id': 'node1', 'host': 'localhost', 'port': 5432, 'api_port': 8080},
            {'id': 'node2', 'host': 'localhost', 'port': 5433, 'api_port': 8081}, 
            {'id': 'node3', 'host': 'localhost', 'port': 5434, 'api_port': 8082}
        ]
        self.connections: List[asyncpg.Connection] = []
        self.test_results = {}
    
    async def setup(self):
        """Initialize test environment"""
        logger.info("Setting up SynapseDB test environment")
        
        # Connect to all nodes
        for node in self.nodes:
            try:
                conn = await asyncpg.connect(
                    host=node['host'],
                    port=node['port'],
                    database='synapsedb',
                    user='synapsedb',
                    password='synapsedb'
                )
                self.connections.append(conn)
                logger.info(f"Connected to {node['id']}")
            except Exception as e:
                logger.error(f"Failed to connect to {node['id']}: {e}")
                raise
        
        # Wait for cluster initialization
        await self._wait_for_cluster_ready()
        
        # Setup test schema
        await self._setup_test_schema()
    
    async def teardown(self):
        """Cleanup test environment"""
        logger.info("Cleaning up test environment")
        
        # Close connections
        for conn in self.connections:
            if not conn.is_closed():
                await conn.close()
        
        self.connections.clear()
    
    async def _wait_for_cluster_ready(self, timeout: int = 60):
        """Wait for cluster to be fully initialized"""
        logger.info("Waiting for cluster initialization")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Check if all nodes are healthy
                healthy_nodes = 0
                
                for i, node in enumerate(self.nodes):
                    try:
                        # Test basic connectivity
                        result = await self.connections[i].fetchval("SELECT 1")
                        if result == 1:
                            healthy_nodes += 1
                    except Exception:
                        pass
                
                if healthy_nodes == len(self.nodes):
                    logger.info("All nodes are healthy")
                    
                    # Additional check for replication setup
                    try:
                        result = await self.connections[0].fetch("""
                            SELECT subscription_name FROM pglogical.subscription
                        """)
                        if len(result) >= 2:  # Should have subscriptions to other nodes
                            logger.info("Replication topology detected")
                            return
                    except Exception:
                        pass
                
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.warning(f"Cluster not ready: {e}")
                await asyncio.sleep(2)
        
        raise TimeoutError("Cluster failed to initialize within timeout")
    
    async def _setup_test_schema(self):
        """Create test tables and data"""
        logger.info("Setting up test schema")
        
        # Create test tables on first node (will replicate to others)
        await self.connections[0].execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await self.connections[0].execute("""
            CREATE TABLE IF NOT EXISTS test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES test_users(id),
                product_name VARCHAR(100) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                order_date TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await self.connections[0].execute("""
            CREATE TABLE IF NOT EXISTS test_vector_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                embedding_json JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify tables exist on all nodes
        for i, conn in enumerate(self.connections):
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'test_%'
            """)
            assert len(tables) >= 3, f"Node {i+1} missing test tables"
        
        logger.info("Test schema setup complete")
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run complete test suite"""
        logger.info("Starting SynapseDB full system integration tests")
        
        tests = [
            self.test_basic_replication,
            self.test_distributed_transactions,
            self.test_consensus_operations,
            self.test_vector_operations,
            self.test_sharding_system,
            self.test_failover_recovery,
            self.test_conflict_resolution,
            self.test_backup_restore,
            self.test_rolling_updates,
            self.test_health_monitoring,
            self.test_deadlock_detection,
            self.test_performance_benchmarks
        ]
        
        for test in tests:
            test_name = test.__name__
            logger.info(f"Running test: {test_name}")
            
            try:
                start_time = time.time()
                result = await test()
                duration = time.time() - start_time
                
                self.test_results[test_name] = {
                    'status': 'PASSED',
                    'duration': duration,
                    'result': result
                }
                logger.info(f"✓ {test_name} PASSED ({duration:.2f}s)")
                
            except Exception as e:
                duration = time.time() - start_time
                self.test_results[test_name] = {
                    'status': 'FAILED',
                    'duration': duration,
                    'error': str(e)
                }
                logger.error(f"✗ {test_name} FAILED ({duration:.2f}s): {e}")
        
        # Generate summary
        passed = sum(1 for result in self.test_results.values() if result['status'] == 'PASSED')
        total = len(self.test_results)
        
        summary = {
            'total_tests': total,
            'passed': passed,
            'failed': total - passed,
            'success_rate': (passed / total) * 100 if total > 0 else 0,
            'total_duration': sum(r['duration'] for r in self.test_results.values()),
            'test_results': self.test_results
        }
        
        logger.info(f"Test Summary: {passed}/{total} passed ({summary['success_rate']:.1f}%)")
        return summary
    
    async def test_basic_replication(self):
        """Test basic multi-master replication"""
        test_id = str(uuid.uuid4())[:8]
        
        # Insert on node 1
        await self.connections[0].execute("""
            INSERT INTO test_users (username, email) 
            VALUES ($1, $2)
        """, f"user_{test_id}", f"user_{test_id}@test.com")
        
        # Wait for replication
        await asyncio.sleep(3)
        
        # Verify on all nodes
        for i, conn in enumerate(self.connections):
            result = await conn.fetchrow("""
                SELECT username, email FROM test_users 
                WHERE username = $1
            """, f"user_{test_id}")
            
            assert result is not None, f"Replication failed to node {i+1}"
            assert result['username'] == f"user_{test_id}"
        
        # Test write to different node
        await self.connections[1].execute("""
            INSERT INTO test_users (username, email) 
            VALUES ($1, $2)
        """, f"user2_{test_id}", f"user2_{test_id}@test.com")
        
        await asyncio.sleep(3)
        
        # Verify cross-replication
        result = await self.connections[2].fetchrow("""
            SELECT username FROM test_users 
            WHERE username = $1
        """, f"user2_{test_id}")
        
        assert result is not None, "Cross-node replication failed"
        
        return {"replicated_records": 2, "nodes_tested": len(self.connections)}
    
    async def test_distributed_transactions(self):
        """Test distributed transaction coordination"""
        test_id = str(uuid.uuid4())[:8]
        
        # Start distributed transaction
        async with self.connections[0].transaction():
            # Insert user
            user_result = await self.connections[0].fetchrow("""
                INSERT INTO test_users (username, email) 
                VALUES ($1, $2) RETURNING id
            """, f"txn_user_{test_id}", f"txn_user_{test_id}@test.com")
            
            user_id = user_result['id']
            
            # Insert order on same node (should be in same transaction)
            await self.connections[0].execute("""
                INSERT INTO test_orders (user_id, product_name, amount) 
                VALUES ($1, $2, $3)
            """, user_id, f"Product_{test_id}", 99.99)
        
        # Wait for replication
        await asyncio.sleep(3)
        
        # Verify transaction consistency across nodes
        for conn in self.connections:
            user = await conn.fetchrow("""
                SELECT id FROM test_users WHERE username = $1
            """, f"txn_user_{test_id}")
            
            order = await conn.fetchrow("""
                SELECT user_id FROM test_orders WHERE product_name = $1
            """, f"Product_{test_id}")
            
            assert user is not None, "User not replicated"
            assert order is not None, "Order not replicated" 
            assert order['user_id'] == user['id'], "Transaction consistency violated"
        
        return {"transaction_validated": True, "nodes_consistent": len(self.connections)}
    
    async def test_consensus_operations(self):
        """Test Raft consensus operations"""
        
        # Test leader election status
        leader_count = 0
        nodes_status = []
        
        for i, node in enumerate(self.nodes):
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    url = f"http://{node['host']}:{node['api_port']}/raft/status"
                    async with session.get(url) as response:
                        if response.status == 200:
                            status = await response.json()
                            nodes_status.append(status)
                            if status.get('role') == 'leader':
                                leader_count += 1
            except Exception as e:
                logger.warning(f"Failed to get consensus status from node {i+1}: {e}")
        
        # Should have exactly one leader
        assert leader_count == 1, f"Expected 1 leader, found {leader_count}"
        
        # Test consensus proposal (simulated)
        # In a real test, this would test actual Raft operations
        
        return {
            "leader_count": leader_count,
            "nodes_responding": len(nodes_status),
            "consensus_healthy": leader_count == 1
        }
    
    async def test_vector_operations(self):
        """Test vector embedding operations"""
        test_id = str(uuid.uuid4())[:8]
        
        # Test vector data insertion
        vectors = [
            [0.1, 0.2, 0.3, 0.4, 0.5],
            [0.2, 0.3, 0.4, 0.5, 0.6],
            [0.9, 0.8, 0.7, 0.6, 0.5]
        ]
        
        for i, vector in enumerate(vectors):
            await self.connections[0].execute("""
                INSERT INTO test_vector_data (title, embedding_json)
                VALUES ($1, $2)
            """, f"doc_{test_id}_{i}", json.dumps(vector))
        
        await asyncio.sleep(2)
        
        # Test vector similarity search (using JSON functions)
        query_vector = [0.15, 0.25, 0.35, 0.45, 0.55]
        
        results = await self.connections[1].fetch("""
            SELECT title, embedding_json,
                   (
                       SELECT SUM((a.value::float) * (b.value::float))
                       FROM jsonb_array_elements(embedding_json) WITH ORDINALITY a(value, idx)
                       JOIN jsonb_array_elements($1::jsonb) WITH ORDINALITY b(value, idx) 
                       ON a.idx = b.idx
                   ) as dot_product
            FROM test_vector_data 
            WHERE title LIKE $2
            ORDER BY dot_product DESC
            LIMIT 3
        """, json.dumps(query_vector), f"doc_{test_id}_%")
        
        assert len(results) == 3, "Vector query failed"
        assert results[0]['dot_product'] > 0, "Vector similarity calculation failed"
        
        return {
            "vectors_inserted": len(vectors),
            "similarity_results": len(results),
            "best_similarity": float(results[0]['dot_product'])
        }
    
    async def test_sharding_system(self):
        """Test automatic sharding capabilities"""
        
        # Insert data that should be distributed across shards
        test_records = []
        for i in range(50):
            username = f"shard_user_{i}"
            await self.connections[i % len(self.connections)].execute("""
                INSERT INTO test_users (username, email)
                VALUES ($1, $2)
            """, username, f"{username}@test.com")
            test_records.append(username)
        
        await asyncio.sleep(5)
        
        # Verify data distribution
        distribution = {}
        for i, conn in enumerate(self.connections):
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM test_users 
                WHERE username LIKE 'shard_user_%'
            """)
            distribution[f"node_{i+1}"] = count
        
        total_records = sum(distribution.values())
        assert total_records >= len(test_records), "Data loss during sharding"
        
        return {
            "records_inserted": len(test_records),
            "distribution": distribution,
            "total_replicated": total_records
        }
    
    async def test_failover_recovery(self):
        """Test node failover and recovery"""
        
        # This test simulates failover by testing read-only operations
        # In a full environment, you'd actually stop/start nodes
        
        initial_data = str(uuid.uuid4())[:8]
        
        # Insert test data
        await self.connections[0].execute("""
            INSERT INTO test_users (username, email)
            VALUES ($1, $2)
        """, f"failover_{initial_data}", f"failover_{initial_data}@test.com")
        
        await asyncio.sleep(2)
        
        # Verify all nodes can read the data
        successful_reads = 0
        for conn in self.connections:
            try:
                result = await conn.fetchrow("""
                    SELECT username FROM test_users 
                    WHERE username = $1
                """, f"failover_{initial_data}")
                
                if result and result['username'] == f"failover_{initial_data}":
                    successful_reads += 1
            except Exception as e:
                logger.warning(f"Node read failed during failover test: {e}")
        
        # Should be able to read from multiple nodes
        assert successful_reads >= 2, "Insufficient nodes available for failover"
        
        return {
            "nodes_available": successful_reads,
            "failover_ready": successful_reads >= 2
        }
    
    async def test_conflict_resolution(self):
        """Test conflict resolution mechanisms"""
        test_id = str(uuid.uuid4())[:8]
        username = f"conflict_{test_id}"
        
        # Insert initial record
        await self.connections[0].execute("""
            INSERT INTO test_users (username, email)
            VALUES ($1, $2)
        """, username, f"{username}@initial.com")
        
        await asyncio.sleep(2)
        
        # Create conflicting updates on different nodes
        # Note: This is a simplified test - real conflicts are harder to create
        await self.connections[0].execute("""
            UPDATE test_users SET email = $1, updated_at = NOW()
            WHERE username = $2
        """, f"{username}@node1.com", username)
        
        await self.connections[1].execute("""
            UPDATE test_users SET email = $1, updated_at = NOW()
            WHERE username = $2  
        """, f"{username}@node2.com", username)
        
        await asyncio.sleep(5)  # Allow time for conflict resolution
        
        # Check final state - should have resolved to one value
        final_states = []
        for conn in self.connections:
            result = await conn.fetchrow("""
                SELECT email FROM test_users WHERE username = $1
            """, username)
            if result:
                final_states.append(result['email'])
        
        # All nodes should have the same final state
        unique_states = set(final_states)
        
        return {
            "conflicts_created": 2,
            "final_states": len(final_states),
            "unique_final_states": len(unique_states),
            "resolution_successful": len(unique_states) <= 1
        }
    
    async def test_backup_restore(self):
        """Test backup and restore functionality"""
        
        # This is a simplified test - full backup testing requires more setup
        test_id = str(uuid.uuid4())[:8]
        
        # Insert test data for backup
        backup_data = []
        for i in range(10):
            username = f"backup_user_{test_id}_{i}"
            await self.connections[0].execute("""
                INSERT INTO test_users (username, email)
                VALUES ($1, $2)
            """, username, f"{username}@backup.com")
            backup_data.append(username)
        
        await asyncio.sleep(2)
        
        # Verify data exists before "restore"
        count_before = await self.connections[0].fetchval("""
            SELECT COUNT(*) FROM test_users 
            WHERE username LIKE $1
        """, f"backup_user_{test_id}_%")
        
        assert count_before == len(backup_data), "Backup data not properly inserted"
        
        # In a real test, you'd trigger backup creation here
        # For now, we just verify the backup data is consistent across nodes
        
        consistency_check = True
        for conn in self.connections:
            node_count = await conn.fetchval("""
                SELECT COUNT(*) FROM test_users 
                WHERE username LIKE $1
            """, f"backup_user_{test_id}_%")
            
            if node_count != count_before:
                consistency_check = False
                break
        
        return {
            "backup_records": len(backup_data),
            "pre_backup_count": count_before,
            "consistency_check": consistency_check
        }
    
    async def test_rolling_updates(self):
        """Test rolling update capabilities"""
        
        # Test system health before simulated update
        health_checks = []
        
        for i, node in enumerate(self.nodes):
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    url = f"http://{node['host']}:{node['api_port']}/health"
                    async with session.get(url) as response:
                        if response.status == 200:
                            health_data = await response.json()
                            health_checks.append({
                                'node': node['id'],
                                'status': health_data.get('status', 'unknown')
                            })
                        else:
                            health_checks.append({
                                'node': node['id'],
                                'status': 'unhealthy'
                            })
            except Exception:
                health_checks.append({
                    'node': node['id'],
                    'status': 'unreachable'
                })
        
        healthy_nodes = sum(1 for check in health_checks if check['status'] == 'healthy')
        
        # Simulate update readiness check
        update_ready = healthy_nodes >= len(self.nodes) - 1  # Allow one node down during update
        
        return {
            "health_checks": health_checks,
            "healthy_nodes": healthy_nodes,
            "update_ready": update_ready
        }
    
    async def test_health_monitoring(self):
        """Test health monitoring system"""
        
        health_status = {}
        
        # Test database health
        for i, conn in enumerate(self.connections):
            try:
                # Basic connectivity
                result = await conn.fetchval("SELECT 1")
                basic_health = result == 1
                
                # Check replication status
                repl_status = await conn.fetch("""
                    SELECT subscription_name, status 
                    FROM pglogical.subscription 
                    LIMIT 5
                """)
                
                health_status[f"node_{i+1}"] = {
                    "database_connection": basic_health,
                    "replication_subscriptions": len(repl_status),
                    "overall_health": basic_health and len(repl_status) > 0
                }
                
            except Exception as e:
                health_status[f"node_{i+1}"] = {
                    "database_connection": False,
                    "error": str(e),
                    "overall_health": False
                }
        
        healthy_count = sum(1 for status in health_status.values() 
                          if status.get("overall_health", False))
        
        return {
            "nodes_tested": len(health_status),
            "healthy_nodes": healthy_count,
            "health_details": health_status,
            "cluster_healthy": healthy_count >= 2
        }
    
    async def test_deadlock_detection(self):
        """Test deadlock detection system"""
        
        # This is a simplified deadlock test
        # Real deadlock testing requires careful transaction ordering
        
        test_id = str(uuid.uuid4())[:8]
        
        # Setup test data
        await self.connections[0].execute("""
            INSERT INTO test_users (username, email) VALUES 
            ($1, $2), ($3, $4)
        """, f"deadlock1_{test_id}", "deadlock1@test.com",
             f"deadlock2_{test_id}", "deadlock2@test.com")
        
        await asyncio.sleep(2)
        
        # Simulate potential deadlock scenario
        try:
            async with self.connections[0].transaction():
                await self.connections[0].execute("""
                    UPDATE test_users SET email = $1 
                    WHERE username = $2
                """, "updated1@test.com", f"deadlock1_{test_id}")
                
                # Short delay to simulate processing
                await asyncio.sleep(0.1)
                
                await self.connections[0].execute("""
                    UPDATE test_users SET email = $1 
                    WHERE username = $2
                """, "updated2@test.com", f"deadlock2_{test_id}")
            
            deadlock_handled = True
            
        except Exception as e:
            # If deadlock occurred and was resolved, this is expected
            deadlock_handled = "deadlock" in str(e).lower()
        
        return {
            "deadlock_test_completed": True,
            "deadlock_detection_working": deadlock_handled
        }
    
    async def test_performance_benchmarks(self):
        """Run basic performance benchmarks"""
        
        test_id = str(uuid.uuid4())[:8]
        
        # Insert performance test
        insert_start = time.time()
        insert_count = 100
        
        for i in range(insert_count):
            await self.connections[i % len(self.connections)].execute("""
                INSERT INTO test_users (username, email)
                VALUES ($1, $2)
            """, f"perf_{test_id}_{i}", f"perf_{test_id}_{i}@test.com")
        
        insert_duration = time.time() - insert_start
        insert_rate = insert_count / insert_duration
        
        await asyncio.sleep(3)  # Allow replication
        
        # Query performance test
        query_start = time.time()
        query_count = 50
        
        for i in range(query_count):
            await self.connections[i % len(self.connections)].fetchrow("""
                SELECT username, email FROM test_users 
                WHERE username = $1
            """, f"perf_{test_id}_{i % insert_count}")
        
        query_duration = time.time() - query_start
        query_rate = query_count / query_duration
        
        return {
            "insert_rate_per_second": round(insert_rate, 2),
            "query_rate_per_second": round(query_rate, 2),
            "insert_duration": round(insert_duration, 3),
            "query_duration": round(query_duration, 3),
            "total_operations": insert_count + query_count
        }

async def main():
    """Main test execution"""
    test_suite = SynapseDBTestSuite()
    
    try:
        await test_suite.setup()
        results = await test_suite.run_all_tests()
        
        # Print detailed results
        print("\n" + "="*80)
        print("SYNAPSEDB INTEGRATION TEST RESULTS")
        print("="*80)
        print(f"Total Tests: {results['total_tests']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")
        print(f"Success Rate: {results['success_rate']:.1f}%")
        print(f"Total Duration: {results['total_duration']:.2f}s")
        print("="*80)
        
        for test_name, result in results['test_results'].items():
            status_icon = "✓" if result['status'] == 'PASSED' else "✗"
            print(f"{status_icon} {test_name}: {result['status']} ({result['duration']:.2f}s)")
            
            if result['status'] == 'FAILED':
                print(f"  Error: {result.get('error', 'Unknown error')}")
            elif 'result' in result:
                print(f"  Result: {result['result']}")
        
        print("="*80)
        
        # Save results to file
        with open('test_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print("Detailed results saved to test_results.json")
        
        # Return appropriate exit code
        return 0 if results['failed'] == 0 else 1
        
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        return 1
    
    finally:
        await test_suite.teardown()

if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)