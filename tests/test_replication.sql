-- SynapseDB: Comprehensive Replication Tests
-- Tests for DML replication, conflict resolution, and consistency

-- Test 1: Basic DML Replication
\echo '=== Test 1: Basic DML Replication ==='

-- Insert on writer node
\echo 'Inserting test data on writer node...'
INSERT INTO users (username, email, profile_data) VALUES 
    ('test_user_1', 'test1@example.com', '{"test": true, "created_by": "replication_test"}');

-- Wait for replication (in real scenario, you'd check replication lag)
\echo 'Waiting for replication...'
SELECT pg_sleep(2);

-- Verify data exists on all nodes (would need to connect to each node separately)
\echo 'Verifying data replication...'
SELECT username, email, profile_data->>'test' as is_test_data 
FROM users 
WHERE username = 'test_user_1';

-- Test 2: Concurrent Updates (Conflict Resolution)
\echo '=== Test 2: Conflict Resolution Test ==='

-- Create a test record
INSERT INTO products (name, description, price, category) VALUES 
    ('conflict_test', 'Test product for conflicts', 100.00, 'test')
ON CONFLICT DO NOTHING;

-- Get the ID for updates
\set product_id (SELECT id FROM products WHERE name = 'conflict_test' LIMIT 1)

-- Simulate concurrent updates (normally would be on different nodes)
\echo 'Testing conflict resolution...'

-- Update 1: Change price
UPDATE products 
SET price = 150.00, 
    metadata = jsonb_set(COALESCE(metadata, '{}'), '{update_source}', '"node1"'),
    last_updated_at = NOW()
WHERE name = 'conflict_test';

-- Small delay to simulate near-simultaneous updates
SELECT pg_sleep(0.1);

-- Update 2: Change description (would conflict if simultaneous)
UPDATE products 
SET description = 'Updated description', 
    metadata = jsonb_set(COALESCE(metadata, '{}'), '{update_source}', '"node2"'),
    last_updated_at = NOW()
WHERE name = 'conflict_test';

-- Check the result
SELECT name, price, description, metadata, last_updated_at
FROM products 
WHERE name = 'conflict_test';

-- Check conflict log
SELECT table_name, conflict_type, resolution_method, resolved_at
FROM conflict_log 
WHERE table_name = 'products'
ORDER BY resolved_at DESC 
LIMIT 5;

-- Test 3: Schema Replication (DDL)
\echo '=== Test 3: Schema Replication Test ==='

-- Create a new table (should replicate to all nodes)
\echo 'Creating test table for DDL replication...'
CREATE TABLE test_ddl_replication (
    id SERIAL PRIMARY KEY,
    test_data TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    updated_by_node UUID
);

-- Check DDL queue
SELECT command_tag, object_name, status, executed_at
FROM ddl_replication_queue 
WHERE command_text LIKE '%test_ddl_replication%'
ORDER BY executed_at DESC;

-- Insert test data
INSERT INTO test_ddl_replication (test_data) VALUES 
    ('DDL replication test data');

-- Verify data
SELECT * FROM test_ddl_replication;

-- Test 4: Quorum Write Test
\echo '=== Test 4: Quorum Write Test ==='

-- Test quorum-based write
\echo 'Testing quorum-based write operation...'
SELECT * FROM execute_with_quorum(
    'INSERT INTO users (username, email, profile_data) VALUES (''quorum_test'', ''quorum@example.com'', ''{"quorum_write": true}'')',
    ARRAY['users'],
    ARRAY['{"username": "quorum_test"}'::jsonb]
);

-- Verify the write succeeded
SELECT username, email, profile_data->>'quorum_write' as is_quorum_write
FROM users 
WHERE username = 'quorum_test';

-- Test 5: Node Health and Heartbeat
\echo '=== Test 5: Node Health Test ==='

-- Check cluster status
SELECT node_name, status, is_writer, last_heartbeat, 
       (NOW() - last_heartbeat) as heartbeat_age
FROM cluster_nodes 
ORDER BY node_name;

-- Check quorum status
SELECT check_quorum() as has_quorum;

-- Check active writer
SELECT * FROM get_active_writer();

-- Test 6: Replication Status
\echo '=== Test 6: Replication Status Test ==='

-- Check replication lag
SELECT 
    src.node_name as source_node,
    tgt.node_name as target_node,
    rs.lag_bytes,
    rs.lag_seconds,
    rs.status,
    rs.last_sync_time
FROM replication_status rs
JOIN cluster_nodes src ON src.node_id = rs.source_node_id
JOIN cluster_nodes tgt ON tgt.node_id = rs.target_node_id
ORDER BY src.node_name, tgt.node_name;

-- Test 7: Vector Operations (when pgvector is available)
\echo '=== Test 7: Vector Operations Test ==='

-- This would work with pgvector extension
-- CREATE TABLE IF NOT EXISTS embeddings_test (
--     id SERIAL PRIMARY KEY,
--     content TEXT,
--     embedding VECTOR(3),
--     last_updated_at TIMESTAMPTZ DEFAULT NOW(),
--     updated_by_node UUID
-- );

-- INSERT INTO embeddings_test (content, embedding) VALUES 
--     ('test vector 1', '[1,2,3]'),
--     ('test vector 2', '[4,5,6]'),
--     ('test vector 3', '[7,8,9]');

-- Vector similarity search
-- SELECT content, embedding, (embedding <=> '[1,2,3]') as distance
-- FROM embeddings_test
-- ORDER BY embedding <=> '[1,2,3]'
-- LIMIT 5;

-- Test 8: Failover Simulation
\echo '=== Test 8: Writer Failover Test ==='

-- Get current writer
\echo 'Current writer node:'
SELECT node_name, is_writer FROM cluster_nodes WHERE is_writer = true;

-- Simulate writer failure by promoting another node
\echo 'Simulating writer failover...'
-- In a real scenario, this would be triggered by the cluster manager
-- UPDATE cluster_nodes SET status = 'failed' WHERE is_writer = true;
-- SELECT promote_to_writer('node2');

-- Check new writer
-- SELECT node_name, is_writer FROM cluster_nodes WHERE is_writer = true;

-- Test 9: Distributed Transaction Test
\echo '=== Test 9: Distributed Transaction Test ==='

-- Begin a distributed write
\echo 'Testing distributed transaction...'
SELECT begin_distributed_write(
    'INSERT INTO documents (title, content) VALUES (''Distributed Test'', ''Test document for distributed transactions'')',
    ARRAY['documents'],
    ARRAY['{"title": "Distributed Test"}'::jsonb]
) as transaction_id \gset

\echo 'Transaction ID: :transaction_id'

-- Check transaction status
SELECT transaction_id, status, coordinator_node_id, votes_received
FROM distributed_transactions 
WHERE transaction_id = :'transaction_id';

-- Test 10: Cleanup and Verification
\echo '=== Test 10: Cleanup and Verification ==='

-- Clean up test data
DELETE FROM users WHERE username IN ('test_user_1', 'quorum_test');
DELETE FROM products WHERE name = 'conflict_test';
DELETE FROM documents WHERE title = 'Distributed Test';
DROP TABLE IF EXISTS test_ddl_replication;

-- Verify cleanup
\echo 'Verifying cleanup...'
SELECT 
    (SELECT COUNT(*) FROM users WHERE username LIKE 'test_%' OR username = 'quorum_test') as test_users,
    (SELECT COUNT(*) FROM products WHERE name LIKE '%test%') as test_products,
    (SELECT COUNT(*) FROM documents WHERE title LIKE '%Test%' AND title != 'Getting Started Guide') as test_docs;

-- Final cluster health check
\echo '=== Final Cluster Health Check ==='
SELECT 
    COUNT(*) as total_nodes,
    COUNT(*) FILTER (WHERE status = 'active') as active_nodes,
    COUNT(*) FILTER (WHERE is_writer = true) as writer_nodes,
    check_quorum() as has_quorum
FROM cluster_nodes;

\echo '=== All tests completed! ==='