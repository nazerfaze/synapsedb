-- SynapseDB: Vector Operations Tests
-- Tests for pgvector functionality across distributed nodes
-- Note: Requires pgvector extension to be installed

-- Enable pgvector extension (if available)
\echo '=== Setting up Vector Tests ==='

-- CREATE EXTENSION IF NOT EXISTS vector;

-- Test 1: Vector Table Creation with Replication
\echo '=== Test 1: Vector Table with Distributed Replication ==='

-- Create vector table (using regular columns if vector not available)
CREATE TABLE IF NOT EXISTS document_embeddings (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(id),
    title TEXT NOT NULL,
    content_preview TEXT,
    -- embedding VECTOR(1536),  -- Uncomment when pgvector is available
    embedding_json JSONB,  -- Fallback: store as JSON array
    model_name TEXT DEFAULT 'text-embedding-ada-002',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    updated_by_node UUID
);

-- Insert sample embeddings
\echo 'Inserting sample vector data...'
INSERT INTO document_embeddings (document_id, title, content_preview, embedding_json) VALUES 
(1, 'Getting Started Guide', 'This guide will help you get started...', 
 '[0.1, 0.2, 0.3, 0.4, 0.5, -0.1, -0.2, 0.8, 0.9, 0.1]'),
(2, 'API Reference', 'Complete API reference for SynapseDB...', 
 '[0.2, 0.3, 0.1, 0.6, 0.4, -0.3, -0.1, 0.7, 0.8, 0.2]'),
(3, 'Troubleshooting', 'Common issues and their solutions...', 
 '[0.3, 0.1, 0.4, 0.5, 0.2, -0.2, -0.4, 0.6, 0.7, 0.3]');

-- Test 2: Vector Similarity Search (JSON-based fallback)
\echo '=== Test 2: Vector Similarity Search (JSON fallback) ==='

-- Function to calculate cosine similarity for JSON arrays
CREATE OR REPLACE FUNCTION cosine_similarity_json(a JSONB, b JSONB) 
RETURNS FLOAT AS $$
DECLARE
    dot_product FLOAT := 0;
    norm_a FLOAT := 0;
    norm_b FLOAT := 0;
    i INTEGER;
    val_a FLOAT;
    val_b FLOAT;
BEGIN
    -- Calculate dot product and norms
    FOR i IN 0..LEAST(jsonb_array_length(a)-1, jsonb_array_length(b)-1)
    LOOP
        val_a := (a->>i)::FLOAT;
        val_b := (b->>i)::FLOAT;
        
        dot_product := dot_product + (val_a * val_b);
        norm_a := norm_a + (val_a * val_a);
        norm_b := norm_b + (val_b * val_b);
    END LOOP;
    
    -- Avoid division by zero
    IF norm_a = 0 OR norm_b = 0 THEN
        RETURN 0;
    END IF;
    
    RETURN dot_product / (sqrt(norm_a) * sqrt(norm_b));
END;
$$ LANGUAGE plpgsql;

-- Test similarity search
\echo 'Testing vector similarity search...'
WITH query_vector AS (
    SELECT '[0.15, 0.25, 0.2, 0.5, 0.45, -0.2, -0.15, 0.75, 0.85, 0.15]'::JSONB as query_embedding
)
SELECT 
    de.id,
    de.title,
    de.content_preview,
    cosine_similarity_json(de.embedding_json, qv.query_embedding) as similarity_score
FROM document_embeddings de
CROSS JOIN query_vector qv
ORDER BY similarity_score DESC;

-- Test 3: Vector Conflict Resolution
\echo '=== Test 3: Vector Conflict Resolution ==='

-- Insert a vector that will be updated from different "nodes"
INSERT INTO document_embeddings (title, content_preview, embedding_json) VALUES 
('Conflict Test Vector', 'Test document for vector conflicts', 
 '[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]');

-- Get the ID for updates
\set vector_id (SELECT id FROM document_embeddings WHERE title = 'Conflict Test Vector')

-- Simulate updates from different nodes
UPDATE document_embeddings 
SET embedding_json = '[0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]',
    updated_by_node = '00000000-0000-0000-0000-000000000001',
    last_updated_at = NOW()
WHERE id = :vector_id;

-- Small delay
SELECT pg_sleep(0.1);

-- Another update (potential conflict)
UPDATE document_embeddings 
SET embedding_json = '[0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]',
    updated_by_node = '00000000-0000-0000-0000-000000000002',
    last_updated_at = NOW()
WHERE id = :vector_id;

-- Check the result
SELECT id, title, embedding_json, updated_by_node, last_updated_at
FROM document_embeddings 
WHERE title = 'Conflict Test Vector';

-- Test 4: Vector Index Simulation
\echo '=== Test 4: Vector Index Management ==='

-- Simulate vector index creation (would be real with pgvector)
INSERT INTO vector_indexes (table_name, column_name, index_name, dimensions, distance_metric) VALUES 
('document_embeddings', 'embedding_json', 'idx_doc_embeddings_vector', 10, 'cosine')
ON CONFLICT (table_name, column_name) DO NOTHING;

-- Check vector indexes
SELECT table_name, column_name, index_name, dimensions, distance_metric, created_at
FROM vector_indexes;

-- Test 5: Vector Batch Operations
\echo '=== Test 5: Vector Batch Operations ==='

-- Insert multiple vectors at once
INSERT INTO document_embeddings (title, content_preview, embedding_json) VALUES 
('Batch Vector 1', 'First batch vector', '[0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'),
('Batch Vector 2', 'Second batch vector', '[0.0, 0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'),
('Batch Vector 3', 'Third batch vector', '[0.0, 0.0, 0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]');

-- Find similar vectors to each batch vector
\echo 'Finding similar vectors...'
WITH batch_queries AS (
    SELECT id, title, embedding_json
    FROM document_embeddings 
    WHERE title LIKE 'Batch Vector%'
)
SELECT 
    bq.title as query_title,
    de.title as result_title,
    cosine_similarity_json(bq.embedding_json, de.embedding_json) as similarity
FROM batch_queries bq
CROSS JOIN document_embeddings de
WHERE de.id != bq.id
  AND cosine_similarity_json(bq.embedding_json, de.embedding_json) > 0.1
ORDER BY bq.title, similarity DESC;

-- Test 6: Vector Replication Across Nodes
\echo '=== Test 6: Vector Replication Test ==='

-- Insert with explicit node tracking
INSERT INTO document_embeddings (title, content_preview, embedding_json, updated_by_node) VALUES 
('Node Replication Test', 'Testing vector replication across nodes',
 '[0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]',
 (SELECT node_id FROM cluster_nodes WHERE node_name = current_setting('synapsedb.node_name', true)));

-- Check replication (would need to verify on other nodes in real deployment)
SELECT title, embedding_json, updated_by_node, created_at
FROM document_embeddings 
WHERE title = 'Node Replication Test';

-- Test 7: Vector Performance Metrics
\echo '=== Test 7: Vector Performance Metrics ==='

-- Create a larger dataset for performance testing
INSERT INTO document_embeddings (title, content_preview, embedding_json)
SELECT 
    'Perf Test ' || i,
    'Performance test document ' || i,
    jsonb_build_array(
        random(), random(), random(), random(), random(),
        random(), random(), random(), random(), random()
    )
FROM generate_series(1, 100) i;

-- Test query performance
\echo 'Testing vector search performance...'
EXPLAIN (ANALYZE, BUFFERS) 
WITH query_vector AS (
    SELECT '[0.5, 0.3, 0.7, 0.2, 0.8, 0.1, 0.9, 0.4, 0.6, 0.1]'::JSONB as qv
)
SELECT 
    title,
    cosine_similarity_json(embedding_json, qv.qv) as similarity
FROM document_embeddings, query_vector qv
WHERE title LIKE 'Perf Test%'
ORDER BY similarity DESC
LIMIT 10;

-- Test 8: Vector Data Integrity
\echo '=== Test 8: Vector Data Integrity ==='

-- Check for vector dimension consistency
SELECT 
    title,
    jsonb_array_length(embedding_json) as dimensions,
    CASE 
        WHEN jsonb_array_length(embedding_json) = 10 THEN 'OK'
        ELSE 'MISMATCH'
    END as dimension_status
FROM document_embeddings 
ORDER BY id DESC 
LIMIT 10;

-- Check for invalid vectors (null or malformed)
SELECT 
    COUNT(*) as total_vectors,
    COUNT(*) FILTER (WHERE embedding_json IS NOT NULL) as valid_vectors,
    COUNT(*) FILTER (WHERE jsonb_typeof(embedding_json) = 'array') as array_vectors,
    COUNT(*) FILTER (WHERE jsonb_array_length(embedding_json) = 10) as correct_dimension_vectors
FROM document_embeddings;

-- Test 9: Vector Cleanup and Maintenance
\echo '=== Test 9: Vector Cleanup ==='

-- Clean up test data
DELETE FROM document_embeddings 
WHERE title IN ('Conflict Test Vector', 'Node Replication Test')
   OR title LIKE 'Batch Vector%'
   OR title LIKE 'Perf Test%';

-- Verify cleanup
SELECT COUNT(*) as remaining_test_vectors 
FROM document_embeddings 
WHERE title NOT IN ('Getting Started Guide', 'API Reference', 'Troubleshooting');

-- Clean up functions and indexes
DROP FUNCTION IF EXISTS cosine_similarity_json(JSONB, JSONB);
DELETE FROM vector_indexes WHERE table_name = 'document_embeddings';

-- Test 10: Vector Schema Evolution
\echo '=== Test 10: Vector Schema Evolution ==='

-- Test adding vector column to existing table (simulation)
-- ALTER TABLE products ADD COLUMN product_embedding_json JSONB;

-- This would test how vector schema changes replicate across nodes
-- UPDATE products SET product_embedding_json = jsonb_build_array(random(), random(), random()) 
-- WHERE id <= 3;

-- Check schema replication
SELECT table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'document_embeddings' 
  AND column_name LIKE '%embedding%'
ORDER BY ordinal_position;

-- Drop the test table
DROP TABLE document_embeddings;

\echo '=== Vector operations tests completed! ==='
\echo 'Note: These tests use JSON arrays as vector fallback.'
\echo 'Install pgvector extension for native vector operations.'