-- SynapseDB: Cluster Setup and Metadata Tables
-- This script creates the foundational tables and triggers for distributed functionality

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pglogical;
CREATE EXTENSION IF NOT EXISTS vector;

-- Cluster metadata table
CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_name TEXT UNIQUE NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 5432,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'failed')),
    is_writer BOOLEAN NOT NULL DEFAULT FALSE,
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Schema migrations tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id BIGSERIAL PRIMARY KEY,
    node_id UUID REFERENCES cluster_nodes(node_id),
    migration_type TEXT NOT NULL CHECK (migration_type IN ('DDL', 'DML')),
    sql_command TEXT NOT NULL,
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    checksum TEXT,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

-- Replication status tracking
CREATE TABLE IF NOT EXISTS replication_status (
    source_node_id UUID REFERENCES cluster_nodes(node_id),
    target_node_id UUID REFERENCES cluster_nodes(node_id),
    last_sync_lsn PG_LSN,
    last_sync_time TIMESTAMPTZ DEFAULT NOW(),
    lag_bytes BIGINT DEFAULT 0,
    lag_seconds INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'lagging', 'broken')),
    PRIMARY KEY (source_node_id, target_node_id)
);

-- Conflict resolution log
CREATE TABLE IF NOT EXISTS conflict_log (
    conflict_id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    row_key JSONB NOT NULL,
    conflict_type TEXT NOT NULL CHECK (conflict_type IN ('update_conflict', 'delete_conflict')),
    winning_value JSONB,
    losing_values JSONB[],
    resolution_method TEXT DEFAULT 'last_write_wins',
    resolved_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_by_node UUID REFERENCES cluster_nodes(node_id)
);

-- Vector index metadata for distributed consistency
CREATE TABLE IF NOT EXISTS vector_indexes (
    index_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    distance_metric TEXT DEFAULT 'cosine' CHECK (distance_metric IN ('cosine', 'l2', 'inner_product')),
    index_params JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

-- Functions for cluster management

-- Update node heartbeat
CREATE OR REPLACE FUNCTION update_node_heartbeat(node_name_param TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE cluster_nodes 
    SET last_heartbeat = NOW(), 
        updated_at = NOW()
    WHERE node_name = node_name_param;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Node % not found', node_name_param;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Get active writer node
CREATE OR REPLACE FUNCTION get_active_writer()
RETURNS TABLE(node_id UUID, node_name TEXT, host TEXT, port INTEGER) AS $$
BEGIN
    RETURN QUERY
    SELECT cn.node_id, cn.node_name, cn.host, cn.port
    FROM cluster_nodes cn
    WHERE cn.is_writer = TRUE 
      AND cn.status = 'active'
      AND cn.last_heartbeat > NOW() - INTERVAL '30 seconds'
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Promote node to writer
CREATE OR REPLACE FUNCTION promote_to_writer(node_name_param TEXT)
RETURNS VOID AS $$
BEGIN
    -- Demote current writer
    UPDATE cluster_nodes SET is_writer = FALSE WHERE is_writer = TRUE;
    
    -- Promote new writer
    UPDATE cluster_nodes 
    SET is_writer = TRUE, updated_at = NOW()
    WHERE node_name = node_name_param AND status = 'active';
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Cannot promote node % - not found or not active', node_name_param;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Check cluster quorum (requires at least 2 out of 3 nodes)
CREATE OR REPLACE FUNCTION check_quorum()
RETURNS BOOLEAN AS $$
DECLARE
    active_nodes INTEGER;
    total_nodes INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_nodes FROM cluster_nodes;
    SELECT COUNT(*) INTO active_nodes 
    FROM cluster_nodes 
    WHERE status = 'active' 
      AND last_heartbeat > NOW() - INTERVAL '30 seconds';
    
    -- Require majority quorum
    RETURN active_nodes >= CEIL(total_nodes::NUMERIC / 2);
END;
$$ LANGUAGE plpgsql;

-- Generic trigger function to add timestamp and node tracking
CREATE OR REPLACE FUNCTION add_replication_metadata()
RETURNS TRIGGER AS $$
BEGIN
    -- Add last_updated_at if column exists
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        IF column_exists(TG_TABLE_NAME, 'last_updated_at') THEN
            NEW.last_updated_at = NOW();
        END IF;
        IF column_exists(TG_TABLE_NAME, 'updated_by_node') THEN
            NEW.updated_by_node = current_setting('synapsedb.node_id', true);
        END IF;
        RETURN NEW;
    END IF;
    
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Helper function to check if column exists
CREATE OR REPLACE FUNCTION column_exists(table_name_param TEXT, column_name_param TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = table_name_param 
          AND column_name = column_name_param
    );
END;
$$ LANGUAGE plpgsql;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_cluster_nodes_status ON cluster_nodes(status, last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_cluster_nodes_writer ON cluster_nodes(is_writer) WHERE is_writer = TRUE;
CREATE INDEX IF NOT EXISTS idx_schema_migrations_node ON schema_migrations(node_id, applied_at);
CREATE INDEX IF NOT EXISTS idx_replication_status_lag ON replication_status(lag_seconds, status);
CREATE INDEX IF NOT EXISTS idx_conflict_log_table ON conflict_log(table_name, resolved_at);