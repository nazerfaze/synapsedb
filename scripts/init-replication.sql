-- SynapseDB Replication Initialization
-- This script sets up the initial replication configuration for each node

\echo 'Initializing SynapseDB replication configuration...'

-- Create replication-specific schemas
CREATE SCHEMA IF NOT EXISTS synapsedb_replication;
CREATE SCHEMA IF NOT EXISTS synapsedb_sharding;
CREATE SCHEMA IF NOT EXISTS synapsedb_consensus;

-- Grant permissions
GRANT USAGE ON SCHEMA synapsedb_replication TO synapsedb_cluster;
GRANT USAGE ON SCHEMA synapsedb_sharding TO synapsedb_cluster;
GRANT USAGE ON SCHEMA synapsedb_consensus TO synapsedb_cluster;

-- Create initial replication monitoring tables in synapsedb_replication schema
CREATE TABLE IF NOT EXISTS synapsedb_replication.node_status (
    node_id UUID PRIMARY KEY,
    node_name TEXT UNIQUE NOT NULL,
    host_address TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 5432,
    status TEXT NOT NULL DEFAULT 'initializing' CHECK (status IN ('initializing', 'active', 'inactive', 'failed', 'maintenance')),
    is_raft_leader BOOLEAN DEFAULT FALSE,
    raft_term BIGINT DEFAULT 0,
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    pglogical_node_id TEXT,
    replication_lag_bytes BIGINT DEFAULT 0,
    replication_lag_seconds INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create replication topology tracking
CREATE TABLE IF NOT EXISTS synapsedb_replication.replication_links (
    source_node_id UUID NOT NULL,
    target_node_id UUID NOT NULL,
    subscription_name TEXT NOT NULL,
    subscription_status TEXT DEFAULT 'creating',
    last_sync_lsn PG_LSN,
    last_sync_time TIMESTAMPTZ DEFAULT NOW(),
    lag_bytes BIGINT DEFAULT 0,
    lag_seconds INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (source_node_id, target_node_id)
);

-- Create shard assignment table
CREATE TABLE IF NOT EXISTS synapsedb_sharding.shard_assignments (
    shard_id INTEGER PRIMARY KEY,
    primary_node_id UUID NOT NULL,
    replica_node_ids UUID[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'migrating', 'inactive')),
    key_range_start TEXT,
    key_range_end TEXT,
    table_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create consensus state table
CREATE TABLE IF NOT EXISTS synapsedb_consensus.raft_state (
    node_id UUID PRIMARY KEY,
    current_term BIGINT NOT NULL DEFAULT 0,
    voted_for UUID,
    commit_index BIGINT NOT NULL DEFAULT 0,
    last_applied BIGINT NOT NULL DEFAULT 0,
    role TEXT NOT NULL DEFAULT 'follower' CHECK (role IN ('leader', 'follower', 'candidate')),
    leader_id UUID,
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    election_timeout INTERVAL DEFAULT '5 seconds',
    heartbeat_interval INTERVAL DEFAULT '1 second',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Raft log entries table
CREATE TABLE IF NOT EXISTS synapsedb_consensus.raft_log (
    log_index BIGINT PRIMARY KEY,
    term BIGINT NOT NULL,
    entry_type TEXT NOT NULL CHECK (entry_type IN ('command', 'config_change', 'noop')),
    command_data JSONB,
    committed BOOLEAN DEFAULT FALSE,
    applied BOOLEAN DEFAULT FALSE,
    node_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_node_status_name ON synapsedb_replication.node_status(node_name);
CREATE INDEX IF NOT EXISTS idx_node_status_leader ON synapsedb_replication.node_status(is_raft_leader) WHERE is_raft_leader = TRUE;
CREATE INDEX IF NOT EXISTS idx_repl_links_source ON synapsedb_replication.replication_links(source_node_id);
CREATE INDEX IF NOT EXISTS idx_repl_links_target ON synapsedb_replication.replication_links(target_node_id);
CREATE INDEX IF NOT EXISTS idx_shard_primary ON synapsedb_sharding.shard_assignments(primary_node_id);
CREATE INDEX IF NOT EXISTS idx_raft_log_index ON synapsedb_consensus.raft_log(log_index);
CREATE INDEX IF NOT EXISTS idx_raft_log_term ON synapsedb_consensus.raft_log(term);
CREATE INDEX IF NOT EXISTS idx_raft_log_committed ON synapsedb_consensus.raft_log(committed) WHERE committed = TRUE;

-- Insert initial node record
DO $$
DECLARE
    node_name TEXT := current_setting('synapsedb.node_name', true);
    node_id TEXT := current_setting('synapsedb.node_id', true);
    cluster_name TEXT := current_setting('synapsedb.cluster_name', true);
BEGIN
    IF node_name IS NOT NULL AND node_name != '' AND node_id IS NOT NULL AND node_id != '' THEN
        INSERT INTO synapsedb_replication.node_status (
            node_id, node_name, host_address, port, status
        ) VALUES (
            node_id::UUID, 
            node_name, 
            'localhost',  -- Will be updated by cluster manager
            5432, 
            'initializing'
        ) ON CONFLICT (node_id) DO UPDATE SET
            node_name = EXCLUDED.node_name,
            status = 'initializing',
            updated_at = NOW();
        
        -- Initialize Raft state for this node
        INSERT INTO synapsedb_consensus.raft_state (
            node_id, current_term, role
        ) VALUES (
            node_id::UUID,
            0,
            'follower'
        ) ON CONFLICT (node_id) DO UPDATE SET
            updated_at = NOW();
        
        RAISE NOTICE 'Initialized replication state for node % (ID: %)', node_name, node_id;
    ELSE
        RAISE WARNING 'Node name or ID not configured, skipping replication initialization';
    END IF;
END
$$;

-- Create utility functions for replication management
CREATE OR REPLACE FUNCTION synapsedb_replication.update_node_heartbeat(p_node_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE synapsedb_replication.node_status 
    SET last_heartbeat = NOW(), updated_at = NOW()
    WHERE node_id = p_node_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Node % not found in cluster', p_node_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create function to get cluster topology
CREATE OR REPLACE FUNCTION synapsedb_replication.get_cluster_topology()
RETURNS TABLE(
    node_id UUID,
    node_name TEXT,
    host_address TEXT,
    port INTEGER,
    status TEXT,
    is_raft_leader BOOLEAN,
    raft_term BIGINT,
    last_heartbeat TIMESTAMPTZ,
    seconds_since_heartbeat INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ns.node_id,
        ns.node_name,
        ns.host_address,
        ns.port,
        ns.status,
        ns.is_raft_leader,
        ns.raft_term,
        ns.last_heartbeat,
        EXTRACT(EPOCH FROM (NOW() - ns.last_heartbeat))::INTEGER as seconds_since_heartbeat
    FROM synapsedb_replication.node_status ns
    ORDER BY ns.is_raft_leader DESC, ns.node_name;
END;
$$ LANGUAGE plpgsql;

\echo 'SynapseDB replication configuration initialized successfully'