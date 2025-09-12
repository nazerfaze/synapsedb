-- SynapseDB: DDL Event Triggers for Schema Propagation
-- These triggers capture DDL changes and propagate them across nodes

-- Enable event trigger support
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Table to queue DDL commands for replication
CREATE TABLE IF NOT EXISTS ddl_replication_queue (
    queue_id BIGSERIAL PRIMARY KEY,
    command_tag TEXT NOT NULL,
    object_type TEXT,
    schema_name TEXT,
    object_name TEXT,
    command_text TEXT NOT NULL,
    command_hash TEXT NOT NULL,
    executed_by TEXT DEFAULT current_user,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    source_node_id UUID REFERENCES cluster_nodes(node_id),
    replicated_to_nodes UUID[] DEFAULT ARRAY[]::UUID[],
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'replicating', 'completed', 'failed')),
    retry_count INTEGER DEFAULT 0,
    error_message TEXT
);

-- Capture DDL commands
CREATE OR REPLACE FUNCTION capture_ddl_command()
RETURNS EVENT_TRIGGER AS $$
DECLARE
    obj RECORD;
    cmd_text TEXT;
    cmd_hash TEXT;
    current_node_id UUID;
BEGIN
    -- Skip if this is a replication-generated DDL
    IF current_setting('synapsedb.is_replicated_ddl', true) = 'true' THEN
        RETURN;
    END IF;
    
    -- Get current node ID
    SELECT node_id INTO current_node_id 
    FROM cluster_nodes 
    WHERE node_name = current_setting('synapsedb.node_name', true);
    
    -- Get the complete command text
    cmd_text := current_query();
    cmd_hash := encode(digest(cmd_text, 'sha256'), 'hex');
    
    -- Check if we've already processed this command (prevent loops)
    IF EXISTS (SELECT 1 FROM ddl_replication_queue WHERE command_hash = cmd_hash) THEN
        RETURN;
    END IF;
    
    -- Queue the DDL command for replication
    INSERT INTO ddl_replication_queue (
        command_tag, command_text, command_hash, source_node_id
    ) VALUES (
        TG_TAG, cmd_text, cmd_hash, current_node_id
    );
    
    -- For CREATE TABLE, automatically add to replication set
    IF TG_TAG = 'CREATE TABLE' THEN
        -- Extract table name from the DDL (simplified parsing)
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF obj.object_type = 'table' THEN
                -- Add the table to replication
                PERFORM add_table_to_replication(obj.schema_name || '.' || obj.object_identity);
                
                -- Add timestamp and node tracking columns if they don't exist
                EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMPTZ DEFAULT NOW()', 
                              obj.schema_name, obj.object_identity);
                EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS updated_by_node UUID', 
                              obj.schema_name, obj.object_identity);
                
                -- Add conflict resolution trigger
                EXECUTE format('CREATE TRIGGER %I_conflict_resolution 
                               BEFORE UPDATE ON %I.%I 
                               FOR EACH ROW EXECUTE FUNCTION detect_and_resolve_conflicts()',
                              obj.object_identity || '_conflict', obj.schema_name, obj.object_identity);
                
                -- Add metadata trigger
                EXECUTE format('CREATE TRIGGER %I_metadata 
                               BEFORE INSERT OR UPDATE ON %I.%I 
                               FOR EACH ROW EXECUTE FUNCTION add_replication_metadata()',
                              obj.object_identity || '_meta', obj.schema_name, obj.object_identity);
            END IF;
        END LOOP;
    END IF;
    
    -- For vector indexes, track them
    IF TG_TAG = 'CREATE INDEX' AND cmd_text ILIKE '%USING ivfflat%' OR cmd_text ILIKE '%USING hnsw%' THEN
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF obj.object_type = 'index' THEN
                -- Parse index creation to extract vector info (simplified)
                INSERT INTO vector_indexes (table_name, column_name, index_name, dimensions)
                SELECT 
                    split_part(obj.object_identity, '.', 2), -- table name
                    'embedding', -- assume column name (would need better parsing)
                    obj.object_identity, -- index name
                    1536 -- assume dimensions (would need better parsing)
                ON CONFLICT (table_name, column_name) DO UPDATE SET
                    index_name = EXCLUDED.index_name,
                    dimensions = EXCLUDED.dimensions;
            END IF;
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Process DDL replication queue
CREATE OR REPLACE FUNCTION process_ddl_replication()
RETURNS VOID AS $$
DECLARE
    ddl_cmd RECORD;
    target_node RECORD;
    connection_string TEXT;
    result BOOLEAN;
BEGIN
    -- Process pending DDL commands
    FOR ddl_cmd IN 
        SELECT * FROM ddl_replication_queue 
        WHERE status = 'pending' 
        ORDER BY queue_id
        LIMIT 10 -- Process in batches
    LOOP
        UPDATE ddl_replication_queue 
        SET status = 'replicating' 
        WHERE queue_id = ddl_cmd.queue_id;
        
        -- Replicate to all other nodes
        FOR target_node IN 
            SELECT * FROM cluster_nodes 
            WHERE node_id != ddl_cmd.source_node_id 
              AND status = 'active'
        LOOP
            BEGIN
                -- Build connection string
                connection_string := format('host=%s port=%s dbname=%s user=synapsedb',
                                          target_node.host, target_node.port, current_database());
                
                -- Execute DDL on target node using dblink
                SELECT dblink_exec(connection_string, 
                    format('SET synapsedb.is_replicated_ddl = true; %s', ddl_cmd.command_text)
                ) IS NOT NULL INTO result;
                
                IF result THEN
                    -- Mark as replicated to this node
                    UPDATE ddl_replication_queue 
                    SET replicated_to_nodes = array_append(replicated_to_nodes, target_node.node_id)
                    WHERE queue_id = ddl_cmd.queue_id;
                END IF;
                
            EXCEPTION WHEN OTHERS THEN
                -- Log error but continue with other nodes
                UPDATE ddl_replication_queue 
                SET error_message = SQLERRM,
                    retry_count = retry_count + 1
                WHERE queue_id = ddl_cmd.queue_id;
            END;
        END LOOP;
        
        -- Check if replicated to all nodes
        IF (SELECT array_length(replicated_to_nodes, 1) 
            FROM ddl_replication_queue 
            WHERE queue_id = ddl_cmd.queue_id) = 
           (SELECT COUNT(*) - 1 FROM cluster_nodes WHERE status = 'active') THEN
            
            UPDATE ddl_replication_queue 
            SET status = 'completed' 
            WHERE queue_id = ddl_cmd.queue_id;
        ELSE
            UPDATE ddl_replication_queue 
            SET status = 'failed' 
            WHERE queue_id = ddl_cmd.queue_id AND retry_count >= 3;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create event triggers
CREATE EVENT TRIGGER synapsedb_ddl_capture
    ON ddl_command_end
    EXECUTE FUNCTION capture_ddl_command();

-- Drop event trigger (for cleanup if needed)
CREATE OR REPLACE FUNCTION drop_ddl_triggers()
RETURNS VOID AS $$
BEGIN
    DROP EVENT TRIGGER IF EXISTS synapsedb_ddl_capture;
END;
$$ LANGUAGE plpgsql;

-- Function to replay DDL from another node
CREATE OR REPLACE FUNCTION replay_ddl_from_node(source_node_name TEXT, since_timestamp TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour')
RETURNS INTEGER AS $$
DECLARE
    source_node RECORD;
    ddl_record RECORD;
    replayed_count INTEGER := 0;
BEGIN
    -- Get source node info
    SELECT * INTO source_node FROM cluster_nodes WHERE node_name = source_node_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Source node % not found', source_node_name;
    END IF;
    
    -- Fetch and replay DDL commands from source node
    FOR ddl_record IN
        SELECT * FROM ddl_replication_queue 
        WHERE source_node_id = source_node.node_id 
          AND executed_at >= since_timestamp
          AND status = 'completed'
        ORDER BY executed_at
    LOOP
        BEGIN
            -- Set flag to prevent infinite replication
            PERFORM set_config('synapsedb.is_replicated_ddl', 'true', true);
            
            -- Execute the DDL command
            EXECUTE ddl_record.command_text;
            
            replayed_count := replayed_count + 1;
            
            -- Log successful replay
            INSERT INTO schema_migrations (migration_type, sql_command, node_id)
            VALUES ('DDL', 'REPLAYED: ' || ddl_record.command_text, 
                   (SELECT node_id FROM cluster_nodes WHERE node_name = current_setting('synapsedb.node_name', true)));
            
        EXCEPTION WHEN OTHERS THEN
            -- Log error but continue
            INSERT INTO schema_migrations (migration_type, sql_command, success, error_message, node_id)
            VALUES ('DDL', 'FAILED REPLAY: ' || ddl_record.command_text, FALSE, SQLERRM,
                   (SELECT node_id FROM cluster_nodes WHERE node_name = current_setting('synapsedb.node_name', true)));
        END;
    END LOOP;
    
    RETURN replayed_count;
END;
$$ LANGUAGE plpgsql;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ddl_queue_status ON ddl_replication_queue(status, executed_at);
CREATE INDEX IF NOT EXISTS idx_ddl_queue_hash ON ddl_replication_queue(command_hash);
CREATE INDEX IF NOT EXISTS idx_ddl_queue_source ON ddl_replication_queue(source_node_id, executed_at);