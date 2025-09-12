-- SynapseDB: Replication Setup using pglogical
-- This script sets up logical replication between nodes

-- Create replication coordination functions

-- Initialize node as provider (publisher)
CREATE OR REPLACE FUNCTION setup_replication_provider(node_name_param TEXT)
RETURNS VOID AS $$
BEGIN
    -- Create the provider node
    PERFORM pglogical.create_node(
        node_name => node_name_param,
        dsn => format('host=%s port=%s dbname=%s user=synapsedb', 
                     current_setting('synapsedb.host'),
                     current_setting('synapsedb.port'),
                     current_database())
    );
    
    -- Create the default replication set
    PERFORM pglogical.create_replication_set('synapsedb_default');
    
    -- Add all user tables to replication set (excluding system tables)
    INSERT INTO schema_migrations (node_id, migration_type, sql_command)
    SELECT 
        (SELECT node_id FROM cluster_nodes WHERE node_name = node_name_param),
        'DDL',
        'setup_replication_provider: ' || node_name_param;
END;
$$ LANGUAGE plpgsql;

-- Initialize node as subscriber
CREATE OR REPLACE FUNCTION setup_replication_subscriber(
    local_node_name TEXT,
    provider_node_name TEXT,
    provider_dsn TEXT
)
RETURNS VOID AS $$
BEGIN
    -- Create the local subscriber node
    PERFORM pglogical.create_node(
        node_name => local_node_name,
        dsn => format('host=%s port=%s dbname=%s user=synapsedb', 
                     current_setting('synapsedb.host'),
                     current_setting('synapsedb.port'),
                     current_database())
    );
    
    -- Create subscription to provider
    PERFORM pglogical.create_subscription(
        subscription_name => format('sub_%s_to_%s', local_node_name, provider_node_name),
        provider_dsn => provider_dsn,
        replication_sets => ARRAY['synapsedb_default'],
        synchronize_structure => false,
        synchronize_data => false
    );
    
    -- Log the subscription
    INSERT INTO schema_migrations (node_id, migration_type, sql_command)
    SELECT 
        (SELECT node_id FROM cluster_nodes WHERE node_name = local_node_name),
        'DDL',
        format('setup_replication_subscriber: %s -> %s', local_node_name, provider_node_name);
END;
$$ LANGUAGE plpgsql;

-- Add table to replication
CREATE OR REPLACE FUNCTION add_table_to_replication(table_name_param TEXT)
RETURNS VOID AS $$
BEGIN
    -- Add table to default replication set
    PERFORM pglogical.replication_set_add_table('synapsedb_default', table_name_param);
    
    -- Log the addition
    INSERT INTO schema_migrations (migration_type, sql_command, node_id)
    VALUES ('DDL', 'add_table_to_replication: ' || table_name_param, 
            (SELECT node_id FROM cluster_nodes WHERE is_writer = true LIMIT 1));
END;
$$ LANGUAGE plpgsql;

-- Remove table from replication
CREATE OR REPLACE FUNCTION remove_table_from_replication(table_name_param TEXT)
RETURNS VOID AS $$
BEGIN
    PERFORM pglogical.replication_set_remove_table('synapsedb_default', table_name_param);
    
    INSERT INTO schema_migrations (migration_type, sql_command, node_id)
    VALUES ('DDL', 'remove_table_from_replication: ' || table_name_param,
            (SELECT node_id FROM cluster_nodes WHERE is_writer = true LIMIT 1));
END;
$$ LANGUAGE plpgsql;

-- Monitor replication lag
CREATE OR REPLACE FUNCTION update_replication_lag()
RETURNS VOID AS $$
DECLARE
    sub_record RECORD;
    lag_info RECORD;
BEGIN
    -- Update lag information for all subscriptions
    FOR sub_record IN 
        SELECT subscription_name, provider_node, subscriber_name
        FROM pglogical.subscription
    LOOP
        -- Get lag information
        SELECT * INTO lag_info
        FROM pglogical.show_subscription_status(sub_record.subscription_name);
        
        -- Update replication status
        INSERT INTO replication_status (
            source_node_id, target_node_id, last_sync_time, 
            lag_bytes, status
        )
        SELECT 
            src.node_id, tgt.node_id, NOW(),
            COALESCE(lag_info.received_lsn::TEXT::BIGINT - lag_info.last_msg_send_time::TEXT::BIGINT, 0),
            CASE 
                WHEN lag_info.status = 'replicating' THEN 'active'
                WHEN lag_info.status = 'down' THEN 'broken'
                ELSE 'lagging'
            END
        FROM cluster_nodes src, cluster_nodes tgt
        WHERE src.node_name = sub_record.provider_node
          AND tgt.node_name = sub_record.subscriber_name
        ON CONFLICT (source_node_id, target_node_id) 
        DO UPDATE SET
            last_sync_time = EXCLUDED.last_sync_time,
            lag_bytes = EXCLUDED.lag_bytes,
            status = EXCLUDED.status;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Conflict detection and resolution trigger
CREATE OR REPLACE FUNCTION detect_and_resolve_conflicts()
RETURNS TRIGGER AS $$
DECLARE
    conflict_detected BOOLEAN := FALSE;
    winning_timestamp TIMESTAMPTZ;
    losing_values JSONB[];
BEGIN
    -- Only for UPDATE operations where last_updated_at exists
    IF TG_OP = 'UPDATE' AND column_exists(TG_TABLE_NAME, 'last_updated_at') THEN
        -- Check if this is a conflict (multiple updates around same time from different nodes)
        IF OLD.last_updated_at = NEW.last_updated_at AND 
           OLD.updated_by_node IS DISTINCT FROM NEW.updated_by_node THEN
            
            conflict_detected := TRUE;
            
            -- Last write wins based on timestamp, then node_id as tiebreaker
            IF NEW.last_updated_at > OLD.last_updated_at OR 
               (NEW.last_updated_at = OLD.last_updated_at AND 
                NEW.updated_by_node > OLD.updated_by_node) THEN
                winning_timestamp := NEW.last_updated_at;
                losing_values := ARRAY[row_to_json(OLD)::JSONB];
            ELSE
                -- Keep the old values, reject the new ones
                NEW := OLD;
                winning_timestamp := OLD.last_updated_at;
                losing_values := ARRAY[row_to_json(NEW)::JSONB];
            END IF;
            
            -- Log the conflict
            INSERT INTO conflict_log (
                table_name, row_key, conflict_type, winning_value, 
                losing_values, resolved_at, resolved_by_node
            ) VALUES (
                TG_TABLE_NAME,
                jsonb_build_object('id', NEW.id), -- Assuming 'id' primary key
                'update_conflict',
                row_to_json(NEW)::JSONB,
                losing_values,
                NOW(),
                NEW.updated_by_node
            );
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to setup bidirectional replication between two nodes
CREATE OR REPLACE FUNCTION setup_bidirectional_replication(
    node1_name TEXT, node1_dsn TEXT,
    node2_name TEXT, node2_dsn TEXT
)
RETURNS VOID AS $$
BEGIN
    -- Setup node1 as provider for node2
    PERFORM setup_replication_provider(node1_name);
    
    -- Setup node2 as subscriber to node1
    PERFORM setup_replication_subscriber(node2_name, node1_name, node1_dsn);
    
    -- Setup node2 as provider for node1
    PERFORM setup_replication_provider(node2_name);
    
    -- Setup node1 as subscriber to node2
    PERFORM setup_replication_subscriber(node1_name, node2_name, node2_dsn);
    
    RAISE NOTICE 'Bidirectional replication setup between % and %', node1_name, node2_name;
END;
$$ LANGUAGE plpgsql;