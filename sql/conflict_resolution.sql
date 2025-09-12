-- SynapseDB: Advanced Conflict Resolution
-- Implements sophisticated conflict detection and resolution strategies

-- Conflict resolution policies
CREATE TABLE IF NOT EXISTS conflict_policies (
    policy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_pattern TEXT NOT NULL, -- e.g., 'users.*', 'orders'
    resolution_strategy TEXT NOT NULL DEFAULT 'last_write_wins' 
        CHECK (resolution_strategy IN (
            'last_write_wins', 
            'first_write_wins', 
            'highest_value_wins', 
            'merge_json', 
            'custom_function'
        )),
    priority_column TEXT, -- Column to use for priority-based resolution
    custom_function TEXT, -- Name of custom resolution function
    conflict_window_seconds INTEGER DEFAULT 5, -- Consider conflicts within this window
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enhanced conflict detection with configurable strategies
CREATE OR REPLACE FUNCTION resolve_conflict_advanced()
RETURNS TRIGGER AS $$
DECLARE
    conflict_policy RECORD;
    resolution_strategy TEXT;
    conflict_detected BOOLEAN := FALSE;
    winning_row RECORD;
    window_seconds INTEGER := 5;
    conflict_timestamp TIMESTAMPTZ;
BEGIN
    -- Only process UPDATE operations with timestamp tracking
    IF TG_OP != 'UPDATE' OR NOT column_exists(TG_TABLE_NAME, 'last_updated_at') THEN
        RETURN NEW;
    END IF;
    
    -- Get conflict resolution policy for this table
    SELECT * INTO conflict_policy
    FROM conflict_policies 
    WHERE TG_TABLE_NAME ~ table_pattern
    ORDER BY LENGTH(table_pattern) DESC -- More specific patterns first
    LIMIT 1;
    
    -- Use default policy if none found
    resolution_strategy := COALESCE(conflict_policy.resolution_strategy, 'last_write_wins');
    window_seconds := COALESCE(conflict_policy.conflict_window_seconds, 5);
    
    -- Detect conflicts: simultaneous updates within the conflict window
    IF ABS(EXTRACT(EPOCH FROM (NEW.last_updated_at - OLD.last_updated_at))) <= window_seconds 
       AND OLD.updated_by_node IS DISTINCT FROM NEW.updated_by_node THEN
        
        conflict_detected := TRUE;
        conflict_timestamp := GREATEST(OLD.last_updated_at, NEW.last_updated_at);
        
        -- Apply resolution strategy
        CASE resolution_strategy
            WHEN 'last_write_wins' THEN
                IF NEW.last_updated_at > OLD.last_updated_at OR 
                   (NEW.last_updated_at = OLD.last_updated_at AND NEW.updated_by_node > OLD.updated_by_node) THEN
                    winning_row := NEW;
                ELSE
                    winning_row := OLD;
                    NEW := OLD; -- Keep old values
                END IF;
                
            WHEN 'first_write_wins' THEN
                IF OLD.last_updated_at < NEW.last_updated_at OR 
                   (OLD.last_updated_at = NEW.last_updated_at AND OLD.updated_by_node < NEW.updated_by_node) THEN
                    winning_row := OLD;
                    NEW := OLD; -- Keep old values
                ELSE
                    winning_row := NEW;
                END IF;
                
            WHEN 'highest_value_wins' THEN
                -- Use priority column if specified
                IF conflict_policy.priority_column IS NOT NULL THEN
                    DECLARE
                        old_priority NUMERIC;
                        new_priority NUMERIC;
                    BEGIN
                        -- Dynamic column access (simplified - would need more robust implementation)
                        EXECUTE format('SELECT ($1).%I::NUMERIC', conflict_policy.priority_column) 
                        INTO old_priority USING OLD;
                        
                        EXECUTE format('SELECT ($1).%I::NUMERIC', conflict_policy.priority_column) 
                        INTO new_priority USING NEW;
                        
                        IF new_priority > old_priority THEN
                            winning_row := NEW;
                        ELSE
                            winning_row := OLD;
                            NEW := OLD;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        -- Fallback to last_write_wins
                        IF NEW.last_updated_at >= OLD.last_updated_at THEN
                            winning_row := NEW;
                        ELSE
                            winning_row := OLD;
                            NEW := OLD;
                        END IF;
                    END;
                END IF;
                
            WHEN 'merge_json' THEN
                -- Merge JSON columns (assumes all non-system columns are JSON-mergeable)
                DECLARE
                    column_record RECORD;
                    old_json JSONB;
                    new_json JSONB;
                    merged_json JSONB;
                BEGIN
                    -- Merge all JSONB columns
                    FOR column_record IN 
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = TG_TABLE_NAME 
                          AND data_type = 'jsonb'
                          AND column_name NOT IN ('last_updated_at', 'updated_by_node', 'created_at')
                    LOOP
                        EXECUTE format('SELECT ($1).%I', column_record.column_name) INTO old_json USING OLD;
                        EXECUTE format('SELECT ($1).%I', column_record.column_name) INTO new_json USING NEW;
                        
                        merged_json := COALESCE(old_json, '{}'::jsonb) || COALESCE(new_json, '{}'::jsonb);
                        
                        -- Set merged value (this is simplified - real implementation would need dynamic SQL)
                        EXECUTE format('SELECT jsonb_set(to_jsonb($1), ''{%s}'', $2)', column_record.column_name) 
                        INTO NEW USING NEW, merged_json;
                    END LOOP;
                    
                    winning_row := NEW;
                    NEW.last_updated_at := conflict_timestamp;
                END;
                
            WHEN 'custom_function' THEN
                -- Call custom resolution function
                IF conflict_policy.custom_function IS NOT NULL THEN
                    EXECUTE format('SELECT * FROM %I($1, $2)', conflict_policy.custom_function) 
                    INTO winning_row USING OLD, NEW;
                    NEW := winning_row;
                ELSE
                    -- Fallback to last_write_wins
                    IF NEW.last_updated_at >= OLD.last_updated_at THEN
                        winning_row := NEW;
                    ELSE
                        winning_row := OLD;
                        NEW := OLD;
                    END IF;
                END IF;
                
            ELSE
                -- Default to last_write_wins
                IF NEW.last_updated_at >= OLD.last_updated_at THEN
                    winning_row := NEW;
                ELSE
                    winning_row := OLD;
                    NEW := OLD;
                END IF;
        END CASE;
        
        -- Log the conflict with detailed information
        INSERT INTO conflict_log (
            table_name, row_key, conflict_type, winning_value, 
            losing_values, resolution_method, resolved_at, resolved_by_node
        ) VALUES (
            TG_TABLE_NAME,
            jsonb_build_object('id', COALESCE(NEW.id, OLD.id)), -- Flexible key extraction
            'update_conflict',
            row_to_json(winning_row)::JSONB,
            ARRAY[row_to_json(CASE WHEN winning_row = NEW THEN OLD ELSE NEW END)::JSONB],
            resolution_strategy,
            NOW(),
            winning_row.updated_by_node
        );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Vector-specific conflict resolution
CREATE OR REPLACE FUNCTION resolve_vector_conflicts()
RETURNS TRIGGER AS $$
DECLARE
    vector_cols TEXT[];
    col_name TEXT;
    old_vector vector;
    new_vector vector;
    similarity_threshold FLOAT := 0.95;
BEGIN
    -- Get vector columns for this table
    SELECT array_agg(column_name) INTO vector_cols
    FROM information_schema.columns 
    WHERE table_name = TG_TABLE_NAME 
      AND udt_name = 'vector';
    
    -- Check each vector column for conflicts
    FOREACH col_name IN ARRAY vector_cols
    LOOP
        EXECUTE format('SELECT ($1).%I', col_name) INTO old_vector USING OLD;
        EXECUTE format('SELECT ($1).%I', col_name) INTO new_vector USING NEW;
        
        -- If vectors are very similar, keep the newer one
        -- If they're different, this might be a legitimate update
        IF old_vector IS NOT NULL AND new_vector IS NOT NULL THEN
            IF 1 - (old_vector <=> new_vector) > similarity_threshold THEN
                -- Vectors are very similar, treat as conflict
                IF NEW.last_updated_at >= OLD.last_updated_at THEN
                    -- Keep new vector
                    NULL; -- No action needed
                ELSE
                    -- Revert to old vector
                    EXECUTE format('SELECT jsonb_set(to_jsonb($1), ''{%s}'', to_jsonb($2))', col_name) 
                    INTO NEW USING NEW, old_vector;
                END IF;
                
                -- Log vector conflict
                INSERT INTO conflict_log (
                    table_name, row_key, conflict_type, winning_value,
                    resolution_method, resolved_at
                ) VALUES (
                    TG_TABLE_NAME,
                    jsonb_build_object('id', NEW.id),
                    'vector_similarity_conflict',
                    jsonb_build_object('similarity_score', 1 - (old_vector <=> new_vector)),
                    'vector_timestamp_resolution',
                    NOW()
                );
            END IF;
        END IF;
    END LOOP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Batch conflict resolution for catching up after outages
CREATE OR REPLACE FUNCTION resolve_batch_conflicts(
    table_name_param TEXT,
    since_timestamp TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour'
)
RETURNS TABLE(resolved_conflicts BIGINT, failed_resolutions BIGINT) AS $$
DECLARE
    resolved_count BIGINT := 0;
    failed_count BIGINT := 0;
    conflict_record RECORD;
    sql_command TEXT;
BEGIN
    -- Find potential conflicts in the specified table
    sql_command := format(
        'SELECT *, ctid FROM %I 
         WHERE last_updated_at >= $1 
         ORDER BY last_updated_at, updated_by_node',
        table_name_param
    );
    
    FOR conflict_record IN EXECUTE sql_command USING since_timestamp
    LOOP
        BEGIN
            -- Trigger conflict resolution by updating the row
            EXECUTE format('UPDATE %I SET last_updated_at = last_updated_at WHERE ctid = $1', 
                          table_name_param) 
            USING conflict_record.ctid;
            
            resolved_count := resolved_count + 1;
            
        EXCEPTION WHEN OTHERS THEN
            failed_count := failed_count + 1;
            
            INSERT INTO conflict_log (
                table_name, conflict_type, error_message, resolved_at
            ) VALUES (
                table_name_param, 'batch_resolution_failed', SQLERRM, NOW()
            );
        END;
    END LOOP;
    
    RETURN QUERY SELECT resolved_count, failed_count;
END;
$$ LANGUAGE plpgsql;

-- Function to create custom conflict resolution function template
CREATE OR REPLACE FUNCTION create_custom_conflict_resolver(
    function_name TEXT,
    table_name TEXT,
    resolution_logic TEXT
)
RETURNS VOID AS $$
DECLARE
    function_body TEXT;
BEGIN
    function_body := format('
        CREATE OR REPLACE FUNCTION %I(old_row %I, new_row %I)
        RETURNS %I AS $func$
        DECLARE
            result_row %I;
        BEGIN
            result_row := new_row; -- Start with new row as base
            
            %s
            
            RETURN result_row;
        END;
        $func$ LANGUAGE plpgsql;',
        function_name, table_name, table_name, table_name, table_name, resolution_logic
    );
    
    EXECUTE function_body;
END;
$$ LANGUAGE plpgsql;

-- Insert default conflict policies
INSERT INTO conflict_policies (table_pattern, resolution_strategy, conflict_window_seconds)
VALUES 
    ('.*', 'last_write_wins', 5),                    -- Default for all tables
    ('user.*', 'last_write_wins', 10),               -- User tables with longer window
    ('product.*', 'highest_value_wins', 3),          -- Products with shorter window
    ('.*_embeddings', 'last_write_wins', 2),         -- Vector embeddings
    ('.*_config', 'merge_json', 60)                  -- Configuration tables
ON CONFLICT DO NOTHING;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_conflict_policies_pattern ON conflict_policies(table_pattern);
CREATE INDEX IF NOT EXISTS idx_conflict_log_table_time ON conflict_log(table_name, resolved_at);
CREATE INDEX IF NOT EXISTS idx_conflict_log_type ON conflict_log(conflict_type, resolved_at);