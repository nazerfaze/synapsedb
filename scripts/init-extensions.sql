-- SynapseDB Extension Initialization
-- This script is run during database initialization to set up all required extensions

\echo 'Initializing SynapseDB extensions...'

-- Set up search path
SET search_path = public, pglogical;

-- Create extensions with error handling
DO $$
DECLARE
    ext_name TEXT;
    ext_list TEXT[] := ARRAY[
        'uuid-ossp',
        'pgcrypto', 
        'pg_stat_statements',
        'pglogical',
        'vector',
        'pg_cron'
    ];
BEGIN
    FOREACH ext_name IN ARRAY ext_list
    LOOP
        BEGIN
            EXECUTE format('CREATE EXTENSION IF NOT EXISTS "%s"', ext_name);
            RAISE NOTICE 'Extension % created successfully', ext_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to create extension %: %', ext_name, SQLERRM;
        END;
    END LOOP;
END
$$;

-- Configure pg_stat_statements
SELECT pg_stat_statements_reset();

-- Configure pglogical for this node
DO $$
DECLARE
    node_name TEXT := current_setting('synapsedb.node_name', true);
    node_id TEXT := current_setting('synapsedb.node_id', true);
BEGIN
    -- Only proceed if we have node configuration
    IF node_name IS NOT NULL AND node_name != '' THEN
        -- This will be called later by the cluster manager
        -- We just ensure the extension is available here
        RAISE NOTICE 'Pglogical extension ready for node %', node_name;
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Pglogical setup deferred: %', SQLERRM;
END
$$;

\echo 'SynapseDB extensions initialized successfully'