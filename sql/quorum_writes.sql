-- SynapseDB: Quorum-based Write System
-- Implements distributed consensus for write operations

-- Transaction coordination table
CREATE TABLE IF NOT EXISTS distributed_transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    coordinator_node_id UUID REFERENCES cluster_nodes(node_id),
    participant_nodes UUID[],
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('write', 'ddl')),
    sql_command TEXT,
    transaction_data JSONB,
    status TEXT NOT NULL DEFAULT 'preparing' 
        CHECK (status IN ('preparing', 'prepared', 'committed', 'aborted', 'failed')),
    quorum_size INTEGER NOT NULL DEFAULT 2,
    votes_received INTEGER DEFAULT 0,
    participant_votes JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    timeout_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '30 seconds'
);

-- Prepare phase votes from participants
CREATE TABLE IF NOT EXISTS transaction_votes (
    transaction_id UUID REFERENCES distributed_transactions(transaction_id),
    node_id UUID REFERENCES cluster_nodes(node_id),
    vote TEXT NOT NULL CHECK (vote IN ('yes', 'no')),
    vote_reason TEXT,
    voted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (transaction_id, node_id)
);

-- Write locks to prevent concurrent modifications
CREATE TABLE IF NOT EXISTS write_locks (
    lock_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    row_key JSONB NOT NULL,
    transaction_id UUID REFERENCES distributed_transactions(transaction_id),
    locked_by_node UUID REFERENCES cluster_nodes(node_id),
    lock_type TEXT NOT NULL CHECK (lock_type IN ('read', 'write', 'exclusive')),
    acquired_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '5 minutes'
);

-- Function to initiate distributed write transaction
CREATE OR REPLACE FUNCTION begin_distributed_write(
    sql_command_param TEXT,
    affected_tables TEXT[],
    affected_keys JSONB[],
    required_quorum INTEGER DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    transaction_uuid UUID;
    active_nodes UUID[];
    quorum_size INTEGER;
    current_node_id UUID;
BEGIN
    -- Get current node ID
    SELECT node_id INTO current_node_id 
    FROM cluster_nodes 
    WHERE node_name = current_setting('synapsedb.node_name', true);
    
    -- Get active nodes for quorum
    SELECT array_agg(node_id) INTO active_nodes
    FROM cluster_nodes 
    WHERE status = 'active' 
      AND last_heartbeat > NOW() - INTERVAL '30 seconds';
    
    -- Calculate required quorum (majority by default)
    quorum_size := COALESCE(required_quorum, CEIL(array_length(active_nodes, 1)::NUMERIC / 2));
    
    -- Check if we have enough nodes for quorum
    IF array_length(active_nodes, 1) < quorum_size THEN
        RAISE EXCEPTION 'Insufficient active nodes for quorum. Need %, have %', 
                       quorum_size, array_length(active_nodes, 1);
    END IF;
    
    -- Generate transaction ID
    transaction_uuid := gen_random_uuid();
    
    -- Create distributed transaction record
    INSERT INTO distributed_transactions (
        transaction_id, coordinator_node_id, participant_nodes,
        transaction_type, sql_command, quorum_size
    ) VALUES (
        transaction_uuid, current_node_id, active_nodes,
        'write', sql_command_param, quorum_size
    );
    
    -- Acquire write locks
    FOR i IN 1..array_length(affected_tables, 1)
    LOOP
        INSERT INTO write_locks (
            table_name, row_key, transaction_id, locked_by_node, lock_type
        ) VALUES (
            affected_tables[i], 
            COALESCE(affected_keys[i], '{}'),
            transaction_uuid, 
            current_node_id, 
            'write'
        );
    END LOOP;
    
    RETURN transaction_uuid;
END;
$$ LANGUAGE plpgsql;

-- Function for participants to vote on transaction
CREATE OR REPLACE FUNCTION vote_on_transaction(
    transaction_uuid UUID,
    vote_decision TEXT,
    reason TEXT DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    current_node_id UUID;
    transaction_record RECORD;
    current_votes INTEGER;
BEGIN
    -- Get current node ID
    SELECT node_id INTO current_node_id 
    FROM cluster_nodes 
    WHERE node_name = current_setting('synapsedb.node_name', true);
    
    -- Check if transaction exists and is in preparing state
    SELECT * INTO transaction_record
    FROM distributed_transactions 
    WHERE transaction_id = transaction_uuid 
      AND status = 'preparing'
      AND timeout_at > NOW();
    
    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;
    
    -- Record the vote
    INSERT INTO transaction_votes (transaction_id, node_id, vote, vote_reason)
    VALUES (transaction_uuid, current_node_id, vote_decision, reason)
    ON CONFLICT (transaction_id, node_id) 
    DO UPDATE SET 
        vote = EXCLUDED.vote, 
        vote_reason = EXCLUDED.vote_reason,
        voted_at = NOW();
    
    -- Update vote count and participant votes
    UPDATE distributed_transactions 
    SET 
        votes_received = (
            SELECT COUNT(*) 
            FROM transaction_votes 
            WHERE transaction_id = transaction_uuid
        ),
        participant_votes = (
            SELECT jsonb_object_agg(node_id::text, vote)
            FROM transaction_votes 
            WHERE transaction_id = transaction_uuid
        )
    WHERE transaction_id = transaction_uuid;
    
    -- Check if we have enough votes to decide
    SELECT votes_received INTO current_votes
    FROM distributed_transactions 
    WHERE transaction_id = transaction_uuid;
    
    -- If we have quorum votes, try to commit
    IF current_votes >= transaction_record.quorum_size THEN
        PERFORM decide_transaction(transaction_uuid);
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to decide transaction outcome based on votes
CREATE OR REPLACE FUNCTION decide_transaction(transaction_uuid UUID)
RETURNS TEXT AS $$
DECLARE
    transaction_record RECORD;
    yes_votes INTEGER;
    no_votes INTEGER;
    decision TEXT;
BEGIN
    -- Get transaction details
    SELECT * INTO transaction_record
    FROM distributed_transactions 
    WHERE transaction_id = transaction_uuid;
    
    IF NOT FOUND THEN
        RETURN 'not_found';
    END IF;
    
    -- Count votes
    SELECT 
        COUNT(*) FILTER (WHERE vote = 'yes'),
        COUNT(*) FILTER (WHERE vote = 'no')
    INTO yes_votes, no_votes
    FROM transaction_votes 
    WHERE transaction_id = transaction_uuid;
    
    -- Decision logic: need majority yes votes
    IF yes_votes >= transaction_record.quorum_size THEN
        decision := 'committed';
        
        -- Execute the transaction on all nodes
        PERFORM commit_distributed_transaction(transaction_uuid);
        
    ELSE
        decision := 'aborted';
        
        -- Abort the transaction
        PERFORM abort_distributed_transaction(transaction_uuid);
    END IF;
    
    -- Update transaction status
    UPDATE distributed_transactions 
    SET 
        status = decision,
        decided_at = NOW()
    WHERE transaction_id = transaction_uuid;
    
    RETURN decision;
END;
$$ LANGUAGE plpgsql;

-- Function to commit distributed transaction
CREATE OR REPLACE FUNCTION commit_distributed_transaction(transaction_uuid UUID)
RETURNS VOID AS $$
DECLARE
    transaction_record RECORD;
    participant_node_id UUID;
    connection_string TEXT;
    node_record RECORD;
BEGIN
    -- Get transaction details
    SELECT * INTO transaction_record
    FROM distributed_transactions 
    WHERE transaction_id = transaction_uuid;
    
    -- Execute on all participant nodes
    FOREACH participant_node_id IN ARRAY transaction_record.participant_nodes
    LOOP
        -- Get node connection info
        SELECT * INTO node_record
        FROM cluster_nodes 
        WHERE node_id = participant_node_id;
        
        IF FOUND AND node_record.status = 'active' THEN
            BEGIN
                -- Build connection string
                connection_string := format('host=%s port=%s dbname=%s user=synapsedb',
                                          node_record.host, node_record.port, current_database());
                
                -- Execute the SQL command on the participant node
                PERFORM dblink_exec(connection_string, 
                    format('BEGIN; %s; COMMIT;', transaction_record.sql_command)
                );
                
            EXCEPTION WHEN OTHERS THEN
                -- Log error but don't fail the whole transaction
                INSERT INTO conflict_log (
                    table_name, conflict_type, error_message, resolved_at
                ) VALUES (
                    'distributed_transactions', 'commit_failed', 
                    format('Failed to commit on node %s: %s', node_record.node_name, SQLERRM),
                    NOW()
                );
            END;
        END IF;
    END LOOP;
    
    -- Release locks
    DELETE FROM write_locks WHERE transaction_id = transaction_uuid;
END;
$$ LANGUAGE plpgsql;

-- Function to abort distributed transaction
CREATE OR REPLACE FUNCTION abort_distributed_transaction(transaction_uuid UUID)
RETURNS VOID AS $$
BEGIN
    -- Release all locks
    DELETE FROM write_locks WHERE transaction_id = transaction_uuid;
    
    -- Log the abort
    INSERT INTO conflict_log (
        table_name, conflict_type, resolved_at
    ) VALUES (
        'distributed_transactions', 'transaction_aborted', NOW()
    );
END;
$$ LANGUAGE plpgsql;

-- Function to execute write with quorum
CREATE OR REPLACE FUNCTION execute_with_quorum(
    sql_command TEXT,
    affected_tables TEXT[] DEFAULT ARRAY['unknown'],
    affected_keys JSONB[] DEFAULT ARRAY['{}']
)
RETURNS TABLE(success BOOLEAN, transaction_id UUID, message TEXT) AS $$
DECLARE
    tx_id UUID;
    decision TEXT;
    timeout_count INTEGER := 0;
BEGIN
    -- Check if we have quorum before starting
    IF NOT check_quorum() THEN
        RETURN QUERY SELECT FALSE, NULL::UUID, 'No quorum available';
        RETURN;
    END IF;
    
    -- Begin distributed write transaction
    BEGIN
        tx_id := begin_distributed_write(sql_command, affected_tables, affected_keys);
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, NULL::UUID, 'Failed to begin distributed transaction: ' || SQLERRM;
        RETURN;
    END;
    
    -- Wait for decision (with timeout)
    WHILE timeout_count < 30 -- Wait up to 30 seconds
    LOOP
        SELECT status INTO decision
        FROM distributed_transactions 
        WHERE transaction_id = tx_id;
        
        IF decision IN ('committed', 'aborted', 'failed') THEN
            EXIT;
        END IF;
        
        PERFORM pg_sleep(1);
        timeout_count := timeout_count + 1;
    END LOOP;
    
    -- Handle timeout
    IF decision NOT IN ('committed', 'aborted', 'failed') THEN
        PERFORM abort_distributed_transaction(tx_id);
        RETURN QUERY SELECT FALSE, tx_id, 'Transaction timed out';
        RETURN;
    END IF;
    
    -- Return result
    IF decision = 'committed' THEN
        RETURN QUERY SELECT TRUE, tx_id, 'Transaction committed successfully';
    ELSE
        RETURN QUERY SELECT FALSE, tx_id, 'Transaction was aborted or failed';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Cleanup function for expired transactions
CREATE OR REPLACE FUNCTION cleanup_expired_transactions()
RETURNS INTEGER AS $$
DECLARE
    cleanup_count INTEGER := 0;
    expired_tx RECORD;
BEGIN
    -- Find expired transactions
    FOR expired_tx IN 
        SELECT transaction_id 
        FROM distributed_transactions 
        WHERE status = 'preparing' 
          AND timeout_at < NOW()
    LOOP
        -- Abort expired transaction
        PERFORM abort_distributed_transaction(expired_tx.transaction_id);
        
        -- Update status
        UPDATE distributed_transactions 
        SET status = 'failed', decided_at = NOW()
        WHERE transaction_id = expired_tx.transaction_id;
        
        cleanup_count := cleanup_count + 1;
    END LOOP;
    
    -- Clean up old locks
    DELETE FROM write_locks WHERE expires_at < NOW();
    
    -- Clean up old transaction records (keep for audit)
    DELETE FROM distributed_transactions 
    WHERE decided_at < NOW() - INTERVAL '24 hours';
    
    DELETE FROM transaction_votes 
    WHERE transaction_id NOT IN (
        SELECT transaction_id FROM distributed_transactions
    );
    
    RETURN cleanup_count;
END;
$$ LANGUAGE plpgsql;

-- Background process to handle transaction coordination
CREATE OR REPLACE FUNCTION process_distributed_transactions()
RETURNS VOID AS $$
DECLARE
    pending_tx RECORD;
    node_record RECORD;
    connection_string TEXT;
BEGIN
    -- Process preparing transactions
    FOR pending_tx IN 
        SELECT * FROM distributed_transactions 
        WHERE status = 'preparing' 
          AND coordinator_node_id = (
              SELECT node_id FROM cluster_nodes 
              WHERE node_name = current_setting('synapsedb.node_name', true)
          )
        ORDER BY created_at
        LIMIT 10
    LOOP
        -- Send prepare requests to all participants
        FOREACH node_record IN ARRAY (
            SELECT array_agg(row_to_json(cn)) 
            FROM cluster_nodes cn 
            WHERE cn.node_id = ANY(pending_tx.participant_nodes) 
              AND cn.status = 'active'
        )
        LOOP
            BEGIN
                connection_string := format('host=%s port=%s dbname=%s user=synapsedb',
                                          node_record->>'host', 
                                          (node_record->>'port')::INTEGER, 
                                          current_database());
                
                -- Ask participant to vote
                PERFORM dblink_exec(connection_string, 
                    format('SELECT vote_on_transaction(''%s'', ''yes'')', pending_tx.transaction_id)
                );
                
            EXCEPTION WHEN OTHERS THEN
                -- Log error and record negative vote
                INSERT INTO transaction_votes (transaction_id, node_id, vote, vote_reason)
                VALUES (
                    pending_tx.transaction_id, 
                    (node_record->>'node_id')::UUID, 
                    'no', 
                    'Connection failed: ' || SQLERRM
                );
            END;
        END LOOP;
    END LOOP;
    
    -- Clean up expired transactions
    PERFORM cleanup_expired_transactions();
END;
$$ LANGUAGE plpgsql;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_distributed_transactions_status ON distributed_transactions(status, created_at);
CREATE INDEX IF NOT EXISTS idx_distributed_transactions_coordinator ON distributed_transactions(coordinator_node_id, status);
CREATE INDEX IF NOT EXISTS idx_transaction_votes_tx ON transaction_votes(transaction_id);
CREATE INDEX IF NOT EXISTS idx_write_locks_table ON write_locks(table_name, expires_at);
CREATE INDEX IF NOT EXISTS idx_write_locks_transaction ON write_locks(transaction_id);