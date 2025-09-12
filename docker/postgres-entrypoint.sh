#!/bin/bash
set -e

# SynapseDB PostgreSQL Custom Entrypoint
# Handles SSL certificate generation, cluster initialization, and extension setup

echo "Starting SynapseDB PostgreSQL node: $SYNAPSEDB_NODE_NAME"

# Set default values
export SYNAPSEDB_NODE_NAME=${SYNAPSEDB_NODE_NAME:-node1}
export SYNAPSEDB_CLUSTER_NAME=${SYNAPSEDB_CLUSTER_NAME:-synapsedb}
export SYNAPSEDB_NODE_ID=${SYNAPSEDB_NODE_ID:-$(uuidgen)}

# Generate SSL certificates
echo "Generating SSL certificates..."
/usr/local/bin/generate-certs.sh

# Create required directories
mkdir -p /var/lib/postgresql/data/log
mkdir -p /var/lib/postgresql/data/archive
mkdir -p /var/lib/postgresql/data/conf.d

# Set ownership
chown -R postgres:postgres /var/lib/postgresql/data
chown -R postgres:postgres /var/lib/postgresql/ssl

# Initialize database if needed
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Initializing PostgreSQL database..."
    
    # Initialize with custom options
    su postgres -c "initdb \
        --encoding=UTF8 \
        --locale=C \
        --data-checksums \
        --auth-local=trust \
        --auth-host=md5"
    
    echo "Database initialized successfully"
fi

# Create SynapseDB configuration
cat > /var/lib/postgresql/data/conf.d/synapsedb.conf << EOF
# SynapseDB Node Configuration
synapsedb.node_id = '$SYNAPSEDB_NODE_ID'
synapsedb.node_name = '$SYNAPSEDB_NODE_NAME'
synapsedb.cluster_name = '$SYNAPSEDB_CLUSTER_NAME'
EOF

# Start PostgreSQL in background for setup
echo "Starting PostgreSQL for initialization..."
su postgres -c "pg_ctl -D $PGDATA -w start -o '-c config_file=/etc/postgresql/postgresql.conf'"

# Wait for PostgreSQL to be ready
until su postgres -c "pg_isready -h localhost -p 5432"; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "PostgreSQL is ready, running initialization scripts..."

# Create extensions and initial setup
su postgres -c "psql -d postgres" << 'EOF'
-- Create SynapseDB database if it doesn't exist
SELECT 'CREATE DATABASE synapsedb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'synapsedb');

-- Create users
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'synapsedb') THEN
        CREATE USER synapsedb WITH SUPERUSER PASSWORD 'changeme123';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'synapsedb_cluster') THEN
        CREATE USER synapsedb_cluster WITH REPLICATION PASSWORD 'cluster_secret_key';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'synapsedb_monitor') THEN
        CREATE USER synapsedb_monitor WITH PASSWORD 'monitor_secret';
        GRANT pg_read_all_stats TO synapsedb_monitor;
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'synapsedb_backup') THEN
        CREATE USER synapsedb_backup WITH REPLICATION PASSWORD 'backup_secret';
    END IF;
END
$$;
EOF

# Connect to synapsedb database and create extensions
su postgres -c "psql -d synapsedb" << 'EOF'
-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pglogical";
CREATE EXTENSION IF NOT EXISTS "vector";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Set up pg_cron
SELECT cron.schedule('cleanup-logs', '0 2 * * *', 'DELETE FROM pg_stat_statements WHERE queryid NOT IN (SELECT queryid FROM pg_stat_statements ORDER BY total_time DESC LIMIT 1000);');
EOF

echo "Extensions created successfully"

# Stop PostgreSQL
su postgres -c "pg_ctl -D $PGDATA stop"

echo "SynapseDB PostgreSQL initialization complete"

# Start PostgreSQL with proper configuration
exec su postgres -c "postgres -c config_file=/etc/postgresql/postgresql.conf"