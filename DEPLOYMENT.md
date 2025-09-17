# SynapseDB Deployment Guide

## Quick Start (5 minutes)

### 1. Prerequisites
- **Docker** 20.10+ and **Docker Compose** 2.0+
- **8GB+ RAM** (4GB minimum)
- **10GB+ free disk space**
- **Ports available**: 5432-5434, 5001-5002, 8080-8091, 3000, 9090, 8404

### 2. Start SynapseDB
```bash
# Clone or navigate to the SynapseDB directory
cd synapsedb.com

# Start the distributed database (one command!)
./start.sh

# Or manually with docker-compose
docker-compose up -d
```

### 3. Wait for Initialization
```bash
# Watch the logs until you see "Cluster initialization successful!"
docker-compose logs -f cluster-init

# Check all services are running
docker-compose ps
```

### 4. Connect & Test
```bash
# Connect to read endpoint
psql "postgresql://synapsedb:synapsedb@localhost:5001/synapsedb"

# Test distributed insert
INSERT INTO users (username, email) VALUES ('test_user', 'test@example.com');

# Verify replication across nodes
\q
psql "postgresql://synapsedb:synapsedb@localhost:5432/synapsedb" -c "SELECT * FROM users;"
psql "postgresql://synapsedb:synapsedb@localhost:5433/synapsedb" -c "SELECT * FROM users;"
psql "postgresql://synapsedb:synapsedb@localhost:5434/synapsedb" -c "SELECT * FROM users;"
```

## System Architecture

### Services Overview
```
Service                 Container               Ports        Purpose
=======================================================================
PostgreSQL Node 1       synapsedb-postgres1     5432        Database + pglogical + pgvector
PostgreSQL Node 2       synapsedb-postgres2     5433        Database + pglogical + pgvector  
PostgreSQL Node 3       synapsedb-postgres3     5434        Database + pglogical + pgvector
Cluster Manager 1       synapsedb-manager1      8080-8083   All distributed services
Cluster Manager 2       synapsedb-manager2      8084-8087   All distributed services
Cluster Manager 3       synapsedb-manager3      8088-8091   All distributed services
HAProxy Load Balancer   synapsedb-haproxy       5001-5002   Read/Write endpoints
Cluster Initializer     synapsedb-init          -           One-time setup
Prometheus             synapsedb-prometheus     9090        Metrics collection
Grafana                synapsedb-grafana        3000        Monitoring dashboards
```

### Distributed Services (per node)
Each cluster manager runs all 14 production components:
- **Raft Consensus** (port 8080): Leader election, log replication
- **Gossip Protocol** (port 8081): Failure detection, network partitions
- **Deadlock Detector** (port 8082): Global wait-for graph analysis
- **Backup Manager** (port 8083): Distributed backup coordination
- **Transaction Coordinator**: 2-phase commit for distributed transactions
- **Replication Manager**: pglogical bi-directional replication
- **Sharding Manager**: Automatic data partitioning
- **Schema Manager**: Coordinated DDL migrations
- **Vector Clock System**: Causal consistency and conflict resolution
- **TLS Manager**: Mutual TLS certificate management
- **Connection Pool**: Intelligent routing with circuit breakers
- **Rolling Update Manager**: Zero-downtime updates

## Connection Endpoints

### Application Connections
```bash
# High-availability read endpoint (HAProxy round-robin)
postgresql://synapsedb:synapsedb@localhost:5001/synapsedb

# High-availability write endpoint (HAProxy to primary)  
postgresql://synapsedb:synapsedb@localhost:5002/synapsedb

# Direct node connections (for debugging)
postgresql://synapsedb:synapsedb@localhost:5432/synapsedb  # Node 1
postgresql://synapsedb:synapsedb@localhost:5433/synapsedb  # Node 2
postgresql://synapsedb:synapsedb@localhost:5434/synapsedb  # Node 3
```

### Management Interfaces
```bash
# Monitoring & Dashboards
http://localhost:3000          # Grafana (admin/admin123)
http://localhost:9090          # Prometheus metrics
http://localhost:8404/stats    # HAProxy statistics

# Cluster APIs (Node 1, 2, 3 respectively)
http://localhost:8080/raft/status      # Raft consensus status
http://localhost:8081/gossip/members   # Gossip protocol members
http://localhost:8082/deadlock/graph   # Deadlock wait-for graph
http://localhost:8083/backup/status    # Backup system status

http://localhost:8084/raft/status      # Node 2 APIs
http://localhost:8088/raft/status      # Node 3 APIs
```

## Testing the System

### 1. Run Integration Tests
```bash
# Complete integration test suite
python tests/integration/test_full_system.py

# Expected output: All 12 tests should PASS
# Tests: replication, transactions, consensus, vectors, sharding, 
#        failover, conflicts, backups, updates, health, deadlocks, performance
```

### 2. Manual Testing

#### Test Multi-Master Replication
```sql
-- Connect to Node 1
\c postgresql://synapsedb:synapsedb@localhost:5432/synapsedb

-- Insert data
INSERT INTO users (username, email) VALUES ('user_node1', 'user1@test.com');

-- Connect to Node 2 and verify replication
\c postgresql://synapsedb:synapsedb@localhost:5433/synapsedb
SELECT * FROM users WHERE username = 'user_node1';  -- Should show the record

-- Insert from Node 2
INSERT INTO users (username, email) VALUES ('user_node2', 'user2@test.com');

-- Connect to Node 3 and verify bi-directional replication
\c postgresql://synapsedb:synapsedb@localhost:5434/synapsedb
SELECT * FROM users;  -- Should show both records
```

#### Test Vector Operations
```sql
-- Create vector table (using JSON for pgvector fallback)
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT,
    embedding_json JSONB
);

-- Insert vector data
INSERT INTO documents (title, content, embedding_json) VALUES 
('Doc 1', 'AI content', '[0.1, 0.2, 0.3, 0.4]'),
('Doc 2', 'ML content', '[0.2, 0.3, 0.4, 0.5]'),
('Doc 3', 'Other content', '[0.9, 0.8, 0.7, 0.6]');

-- Vector similarity search (simplified dot product)
SELECT title, 
       (SELECT SUM((a.value::float) * (b.value::float))
        FROM jsonb_array_elements(embedding_json) WITH ORDINALITY a(value, idx)
        JOIN jsonb_array_elements('[0.15, 0.25, 0.35, 0.45]'::jsonb) WITH ORDINALITY b(value, idx)
        ON a.idx = b.idx) as similarity
FROM documents
ORDER BY similarity DESC;
```

#### Test Distributed Transactions
```sql
-- Start distributed transaction
BEGIN;

-- Insert user
INSERT INTO users (username, email) VALUES ('txn_user', 'txn@test.com');

-- Insert related order (will be replicated together)
INSERT INTO orders (user_id, product_name, amount) 
SELECT id, 'Test Product', 99.99 FROM users WHERE username = 'txn_user';

COMMIT;

-- Verify consistency across nodes
\c postgresql://synapsedb:synapsedb@localhost:5433/synapsedb
SELECT u.username, o.product_name FROM users u 
JOIN orders o ON u.id = o.user_id WHERE u.username = 'txn_user';
```

### 3. Performance Testing
```bash
# Use pgbench for load testing
docker exec synapsedb-postgres1 pgbench -i -s 10 synapsedb

# Run load test through HAProxy read endpoint
docker run --rm --network synapsedb.com_synapsedb_network postgres:15-alpine \
  pgbench -h haproxy -p 5001 -U synapsedb -d synapsedb \
  -c 10 -j 4 -T 60 -S  # 10 connections, 60 seconds, SELECT-only
```

## Operational Commands

### Health Monitoring
```bash
# Check all services status
docker-compose ps

# View aggregated logs
docker-compose logs -f --tail=50

# Check specific service logs
docker-compose logs -f cluster-manager1
docker-compose logs -f postgres1

# Check cluster health via API
curl http://localhost:8080/raft/status
curl http://localhost:8081/gossip/members
curl http://localhost:8082/deadlock/stats
```

### Cluster Operations
```bash
# Restart a single service
docker-compose restart cluster-manager1

# Scale up (add more monitoring)
docker-compose up -d --scale prometheus=1

# Update configuration and reload
docker-compose up -d --force-recreate cluster-manager1

# Backup data volumes
docker run --rm -v synapsedb.com_postgres1_data:/data -v $(pwd)/backup:/backup \
  alpine tar czf /backup/postgres1_backup.tar.gz -C /data .
```

### Troubleshooting
```bash
# Check PostgreSQL logs
docker exec synapsedb-postgres1 tail -f /var/lib/postgresql/data/log/postgresql-*.log

# Check pglogical replication status
docker exec synapsedb-postgres1 psql -U synapsedb -d synapsedb -c \
  "SELECT subscription_name, status, worker_count FROM pglogical.subscription;"

# Check system resources
docker stats

# Enter container for debugging
docker exec -it synapsedb-manager1 /bin/bash

# Check network connectivity between nodes
docker exec synapsedb-postgres1 ping postgres2
```

## Production Deployment

### Resource Requirements
```yaml
# Minimum production requirements:
CPU: 8 cores (2+ per PostgreSQL node)
Memory: 16GB (4GB per PostgreSQL node)
Storage: 100GB+ SSD (depends on data size)
Network: Gigabit Ethernet, low latency preferred

# Recommended production:
CPU: 16+ cores
Memory: 32GB+
Storage: NVMe SSD with high IOPS
Network: 10Gbps with <1ms latency between nodes
```

### Configuration Updates
```bash
# For production, update these files:
config/pg_config.conf          # PostgreSQL optimization
config/haproxy_config.cfg       # Load balancer tuning
docker-compose.yml              # Resource limits and replica counts

# Production PostgreSQL settings:
shared_buffers = 4GB            # 25% of system RAM
effective_cache_size = 12GB     # 75% of system RAM
work_mem = 64MB                 # For complex queries
max_connections = 500           # Based on application needs
```

### Security Hardening
```bash
# Change default passwords
export POSTGRES_PASSWORD="your-secure-password"

# Enable TLS for all connections
# Update pg_hba.conf to require SSL

# Use external secrets management
# Mount secrets instead of environment variables

# Network segmentation
# Restrict cluster communication to private network
```

### Backup Strategy
```bash
# Automated backups are built-in via backup_manager.py
# Configure backup destination:
# Local: /app/data/backups (default)
# S3: Set AWS credentials and bucket in backup config

# Manual backup
curl -X POST http://localhost:8083/backup/create

# List backups
curl http://localhost:8083/backup/list

# Restore from backup
curl -X POST http://localhost:8083/backup/restore/BACKUP_ID
```

## Stopping and Cleanup

### Stop Services
```bash
# Stop all services (preserves data)
docker-compose down

# Stop and remove everything including data
docker-compose down -v

# Remove just containers (keep data)
docker-compose rm -f
```

### Complete Cleanup
```bash
# Remove all containers, networks, and volumes
docker-compose down -v --remove-orphans

# Remove built images
docker rmi $(docker images "synapsedb*" -q)

# Clean up system
docker system prune -f
```

## Support and Troubleshooting

### Common Issues

1. **"Port already in use"**
   ```bash
   # Check what's using the ports
   netstat -tulpn | grep :5432
   
   # Kill processes or change ports in docker-compose.yml
   ```

2. **"pglogical extension not found"**
   ```bash
   # Rebuild PostgreSQL containers
   docker-compose build --no-cache postgres1 postgres2 postgres3
   ```

3. **"Cluster initialization failed"**
   ```bash
   # Check logs
   docker-compose logs cluster-init
   
   # Restart initialization
   docker-compose restart cluster-init
   ```

4. **"Replication not working"**
   ```bash
   # Check pglogical status on each node
   docker exec synapsedb-postgres1 psql -U synapsedb -d synapsedb -c \
     "SELECT * FROM pglogical.subscription;"
   ```

### Performance Issues
- Monitor with Grafana dashboard at http://localhost:3000
- Check resource usage with `docker stats`
- Optimize PostgreSQL configuration for your workload
- Consider scaling by adding more nodes

### Getting Help
- Check the comprehensive technical documentation in `docs/TECHNICAL_SPECIFICATIONS.md`
- Run the full test suite to identify specific issues
- Review service logs for detailed error information
- Use the built-in health check APIs for diagnostics

---

**🎉 You now have a production-grade distributed PostgreSQL database running!**

The system provides enterprise features like automatic failover, distributed transactions, vector operations, TLS security, point-in-time backups, and zero-downtime updates - all built on proven PostgreSQL foundations.