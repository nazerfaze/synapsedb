# SynapseDB: Distributed PostgreSQL with Vector Support

A distributed database system built on PostgreSQL with pgvector support, inspired by CockroachDB's architecture but implemented as a layer on top of PostgreSQL using logical replication.

## Features

### 🌐 Multi-Master Replication
- **Write Anywhere**: Any node can accept writes with automatic replication
- **DML Replication**: Row-level changes replicated via WAL logical replication
- **DDL Replication**: Schema changes captured via event triggers and replicated
- **Conflict Resolution**: Configurable strategies including last-write-wins

### 🔄 High Availability & Consensus
- **Quorum-Based Writes**: Writes only succeed with majority consensus (2/3 nodes)
- **Automatic Failover**: Writer node failover with health monitoring
- **Split-Brain Protection**: Prevents inconsistent states during partitions
- **Health Monitoring**: Continuous heartbeat and lag monitoring

### 🧠 Vector Operations
- **pgvector Integration**: Native support for vector embeddings (when available)
- **Distributed Vector Search**: Similarity searches across all nodes
- **Vector Conflict Resolution**: Specialized handling for embedding updates
- **Fallback Support**: JSON-based vector operations when pgvector unavailable

### 📊 Monitoring & Management
- **Query Router**: Intelligent query routing based on load and health
- **Cluster Manager**: Node coordination and metadata management
- **HAProxy Integration**: High availability endpoints for reads/writes
- **Grafana Dashboards**: Real-time monitoring and metrics

## Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB RAM
- Ports 5432-5434, 5000-5001, 8080, 3000, 9090 available

### 1. Clone and Start

```bash
git clone <repository-url>
cd synapsedb.com

# Start the cluster
docker-compose up -d

# Wait for initialization
docker-compose logs -f cluster-init
```

### 2. Verify Installation

```bash
# Run comprehensive tests
tests/run_tests.sh

# Check cluster health
curl http://localhost:8080/status
```

### 3. Connect to the Database

```bash
# Writer endpoint (for INSERT/UPDATE/DELETE)
psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb"

# Reader endpoint (for SELECT queries)
psql "postgresql://synapsedb:changeme123@localhost:5001/synapsedb"

# Direct node access
psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb"  # Node 1
psql "postgresql://synapsedb:changeme123@localhost:5433/synapsedb"  # Node 2  
psql "postgresql://synapsedb:changeme123@localhost:5434/synapsedb"  # Node 3
```

## Architecture Overview

```
                    ┌─────────────┐
                    │  HAProxy    │
                    │             │
            ┌───────┤ Writer:5000 ├───────┐
            │       │ Reader:5001 │       │
            │       └─────────────┘       │
            │                             │
            │       ┌─────────────┐       │
            │       │ Query       │       │
            └───────┤ Router:8080 ├───────┘
                    └─────────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │  Node 1   │   │  Node 2   │   │  Node 3   │
    │(Writer)   │◄─►│           │◄─►│           │
    │PostgreSQL │   │PostgreSQL │   │PostgreSQL │
    │   :5432   │   │   :5433   │   │   :5434   │
    └───────────┘   └───────────┘   └───────────┘
            │               │               │
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │ Cluster   │   │ Cluster   │   │ Cluster   │
    │ Manager   │   │ Manager   │   │ Manager   │
    └───────────┘   └───────────┘   └───────────┘
```

### Core Components

1. **PostgreSQL Nodes**: 3-node cluster with logical replication
2. **Cluster Managers**: Node coordination and health monitoring
3. **Query Router**: Load balancing and intelligent query routing  
4. **HAProxy**: High availability endpoints
5. **Monitoring Stack**: Prometheus + Grafana

## Usage Examples

### Basic Operations

```sql
-- Create a table (automatically replicated)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price DECIMAL(10,2),
    embedding_data JSONB  -- Vector storage
);

-- Insert data (quorum-based write)
INSERT INTO products (name, description, price) VALUES 
('Laptop', 'High-performance laptop', 999.99);

-- Query data (reads from any healthy node)
SELECT * FROM products WHERE price > 500;
```

### Vector Operations

```sql
-- Create embeddings table
CREATE TABLE document_embeddings (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT,
    embedding_json JSONB  -- Vector as JSON array
);

-- Insert vector data
INSERT INTO document_embeddings (title, content, embedding_json) VALUES 
('AI Guide', 'Comprehensive AI guide...', '[0.1, 0.2, 0.3, ...]');

-- Vector similarity search (using JSON fallback)
SELECT title, cosine_similarity_json(embedding_json, '[0.1, 0.2, 0.3]') as similarity
FROM document_embeddings
ORDER BY similarity DESC
LIMIT 10;
```

### Distributed Operations

```sql
-- Execute with quorum requirement
SELECT * FROM execute_with_quorum(
    'INSERT INTO products (name, price) VALUES (''Test Product'', 99.99)',
    ARRAY['products'],
    ARRAY['{"name": "Test Product"}'::jsonb]
);

-- Check cluster status
SELECT node_name, status, is_writer, last_heartbeat 
FROM cluster_nodes;

-- Monitor replication lag
SELECT src.node_name as source, tgt.node_name as target, 
       lag_seconds, status
FROM replication_status rs
JOIN cluster_nodes src ON src.node_id = rs.source_node_id
JOIN cluster_nodes tgt ON tgt.node_id = rs.target_node_id;
```

## Monitoring

### Web Interfaces

- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **HAProxy Stats**: http://localhost:8404/stats
- **Query Router API**: http://localhost:8080/status

### Key Metrics

- Node health and availability
- Replication lag across nodes
- Query performance and routing
- Conflict resolution statistics
- Vector operation performance

## Testing

The system includes comprehensive tests covering:

```bash
# Run all tests
tests/run_tests.sh

# Individual test suites
psql -f tests/test_replication.sql
psql -f tests/test_vector_operations.sql
python3 tests/test_failover.py
```

Test categories:
- **Replication Tests**: DML/DDL replication and consistency
- **Vector Tests**: Vector operations and similarity search
- **Failover Tests**: Node failures and automatic recovery
- **Performance Tests**: Load testing and benchmarks
- **Consistency Tests**: Data consistency across nodes

## Configuration

### Environment Variables

```bash
# Database settings
POSTGRES_DB=synapsedb
POSTGRES_USER=synapsedb
POSTGRES_PASSWORD=changeme123

# Node settings
NODE_NAME=node1
DB_HOST=postgres1
DB_PORT=5432

# Router settings
ROUTER_PORT=8080
```

### Custom Configurations

- **Node configs**: `config/node[1-3]-config.yaml`
- **Router config**: `config/router-config.yaml`
- **PostgreSQL**: `config/postgresql.conf`
- **HAProxy**: `config/haproxy.cfg`

## Troubleshooting

### Common Issues

1. **Nodes not connecting**: Check network connectivity and firewall rules
2. **Replication lag**: Monitor `replication_status` table and check network
3. **Split brain**: Ensure quorum requirements are met
4. **Vector operations**: Verify pgvector extension installation

### Debugging Commands

```sql
-- Check cluster health
SELECT check_quorum();

-- View replication status  
SELECT * FROM replication_status;

-- Check conflict log
SELECT * FROM conflict_log ORDER BY resolved_at DESC LIMIT 10;

-- Monitor distributed transactions
SELECT * FROM distributed_transactions WHERE status = 'preparing';
```

### Log Locations

- PostgreSQL logs: `/var/lib/postgresql/data/log/`
- Cluster manager logs: `docker-compose logs cluster-manager1`
- Query router logs: `docker-compose logs query-router`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details