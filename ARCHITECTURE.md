# SynapseDB: Distributed Postgres with pgvector

## System Architecture

### Core Components

1. **Node Manager**: Coordinates cluster membership and health
2. **Replication Coordinator**: Captures and applies DML/DDL changes
3. **Schema Propagator**: Ensures schema consistency across nodes
4. **Conflict Resolver**: Implements last-write-wins resolution
5. **Query Router**: Routes queries based on node health and quorum
6. **Vector Index Manager**: Maintains pgvector consistency

### Node Roles

- **Primary Writer**: Accepts writes and coordinates replication
- **Secondary Nodes**: Apply replicated changes, serve reads
- **Quorum Members**: Participate in write consensus

### Data Flow

1. Client connects to any node via load balancer
2. Writes require quorum (2/3 nodes) for commit
3. DML changes captured via WAL logical replication
4. DDL changes captured via event triggers
5. Changes streamed to all nodes asynchronously
6. Conflicts resolved using timestamps

### Consistency Model

- **Strong consistency**: For quorum writes
- **Eventual consistency**: For cross-node replication
- **Read-your-writes**: Guaranteed on same node
- **Monotonic reads**: Guaranteed across cluster