# SynapseDB Technical Specifications

## System Overview

SynapseDB is a production-grade distributed PostgreSQL database system providing enterprise features including consensus-based replication, automatic sharding, vector operations, and comprehensive operational tooling.

## Architecture Components

### 1. Raft Consensus System (`services/raft_consensus.py`)

**Purpose**: Provides distributed consensus for cluster coordination, leader election, and consistent decision-making.

**Key Features**:
- Complete Raft implementation with leader election
- Log replication with consistency guarantees
- Leadership lease system to prevent split-brain scenarios
- Election timeouts with randomized intervals
- Automatic failover and recovery

**Technical Details**:
- **Term Management**: Monotonic term numbers for ordering
- **Vote Requests**: Candidate nodes request votes from peers  
- **Log Replication**: Leaders replicate log entries to followers
- **Heartbeats**: Regular heartbeats maintain leadership
- **Safety**: Ensures at most one leader per term

**Configuration**:
```python
election_timeout_min = 3000  # ms
election_timeout_max = 6000  # ms
heartbeat_interval = 1000    # ms
leadership_lease_duration = 5000  # ms
```

### 2. Distributed Transaction Coordinator (`services/transaction_coordinator.py`)

**Purpose**: Manages distributed transactions using 2-phase commit protocol across multiple nodes.

**Key Features**:
- 2-phase commit (2PC) implementation
- Distributed transaction isolation
- Automatic timeout and recovery
- Participant node coordination
- Transaction persistence and recovery

**Technical Details**:
- **Phase 1 (Prepare)**: All participants vote on transaction
- **Phase 2 (Commit/Abort)**: Coordinator decides based on votes
- **Timeout Handling**: Automatic abort of timed-out transactions
- **Recovery**: Reload and continue interrupted transactions
- **Persistence**: Transaction state stored in database

**Transaction States**:
- `PREPARING`: Phase 1 in progress
- `PREPARED`: All participants voted YES
- `COMMITTING`: Phase 2 commit in progress
- `COMMITTED`: Transaction successfully committed
- `ABORTING`: Phase 2 abort in progress
- `ABORTED`: Transaction rolled back
- `FAILED`: Transaction failed with errors

### 3. Replication Manager (`services/replication_manager.py`)

**Purpose**: Manages bi-directional logical replication between PostgreSQL nodes using pglogical extension.

**Key Features**:
- Bi-directional logical replication
- Automatic subscription management
- Replication lag monitoring
- Failed subscription repair
- DDL replication coordination

**Technical Details**:
- **pglogical Integration**: Uses pglogical extension for logical replication
- **Subscription Topology**: Full mesh replication between all nodes
- **Lag Monitoring**: Continuous monitoring of replication lag
- **Repair Mechanism**: Automatic detection and repair of failed subscriptions
- **DDL Handling**: Coordinated schema changes across nodes

**Replication Flow**:
1. Create pglogical node on each database
2. Create replication set for tables
3. Add tables to replication set
4. Create subscriptions between nodes
5. Monitor and maintain replication health

### 4. Automatic Sharding Manager (`services/sharding_manager.py`)

**Purpose**: Provides automatic data partitioning and distribution across cluster nodes.

**Key Features**:
- Consistent hash-based sharding
- Automatic shard rebalancing
- Shard migration capabilities
- Virtual node mapping
- Load-aware shard placement

**Technical Details**:
- **Hash Function**: MD5-based consistent hashing
- **Virtual Nodes**: Multiple virtual nodes per physical node
- **Shard Count**: Default 256 shards for fine-grained distribution
- **Rebalancing**: Automatic migration when nodes added/removed
- **Health Monitoring**: Shard health tracking and migration

**Shard Operations**:
- **Create Shard**: Partition tables across nodes
- **Migrate Shard**: Move shard between nodes
- **Rebalance**: Redistribute shards for optimal balance
- **Health Check**: Monitor shard accessibility and performance

### 5. Schema Versioning Manager (`services/schema_manager.py`)

**Purpose**: Coordinates schema changes and migrations across the distributed cluster.

**Key Features**:
- Version-controlled schema changes
- Raft-coordinated migrations
- Dependency tracking
- Rollback capabilities
- Cross-node schema consistency

**Technical Details**:
- **Version Control**: Sequential version numbers for migrations
- **Raft Coordination**: Use Raft consensus for coordinated changes
- **Dependencies**: Track migration dependencies and ordering
- **Rollback**: Support for rolling back failed migrations
- **Validation**: Schema consistency verification

**Migration Process**:
1. Propose migration through Raft
2. Achieve consensus on migration order
3. Execute migration on all nodes
4. Verify consistency across nodes
5. Update schema version tracking

### 6. Vector Clock System (`services/vector_clock.py`)

**Purpose**: Provides causal consistency and advanced conflict resolution using vector clocks.

**Key Features**:
- Vector clock implementation for causal ordering
- Concurrent event detection
- Multiple conflict resolution strategies
- Event buffering and delivery ordering
- Causal consistency guarantees

**Technical Details**:
- **Vector Clocks**: Track causal relationships between events
- **Happens-Before**: Determine causal ordering of events
- **Concurrency Detection**: Identify concurrent events
- **Conflict Resolution**: Last-write-wins, merge, or custom strategies
- **Event Delivery**: Ensure causal delivery order

**Conflict Resolution Strategies**:
- **Last Write Wins**: Use timestamp to resolve conflicts
- **Merge**: Combine concurrent changes
- **Custom**: Application-specific resolution logic

### 7. Distributed Deadlock Detection (`services/deadlock_detector.py`)

**Purpose**: Detects and resolves deadlocks across the distributed system using wait-for graphs.

**Key Features**:
- Global wait-for graph construction
- Cycle detection algorithms
- Victim selection for deadlock resolution
- Transaction timeout handling
- Recovery from coordinator failures

**Technical Details**:
- **Wait-For Graphs**: Model transaction dependencies
- **Cycle Detection**: Use NetworkX for graph cycle detection
- **Global Detection**: Collect wait graphs from all nodes
- **Victim Selection**: Choose transaction to abort (youngest first)
- **Recovery**: Handle coordinator failures and orphaned transactions

**Deadlock Resolution Process**:
1. Detect local wait-for relationships
2. Exchange wait graphs between nodes
3. Construct global wait-for graph
4. Detect cycles in global graph
5. Select victim transaction to abort
6. Send abort request to appropriate node

### 8. SWIM Gossip Protocol (`services/gossip_protocol.py`)

**Purpose**: Network partition detection and failure monitoring using SWIM gossip protocol.

**Key Features**:
- Failure detection with direct and indirect pings
- Network partition detection
- Member state management
- Incarnation numbers for conflict resolution
- Configurable failure detection parameters

**Technical Details**:
- **Direct Ping**: Primary failure detection mechanism
- **Indirect Ping**: Secondary verification through other nodes
- **Incarnation Numbers**: Resolve conflicting member states
- **Suspicion**: Gradual failure detection with confirmations
- **Partition Detection**: Identify network partitions vs node failures

**Member States**:
- `ALIVE`: Node is healthy and reachable
- `SUSPECTED`: Node may have failed (indirect ping failed)
- `FAILED`: Node confirmed failed
- `LEFT`: Node gracefully left the cluster

### 9. TLS Certificate Management (`services/tls_manager.py`)

**Purpose**: Complete mutual TLS certificate lifecycle management for secure cluster communication.

**Key Features**:
- Internal Certificate Authority (CA)
- Automatic certificate generation and renewal
- Mutual TLS enforcement
- Certificate distribution
- Revocation and cleanup

**Technical Details**:
- **Internal CA**: Self-signed CA for cluster certificates
- **Certificate Types**: Server and client certificates with appropriate key usage
- **Automatic Renewal**: Renew certificates before expiration
- **SAN Support**: Subject Alternative Names for hostname/IP flexibility
- **Storage**: Secure certificate storage and distribution

**Certificate Lifecycle**:
1. Generate internal CA certificate
2. Create node certificates with proper SANs
3. Distribute certificates to nodes
4. Monitor certificate expiration
5. Automatic renewal and distribution
6. Revocation and cleanup of old certificates

### 10. Cluster-Aware Connection Pool (`services/connection_pool.py`)

**Purpose**: Intelligent connection pooling with load balancing, health monitoring, and failover.

**Key Features**:
- Multiple load balancing strategies
- Health monitoring and circuit breakers
- Role-aware routing (leader/follower)
- Connection utilization tracking
- Automatic failover

**Technical Details**:
- **Load Balancing**: Round-robin, least connections, weighted random, consistent hash
- **Health Checks**: Database connectivity, API health, replication lag
- **Circuit Breakers**: Prevent cascading failures
- **Role Detection**: Route writes to leaders, reads to any node
- **Connection Management**: Pool sizing and connection lifecycle

**Load Balancing Strategies**:
- **Round Robin**: Distribute requests evenly
- **Least Connections**: Route to node with fewest connections
- **Weighted Random**: Use health scores for weighted selection
- **Consistent Hash**: Route based on request hash for consistency

### 11. Distributed Backup System (`services/backup_manager.py`)

**Purpose**: Coordinated backup and restore across the distributed cluster with point-in-time recovery.

**Key Features**:
- Consistent cluster-wide backups
- Incremental and full backup support
- Multiple storage backends (Local, S3)
- WAL archiving and point-in-time recovery
- Backup verification and integrity checking

**Technical Details**:
- **Consistency**: Use LSN coordination for consistent backups
- **Compression**: Gzip compression for space efficiency
- **Incremental**: WAL-based incremental backups
- **Storage**: Pluggable storage backends with metadata
- **Recovery**: Point-in-time recovery with WAL replay

**Backup Types**:
- **Full Backup**: Complete database backup using pg_basebackup
- **Incremental**: WAL files since last backup
- **WAL Archive**: Continuous WAL file archiving

### 12. Rolling Update Manager (`services/rolling_update_manager.py`)

**Purpose**: Zero-downtime cluster updates with traffic draining and validation.

**Key Features**:
- Multiple update strategies (rolling, blue-green, canary)
- Traffic draining and restoration
- Health validation at each step
- Automatic rollback on failure
- Update progress tracking

**Technical Details**:
- **Strategies**: Rolling (sequential), blue-green (role-based), canary (test node first)
- **Traffic Draining**: Graceful connection draining before updates
- **Health Checks**: Multi-level validation after updates
- **Rollback**: Automatic rollback on validation failure
- **Batching**: Configurable batch sizes for rolling updates

**Update Process**:
1. Validate update configuration
2. Plan update batches based on strategy
3. For each node: drain traffic → update → validate → restore traffic
4. Verify cluster health after all updates
5. Rollback if any step fails

## Performance Characteristics

### Throughput
- **Write Operations**: Scales with number of leaders (typically 1-2)
- **Read Operations**: Scales linearly with number of nodes
- **Consensus Operations**: Limited by network latency and Raft rounds

### Latency
- **Local Operations**: PostgreSQL native performance
- **Distributed Transactions**: ~2x overhead for 2PC coordination
- **Consensus Operations**: Network RTT × 2 for Raft consensus
- **Cross-Shard Queries**: Additional network hops for data gathering

### Consistency Guarantees
- **ACID Transactions**: Full ACID within single node
- **Distributed Transactions**: ACID across nodes with 2PC
- **Replication**: Eventual consistency with conflict resolution
- **Consensus**: Strong consistency for cluster metadata

### Availability
- **Node Failures**: Automatic failover with Raft consensus
- **Network Partitions**: Majority partition remains available
- **Split Brain**: Prevented by Raft leadership and quorum requirements
- **Rolling Updates**: Zero downtime with proper traffic draining

## Deployment Requirements

### System Resources
- **CPU**: 4+ cores per node recommended
- **Memory**: 8GB+ per node (4GB for PostgreSQL, 4GB for services)
- **Storage**: SSD recommended for WAL and data
- **Network**: Gigabit Ethernet minimum, low-latency preferred

### PostgreSQL Version
- **Minimum**: PostgreSQL 12+ (for pglogical support)
- **Recommended**: PostgreSQL 14+ for optimal performance
- **Extensions**: pglogical, pgvector (optional), uuid-ossp

### Network Configuration
- **Ports**: 5432 (PostgreSQL), 8080 (API), 8081-8083 (internal services)
- **Firewall**: Allow inter-node communication on all service ports
- **DNS**: Proper hostname resolution for all nodes
- **TLS**: All inter-node communication secured with mutual TLS

### Storage Requirements
- **PostgreSQL Data**: Based on dataset size + indexes + WAL
- **Backup Storage**: 2-3x data size for backup retention
- **Certificate Storage**: Minimal (~10MB per node)
- **Log Storage**: 100MB-1GB per node depending on log level

## Monitoring and Observability

### Key Metrics
- **Raft**: Leader elections, log replication lag, term changes
- **Replication**: Logical replication lag, subscription health
- **Transactions**: 2PC success rate, transaction duration
- **Sharding**: Shard distribution, migration progress
- **Performance**: Query latency, throughput, connection utilization

### Health Checks
- **Database**: Connection, basic queries, replication status
- **Services**: Process health, resource utilization
- **Network**: Inter-node connectivity, partition detection
- **Certificates**: Expiration monitoring, rotation alerts

### Alerting
- **Critical**: Node failures, split-brain scenarios, data inconsistencies
- **Warning**: High replication lag, certificate expiration, resource usage
- **Info**: Successful failovers, completed migrations, backup status

## Operational Procedures

### Cluster Bootstrap
1. Deploy PostgreSQL with required extensions
2. Initialize first node as Raft leader
3. Add additional nodes and join cluster
4. Configure replication topology
5. Initialize sharding and distribute data

### Node Addition
1. Deploy new PostgreSQL instance
2. Add to cluster configuration
3. Join Raft cluster as follower
4. Set up replication subscriptions
5. Rebalance shards to include new node

### Node Removal
1. Drain traffic from node
2. Migrate shards to other nodes  
3. Remove replication subscriptions
4. Remove from Raft cluster
5. Update cluster configuration

### Backup and Recovery
1. Schedule regular full and incremental backups
2. Monitor backup success and storage usage
3. Test restore procedures regularly
4. Maintain backup retention policies
5. Document recovery procedures

### Updates and Maintenance
1. Plan maintenance windows
2. Use rolling update manager for zero-downtime updates
3. Validate each node after updates
4. Monitor cluster health during maintenance
5. Have rollback plan ready

## Security Considerations

### Network Security
- All inter-node communication uses mutual TLS
- Certificate rotation every 90 days
- Network segmentation for cluster traffic
- Firewall rules limiting external access

### Database Security
- Strong passwords for all database users
- Role-based access control
- Connection limits and timeouts
- SQL injection prevention
- Audit logging for sensitive operations

### Operational Security
- Secure certificate storage and distribution
- Encrypted backup storage
- Access controls for operational APIs
- Monitoring and alerting for security events
- Regular security updates and patches

## Limitations and Considerations

### Current Limitations
- Maximum 10 nodes per cluster (Raft scalability)
- Single-region deployment only
- PostgreSQL version dependencies
- Complex operational procedures
- Learning curve for distributed concepts

### Performance Considerations
- 2PC adds latency to distributed transactions
- Raft consensus can be bottleneck for metadata operations
- Cross-shard queries require careful optimization
- Network latency affects all distributed operations

### Operational Complexity
- Multiple components require monitoring
- Troubleshooting distributed issues challenging
- Backup and recovery procedures complex
- Rolling updates require careful planning
- Certificate management adds operational overhead

### Future Enhancements
- Multi-region support with cross-region replication
- Automatic cluster scaling based on load
- Advanced query optimization across shards
- Integration with cloud provider managed services
- Simplified deployment and management tooling