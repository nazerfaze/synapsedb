# Multi-Server Deployment Guide

## Overview

SynapseDB supports true horizontal scaling across multiple physical servers. Here are the deployment strategies:

## Strategy 1: Manual Multi-Server Setup

### Prerequisites
- 3+ physical servers with Docker installed
- Network connectivity between all servers
- Same subnet or proper routing configured
- Ports 5432, 8080-8083 open between servers

### Step 1: Prepare Servers

**Server 1 (10.0.1.10):**
```bash
# Clone SynapseDB
git clone <repo> synapsedb
cd synapsedb

# Create node1-only configuration
cp docker-compose.yml docker-compose.node1.yml
# Edit to keep only postgres1 and cluster-manager1 services
# Update network configuration for multi-server
```

**Server 2 (10.0.1.11):**
```bash
# Same setup, but create node2 configuration
cp docker-compose.yml docker-compose.node2.yml
# Keep only postgres2 and cluster-manager2 services
```

**Server 3 (10.0.1.12):**
```bash
# Same setup, but create node3 configuration  
cp docker-compose.yml docker-compose.node3.yml
# Keep only postgres3 and cluster-manager3 services
```

### Step 2: Update Network Configuration

Update each server's compose file:
```yaml
# In each docker-compose.nodeX.yml
environment:
  CLUSTER_NODES: node1:10.0.1.10:5432,node2:10.0.1.11:5432,node3:10.0.1.12:5432

networks:
  synapsedb_network:
    driver: bridge
    # Remove ipam to use default networking
```

### Step 3: Deploy Sequentially
```bash
# Server 1 (start first - becomes initial leader)
docker-compose -f docker-compose.node1.yml up -d

# Wait for Server 1 to be healthy, then Server 2
docker-compose -f docker-compose.node2.yml up -d

# Finally Server 3
docker-compose -f docker-compose.node3.yml up -d

# Run cluster initialization from any server
docker run --rm --network host synapsedb/services:latest python /app/services/cluster_init.py
```

## Strategy 2: Docker Swarm (Recommended)

### Step 1: Initialize Swarm
```bash
# On manager node (Server 1)
docker swarm init --advertise-addr 10.0.1.10

# Join worker nodes (Servers 2 & 3)
docker swarm join --token <worker-token> 10.0.1.10:2377
```

### Step 2: Create Swarm-Compatible Compose
```yaml
# docker-compose.swarm.yml
version: '3.8'

services:
  postgres1:
    # ... existing config ...
    deploy:
      placement:
        constraints:
          - node.hostname == server1
      replicas: 1
      resources:
        limits:
          memory: 4G
          cpus: '2'
        reservations:
          memory: 2G
          cpus: '1'

  postgres2:
    # ... existing config ...
    deploy:
      placement:
        constraints:
          - node.hostname == server2
      replicas: 1

  postgres3:
    # ... existing config ...
    deploy:
      placement:
        constraints:
          - node.hostname == server3  
      replicas: 1

networks:
  synapsedb_network:
    driver: overlay
    attachable: true
```

### Step 3: Deploy to Swarm
```bash
# Deploy entire stack
docker stack deploy -c docker-compose.swarm.yml synapsedb

# Check status
docker service ls
docker service logs synapsedb_postgres1
```

## Strategy 3: Kubernetes Deployment

### Step 1: Create Kubernetes Manifests
```yaml
# postgres-cluster.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres-cluster
spec:
  serviceName: postgres-cluster
  replicas: 3
  selector:
    matchLabels:
      app: postgres-cluster
  template:
    metadata:
      labels:
        app: postgres-cluster
    spec:
      containers:
      - name: postgres
        image: synapsedb/postgres:latest
        env:
        - name: POSTGRES_DB
          value: synapsedb
        # ... more config
      - name: cluster-manager
        image: synapsedb/services:latest
        # ... manager config
```

### Step 2: Deploy to Kubernetes
```bash
# Apply manifests
kubectl apply -f kubernetes/

# Check pods
kubectl get pods -l app=postgres-cluster

# Check services
kubectl get services
```

## Adding More Nodes (Horizontal Scaling)

### Automatic Scaling Process

1. **Add new server to infrastructure**
2. **Deploy new PostgreSQL + services**
3. **Update cluster configuration**
4. **Rebalance shards automatically**

```bash
# Example: Adding Node 4
# 1. Deploy new node
docker-compose -f scaling/add-node-example.yml up -d

# 2. Register with existing cluster
curl -X POST http://10.0.1.10:8080/cluster/add-node \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node4", "host": "10.0.1.13", "port": 5432}'

# 3. Trigger shard rebalancing
curl -X POST http://10.0.1.10:8080/sharding/rebalance

# 4. Setup replication to new node
curl -X POST http://10.0.1.10:8080/replication/add-node \
  -d '{"node_id": "node4"}'
```

### Scaling Characteristics

**Current Limits:**
- **Raft consensus**: Optimized for 3-7 nodes (odd numbers preferred)
- **pglogical**: Supports 10+ nodes but network overhead increases
- **Sharding**: Scales to 100+ nodes with proper shard distribution

**Performance Scaling:**
- **Read Performance**: Linear scaling (more nodes = more read capacity)
- **Write Performance**: Limited by consensus overhead (3-5x improvement max)
- **Storage**: Linear scaling (distribute data across more nodes)

## Production Multi-Server Recommendations

### Infrastructure Requirements
```yaml
Minimum per server:
  CPU: 4 cores
  RAM: 8GB
  Storage: 100GB SSD
  Network: 1Gbps, <5ms latency between nodes

Recommended per server:
  CPU: 8+ cores  
  RAM: 16GB+
  Storage: NVMe SSD, 1000+ IOPS
  Network: 10Gbps, <1ms latency
```

### High Availability Setup
```bash
# 5-node cluster for production
Region 1: Servers 1, 2 (primary data center)
Region 2: Servers 3, 4 (secondary data center)  
Region 3: Server 5 (tie-breaker for consensus)

# This survives entire data center failure
# Automatic failover maintains service
```

### Load Balancer Configuration
```yaml
# HAProxy for multi-server (update haproxy.cfg)
backend postgres-read
  balance roundrobin
  server node1 10.0.1.10:5432 check
  server node2 10.0.1.11:5432 check
  server node3 10.0.1.12:5432 check
  server node4 10.0.1.13:5432 check
  server node5 10.0.1.14:5432 check

backend postgres-write
  balance first
  server node1 10.0.1.10:5432 check
  server node2 10.0.1.11:5432 check backup
  server node3 10.0.1.12:5432 check backup
```

## Monitoring Multi-Server Deployments

### Centralized Monitoring
```bash
# Deploy monitoring stack on dedicated server
# Or use cloud monitoring (Datadog, New Relic, etc.)

# Grafana shows cluster-wide metrics
# Prometheus scrapes all nodes
# Alerting for node failures, split-brain, etc.
```

### Health Checks
```bash
# Check cluster health across all servers
for node in 10.0.1.10 10.0.1.11 10.0.1.12; do
  curl -s http://$node:8080/raft/status | jq '.role'
done

# Should show: leader, follower, follower
```

## Operational Procedures

### Rolling Updates Across Servers
```bash
# Update nodes one at a time
# 1. Drain traffic from node
curl -X POST http://10.0.1.10:8080/traffic/drain

# 2. Update node software
docker-compose pull && docker-compose up -d

# 3. Validate health
curl http://10.0.1.10:8080/health

# 4. Restore traffic
curl -X POST http://10.0.1.10:8080/traffic/restore

# Repeat for each node
```

### Backup Strategy Multi-Server
```bash
# Coordinated backups across all nodes
curl -X POST http://10.0.1.10:8080/backup/create-distributed

# Backup data is distributed and replicated
# Can restore even if multiple servers fail
```

---

## Summary

**Single Machine (Current)**: Perfect for development, testing, and small-medium workloads
**Multi-Server Manual**: Full control, requires more setup
**Docker Swarm**: Easier orchestration, good for 3-10 servers
**Kubernetes**: Enterprise-grade, best for large deployments

**Horizontal Scaling**: Yes, supports adding nodes dynamically with automatic shard rebalancing

Choose the deployment strategy based on your scale and operational requirements!