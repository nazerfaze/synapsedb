#!/bin/bash

# SynapseDB Startup Script
# Starts the complete distributed database system

set -e

echo "🚀 Starting SynapseDB Distributed Database System..."

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p certs logs data/{backups,raft,sharding}
chmod 755 certs logs data

# Check if Docker and Docker Compose are available
echo "🔍 Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed or not in PATH"
    echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

# Use docker compose or docker-compose depending on what's available
DOCKER_COMPOSE="docker compose"
if ! docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
fi

echo "✅ Prerequisites check passed"

# Stop any existing containers
echo "🛑 Stopping any existing SynapseDB containers..."
$DOCKER_COMPOSE down -v 2>/dev/null || true

# Build and start the system
echo "🔨 Building Docker images..."
$DOCKER_COMPOSE build --parallel

echo "🚀 Starting SynapseDB cluster..."
$DOCKER_COMPOSE up -d

echo "⏳ Waiting for services to initialize..."

# Wait for PostgreSQL nodes to be healthy
echo "🔍 Waiting for PostgreSQL nodes..."
for i in {1..60}; do
    if docker exec synapsedb-postgres1 pg_isready -U synapsedb -d synapsedb >/dev/null 2>&1 && \
       docker exec synapsedb-postgres2 pg_isready -U synapsedb -d synapsedb >/dev/null 2>&1 && \
       docker exec synapsedb-postgres3 pg_isready -U synapsedb -d synapsedb >/dev/null 2>&1; then
        echo "✅ All PostgreSQL nodes are ready"
        break
    fi
    echo "⏳ PostgreSQL nodes initializing... ($i/60)"
    sleep 5
done

# Wait for cluster initialization to complete
echo "🔧 Waiting for cluster initialization..."
for i in {1..30}; do
    if $DOCKER_COMPOSE logs cluster-init | grep -q "Cluster initialization successful" 2>/dev/null; then
        echo "✅ Cluster initialization completed"
        break
    fi
    echo "⏳ Cluster initializing... ($i/30)"
    sleep 10
done

# Show status
echo ""
echo "🎉 SynapseDB is starting up!"
echo ""
echo "📊 Service Status:"
$DOCKER_COMPOSE ps

echo ""
echo "🔗 Connection Endpoints:"
echo "  📖 Read Endpoint (HAProxy):  localhost:5001"
echo "  ✏️  Write Endpoint (HAProxy): localhost:5002"
echo "  🔧 Direct Node 1:           localhost:5432"
echo "  🔧 Direct Node 2:           localhost:5433" 
echo "  🔧 Direct Node 3:           localhost:5434"
echo ""
echo "🌐 Web Interfaces:"
echo "  📈 Grafana:       http://localhost:3000 (admin/admin123)"
echo "  📊 Prometheus:    http://localhost:9090"
echo "  🔄 HAProxy Stats: http://localhost:8404/stats"
echo "  🖥️  Node APIs:     http://localhost:8080 (8084, 8088)"
echo ""
echo "📝 Quick Start:"
echo "  # Connect to read endpoint"
echo "  psql 'postgresql://synapsedb:synapsedb@localhost:5001/synapsedb'"
echo ""
echo "  # Connect to write endpoint"  
echo "  psql 'postgresql://synapsedb:synapsedb@localhost:5002/synapsedb'"
echo ""
echo "  # Run integration tests"
echo "  python tests/integration/test_full_system.py"
echo ""
echo "📋 Monitoring:"
echo "  # View logs"
echo "  $DOCKER_COMPOSE logs -f"
echo ""
echo "  # Check cluster status"
echo "  curl http://localhost:8080/raft/status"
echo ""

# Optionally run basic health check
if [[ "$1" == "--health-check" ]]; then
    echo "🏥 Running health check..."
    sleep 5
    
    # Test database connectivity
    if docker exec synapsedb-postgres1 psql -U synapsedb -d synapsedb -c "SELECT 'Node 1 OK' as status;" >/dev/null 2>&1; then
        echo "✅ Node 1 database connectivity: OK"
    else
        echo "❌ Node 1 database connectivity: FAILED"
    fi
    
    if docker exec synapsedb-postgres2 psql -U synapsedb -d synapsedb -c "SELECT 'Node 2 OK' as status;" >/dev/null 2>&1; then
        echo "✅ Node 2 database connectivity: OK"
    else
        echo "❌ Node 2 database connectivity: FAILED"
    fi
    
    if docker exec synapsedb-postgres3 psql -U synapsedb -d synapsedb -c "SELECT 'Node 3 OK' as status;" >/dev/null 2>&1; then
        echo "✅ Node 3 database connectivity: OK"
    else
        echo "❌ Node 3 database connectivity: FAILED"
    fi
fi

echo ""
echo "✨ SynapseDB is now running! Check the logs with: $DOCKER_COMPOSE logs -f"
echo "🛑 To stop: $DOCKER_COMPOSE down"
echo "🗑️  To reset: $DOCKER_COMPOSE down -v (removes all data)"