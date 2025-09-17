#!/bin/bash

# SynapseDB Multi-Server Deployment Script
# Deploys one node per physical server

set -e

echo "🌐 SynapseDB Multi-Server Deployment"
echo "=================================================="

# Configuration - UPDATE THESE WITH YOUR ACTUAL SERVER IPs
SERVER1_IP="10.0.1.10"  # Replace with actual Server 1 IP
SERVER2_IP="10.0.1.11"  # Replace with actual Server 2 IP  
SERVER3_IP="10.0.1.12"  # Replace with actual Server 3 IP

SERVER1_USER="root"     # SSH user for servers
SERVER2_USER="root"
SERVER3_USER="root"

echo "📋 Configuration:"
echo "  Server 1 (Node 1): $SERVER1_IP"
echo "  Server 2 (Node 2): $SERVER2_IP"
echo "  Server 3 (Node 3): $SERVER3_IP"
echo ""

# Function to update IP addresses in compose files
update_cluster_ips() {
    local file=$1
    echo "📝 Updating cluster IPs in $file"
    
    # Update CLUSTER_NODES environment variable with real IPs
    sed -i "s/SERVER1_IP/$SERVER1_IP/g" "$file"
    sed -i "s/SERVER2_IP/$SERVER2_IP/g" "$file"  
    sed -i "s/SERVER3_IP/$SERVER3_IP/g" "$file"
}

# Function to deploy to a server
deploy_to_server() {
    local server_ip=$1
    local server_user=$2
    local compose_file=$3
    local node_name=$4
    
    echo "🚀 Deploying $node_name to $server_ip"
    
    # Copy entire project to server
    echo "  📂 Copying project files..."
    rsync -avz --exclude='.git' --exclude='*.log' \
          ./ $server_user@$server_ip:~/synapsedb/
    
    # Update IPs in compose file on server
    ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER1_IP/$SERVER1_IP/g' $compose_file"
    ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER2_IP/$SERVER2_IP/g' $compose_file"
    ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER3_IP/$SERVER3_IP/g' $compose_file"
    
    # Also update HAProxy config if it exists
    if ssh $server_user@$server_ip "cd ~/synapsedb && ls config/haproxy_multiserver.cfg" 2>/dev/null; then
        ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER1_IP/$SERVER1_IP/g' config/haproxy_multiserver.cfg"
        ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER2_IP/$SERVER2_IP/g' config/haproxy_multiserver.cfg"
        ssh $server_user@$server_ip "cd ~/synapsedb && sed -i 's/SERVER3_IP/$SERVER3_IP/g' config/haproxy_multiserver.cfg"
    fi
    
    # Deploy on server
    echo "  🔨 Building and starting services..."
    ssh $server_user@$server_ip "cd ~/synapsedb && docker-compose -f $compose_file build"
    ssh $server_user@$server_ip "cd ~/synapsedb && docker-compose -f $compose_file up -d"
    
    echo "  ✅ $node_name deployed successfully"
}

# Function to check if server is reachable
check_server() {
    local server_ip=$1
    local server_user=$2
    echo "🔍 Checking connectivity to $server_ip..."
    
    if ssh -o ConnectTimeout=10 $server_user@$server_ip "echo 'Server reachable'" >/dev/null 2>&1; then
        echo "  ✅ $server_ip is reachable"
        return 0
    else
        echo "  ❌ $server_ip is NOT reachable"
        return 1
    fi
}

# Function to check if Docker is installed
check_docker() {
    local server_ip=$1
    local server_user=$2
    echo "🐳 Checking Docker on $server_ip..."
    
    if ssh $server_user@$server_ip "docker --version && docker-compose --version" >/dev/null 2>&1; then
        echo "  ✅ Docker is installed"
        return 0
    else
        echo "  ❌ Docker is NOT installed"
        return 1
    fi
}

# Main deployment process
main() {
    echo "🔍 Pre-deployment checks..."
    
    # Check connectivity to all servers
    if ! check_server $SERVER1_IP $SERVER1_USER || \
       ! check_server $SERVER2_IP $SERVER2_USER || \
       ! check_server $SERVER3_IP $SERVER3_USER; then
        echo "❌ Cannot reach all servers. Please check IP addresses and SSH access."
        exit 1
    fi
    
    # Check Docker on all servers
    if ! check_docker $SERVER1_IP $SERVER1_USER || \
       ! check_docker $SERVER2_IP $SERVER2_USER || \
       ! check_docker $SERVER3_IP $SERVER3_USER; then
        echo "❌ Docker not installed on all servers. Please install Docker first."
        exit 1
    fi
    
    echo "✅ All pre-deployment checks passed!"
    echo ""
    
    # Stop any existing deployments
    echo "🛑 Stopping any existing SynapseDB deployments..."
    ssh $SERVER1_USER@$SERVER1_IP "cd ~/synapsedb 2>/dev/null && docker-compose -f docker-compose.node1.yml down" 2>/dev/null || true
    ssh $SERVER2_USER@$SERVER2_IP "cd ~/synapsedb 2>/dev/null && docker-compose -f docker-compose.node2.yml down" 2>/dev/null || true
    ssh $SERVER3_USER@$SERVER3_IP "cd ~/synapsedb 2>/dev/null && docker-compose -f docker-compose.node3.yml down" 2>/dev/null || true
    
    echo ""
    echo "🚀 Starting multi-server deployment..."
    echo ""
    
    # Deploy to each server (Node 1 first as it becomes initial leader)
    deploy_to_server $SERVER1_IP $SERVER1_USER "docker-compose.node1.yml" "Node 1 (Leader)"
    
    echo ""
    echo "⏳ Waiting 30 seconds for Node 1 to initialize..."
    sleep 30
    
    deploy_to_server $SERVER2_IP $SERVER2_USER "docker-compose.node2.yml" "Node 2"
    
    echo ""
    echo "⏳ Waiting 15 seconds before deploying Node 3..."
    sleep 15
    
    deploy_to_server $SERVER3_IP $SERVER3_USER "docker-compose.node3.yml" "Node 3"
    
    echo ""
    echo "⏳ Waiting for cluster to stabilize..."
    sleep 30
    
    # Initialize the cluster
    echo "🔧 Initializing distributed cluster..."
    ssh $SERVER1_USER@$SERVER1_IP "cd ~/synapsedb && docker run --rm --network synapsedb.com_synapsedb_network -e CLUSTER_NODES=node1:$SERVER1_IP:5432,node2:$SERVER2_IP:5432,node3:$SERVER3_IP:5432 -e DB_USER=synapsedb -e DB_PASSWORD=synapsedb -e DB_NAME=synapsedb -v ~/synapsedb/services:/app/services synapsedb_services:latest python /app/services/cluster_init.py"
    
    echo ""
    echo "📊 Deployment Status:"
    echo "=================================================="
    
    # Check status on each server
    echo "Server 1 ($SERVER1_IP):"
    ssh $SERVER1_USER@$SERVER1_IP "cd ~/synapsedb && docker-compose -f docker-compose.node1.yml ps" | grep -E "(postgres1|cluster-manager1)" || echo "  Not running"
    
    echo ""
    echo "Server 2 ($SERVER2_IP):"
    ssh $SERVER2_USER@$SERVER2_IP "cd ~/synapsedb && docker-compose -f docker-compose.node2.yml ps" | grep -E "(postgres2|cluster-manager2)" || echo "  Not running"
    
    echo ""
    echo "Server 3 ($SERVER3_IP):"
    ssh $SERVER3_USER@$SERVER3_IP "cd ~/synapsedb && docker-compose -f docker-compose.node3.yml ps" | grep -E "(postgres3|cluster-manager3)" || echo "  Not running"
    
    echo ""
    echo "🎉 Multi-Server Deployment Complete!"
    echo "=================================================="
    echo ""
    echo "🔗 Connection Endpoints:"
    echo "  📖 Read Endpoint:  $SERVER3_IP:5001 (if HAProxy deployed)"
    echo "  ✏️  Write Endpoint: $SERVER3_IP:5002 (if HAProxy deployed)"
    echo "  🔧 Direct Nodes:"
    echo "    Node 1: $SERVER1_IP:5432"
    echo "    Node 2: $SERVER2_IP:5432"
    echo "    Node 3: $SERVER3_IP:5432"
    echo ""
    echo "🌐 Management Interfaces:"
    echo "  📈 Grafana:       http://$SERVER3_IP:3000 (admin/admin123)"
    echo "  🔄 HAProxy Stats: http://$SERVER3_IP:8404/stats"
    echo "  🖥️  Cluster APIs:"
    echo "    Node 1: http://$SERVER1_IP:8080/raft/status"
    echo "    Node 2: http://$SERVER2_IP:8080/raft/status" 
    echo "    Node 3: http://$SERVER3_IP:8080/raft/status"
    echo ""
    echo "✅ Test connection:"
    echo "  psql 'postgresql://synapsedb:synapsedb@$SERVER1_IP:5432/synapsedb'"
    echo ""
    echo "🧪 Run tests:"
    echo "  # Copy test file to any server and run"
    echo "  python tests/integration/test_full_system.py"
}

# Check if configuration looks correct
echo "⚠️  IMPORTANT: Please verify the server IPs above are correct!"
echo "   If not, edit the variables at the top of this script."
echo ""
read -p "Continue with deployment? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    main
else
    echo "Deployment cancelled."
    exit 0
fi