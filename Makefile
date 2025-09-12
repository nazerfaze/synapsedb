# SynapseDB Makefile
# Convenient commands for managing the distributed PostgreSQL cluster

.PHONY: help build start stop restart status test clean logs shell

# Default target
help: ## Show this help message
	@echo "SynapseDB - Distributed PostgreSQL with Vector Support"
	@echo "====================================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Build targets
build: ## Build all Docker images
	@echo "🔨 Building SynapseDB Docker images..."
	docker-compose build --parallel

build-nocache: ## Build all Docker images without cache
	@echo "🔨 Building SynapseDB Docker images (no cache)..."
	docker-compose build --no-cache --parallel

# Deployment targets
start: ## Start the SynapseDB cluster
	@echo "🚀 Starting SynapseDB cluster..."
	docker-compose up -d
	@echo "⏳ Waiting for cluster initialization..."
	@echo "📊 Monitor initialization: docker-compose logs -f cluster-init"

stop: ## Stop the SynapseDB cluster
	@echo "🛑 Stopping SynapseDB cluster..."
	docker-compose down

restart: ## Restart the SynapseDB cluster
	@echo "🔄 Restarting SynapseDB cluster..."
	docker-compose restart

destroy: ## Destroy cluster and remove all data
	@echo "⚠️  WARNING: This will destroy all data!"
	@read -p "Are you sure? [y/N]: " -n 1 -r && echo && [[ $$REPLY =~ ^[Yy]$$ ]]
	docker-compose down -v --remove-orphans
	docker system prune -f

# Status and monitoring
status: ## Show cluster status
	@echo "📊 SynapseDB Cluster Status"
	@echo "=========================="
	@echo ""
	@echo "Services:"
	@docker-compose ps
	@echo ""
	@echo "Health Checks:"
	@curl -s http://localhost:8080/status 2>/dev/null | python3 -m json.tool || echo "❌ Query Router not responding"

health: ## Check cluster health
	@echo "🏥 Health Check Results:"
	@echo "======================="
	@echo ""
	@echo "Node 1 (5432):"
	@psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb" -c "SELECT 'OK' as status, check_quorum() as has_quorum;" 2>/dev/null || echo "❌ Node 1 unavailable"
	@echo ""
	@echo "Node 2 (5433):"
	@psql "postgresql://synapsedb:changeme123@localhost:5433/synapsedb" -c "SELECT 'OK' as status, check_quorum() as has_quorum;" 2>/dev/null || echo "❌ Node 2 unavailable"
	@echo ""
	@echo "Node 3 (5434):"
	@psql "postgresql://synapsedb:changeme123@localhost:5434/synapsedb" -c "SELECT 'OK' as status, check_quorum() as has_quorum;" 2>/dev/null || echo "❌ Node 3 unavailable"
	@echo ""
	@echo "Writer Endpoint (5000):"
	@psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb" -c "SELECT 'Writer OK' as status;" 2>/dev/null || echo "❌ Writer endpoint unavailable"
	@echo ""
	@echo "Reader Endpoint (5001):"
	@psql "postgresql://synapsedb:changeme123@localhost:5001/synapsedb" -c "SELECT 'Reader OK' as status;" 2>/dev/null || echo "❌ Reader endpoint unavailable"

# Testing targets
test: ## Run all tests
	@echo "🧪 Running SynapseDB test suite..."
	@if [ ! -x "tests/run_tests.sh" ]; then chmod +x tests/run_tests.sh; fi
	./tests/run_tests.sh

test-replication: ## Run replication tests only
	@echo "🔄 Testing replication functionality..."
	psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb" -f tests/test_replication.sql

test-vector: ## Run vector operation tests
	@echo "🧠 Testing vector operations..."
	psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb" -f tests/test_vector_operations.sql

test-failover: ## Run failover tests
	@echo "🔴 Testing failover scenarios..."
	python3 tests/test_failover.py

benchmark: ## Run performance benchmarks
	@echo "⚡ Running performance benchmarks..."
	@echo "Creating benchmark data..."
	@psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb" -c "CREATE TABLE IF NOT EXISTS benchmark_test AS SELECT i as id, 'test_data_' || i as data FROM generate_series(1, 10000) i;"
	@echo "Testing write performance..."
	@time psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb" -c "INSERT INTO benchmark_test SELECT i, 'benchmark_' || i FROM generate_series(10001, 20000) i;"
	@echo "Testing read performance..."
	@time psql "postgresql://synapsedb:changeme123@localhost:5001/synapsedb" -c "SELECT COUNT(*) FROM benchmark_test WHERE data LIKE 'benchmark_%';"
	@echo "Cleaning up..."
	@psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb" -c "DROP TABLE benchmark_test;"

# Database connection shortcuts
connect-writer: ## Connect to writer endpoint
	psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb"

connect-reader: ## Connect to reader endpoint
	psql "postgresql://synapsedb:changeme123@localhost:5001/synapsedb"

connect-node1: ## Connect to node 1
	psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb"

connect-node2: ## Connect to node 2
	psql "postgresql://synapsedb:changeme123@localhost:5433/synapsedb"

connect-node3: ## Connect to node 3
	psql "postgresql://synapsedb:changeme123@localhost:5434/synapsedb"

# Monitoring shortcuts
logs: ## Show logs for all services
	docker-compose logs -f

logs-postgres: ## Show PostgreSQL logs
	docker-compose logs -f postgres1 postgres2 postgres3

logs-cluster: ## Show cluster manager logs
	docker-compose logs -f cluster-manager1 cluster-manager2 cluster-manager3

logs-router: ## Show query router logs
	docker-compose logs -f query-router

grafana: ## Open Grafana dashboard
	@echo "🎯 Opening Grafana dashboard..."
	@echo "URL: http://localhost:3000"
	@echo "Login: admin/admin123"
	@command -v open >/dev/null 2>&1 && open http://localhost:3000 || echo "Open http://localhost:3000 in your browser"

prometheus: ## Open Prometheus interface
	@echo "📊 Opening Prometheus interface..."
	@echo "URL: http://localhost:9090"
	@command -v open >/dev/null 2>&1 && open http://localhost:9090 || echo "Open http://localhost:9090 in your browser"

haproxy-stats: ## Open HAProxy statistics
	@echo "📈 Opening HAProxy statistics..."
	@echo "URL: http://localhost:8404/stats"
	@command -v open >/dev/null 2>&1 && open http://localhost:8404/stats || echo "Open http://localhost:8404/stats in your browser"

# Development targets
shell-node1: ## Shell into node 1 container
	docker-compose exec postgres1 /bin/sh

shell-node2: ## Shell into node 2 container
	docker-compose exec postgres2 /bin/sh

shell-node3: ## Shell into node 3 container
	docker-compose exec postgres3 /bin/sh

shell-router: ## Shell into query router container
	docker-compose exec query-router /bin/sh

# Data management
backup: ## Create cluster backup
	@echo "💾 Creating cluster backup..."
	@mkdir -p backups
	@pg_dump "postgresql://synapsedb:changeme123@localhost:5432/synapsedb" > backups/synapsedb_backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup created in backups/ directory"

restore: ## Restore from backup (requires BACKUP_FILE variable)
	@if [ -z "$(BACKUP_FILE)" ]; then echo "❌ Please specify BACKUP_FILE=path/to/backup.sql"; exit 1; fi
	@echo "📥 Restoring from $(BACKUP_FILE)..."
	@psql "postgresql://synapsedb:changeme123@localhost:5432/synapsedb" < $(BACKUP_FILE)
	@echo "✅ Restore completed"

seed-data: ## Load sample data for testing
	@echo "🌱 Loading sample data..."
	@psql "postgresql://synapsedb:changeme123@localhost:5000/synapsedb" -c "\
		INSERT INTO users (username, email, profile_data) VALUES \
			('alice', 'alice@example.com', '{\"role\": \"admin\"}'), \
			('bob', 'bob@example.com', '{\"role\": \"user\"}'), \
			('charlie', 'charlie@example.com', '{\"role\": \"user\"}') \
		ON CONFLICT (username) DO NOTHING; \
		INSERT INTO products (name, description, price, category) VALUES \
			('Laptop', 'High-performance laptop', 999.99, 'electronics'), \
			('Mouse', 'Wireless mouse', 29.99, 'accessories'), \
			('Keyboard', 'Mechanical keyboard', 79.99, 'accessories') \
		ON CONFLICT DO NOTHING;"
	@echo "✅ Sample data loaded"

# Cleanup targets
clean: ## Clean up Docker resources
	@echo "🧹 Cleaning up Docker resources..."
	docker-compose down --remove-orphans
	docker system prune -f
	docker volume prune -f

clean-logs: ## Clean up log files
	@echo "🧹 Cleaning up log files..."
	rm -f test_results_*.log
	rm -f *.log

# Information targets
info: ## Show connection information
	@echo "🔗 SynapseDB Connection Information"
	@echo "=================================="
	@echo ""
	@echo "Database Endpoints:"
	@echo "  Writer:  postgresql://synapsedb:changeme123@localhost:5000/synapsedb"
	@echo "  Reader:  postgresql://synapsedb:changeme123@localhost:5001/synapsedb"
	@echo ""
	@echo "Direct Node Access:"
	@echo "  Node 1:  postgresql://synapsedb:changeme123@localhost:5432/synapsedb"
	@echo "  Node 2:  postgresql://synapsedb:changeme123@localhost:5433/synapsedb"
	@echo "  Node 3:  postgresql://synapsedb:changeme123@localhost:5434/synapsedb"
	@echo ""
	@echo "Management Interfaces:"
	@echo "  Query Router:    http://localhost:8080/status"
	@echo "  Grafana:         http://localhost:3000 (admin/admin123)"
	@echo "  Prometheus:      http://localhost:9090"
	@echo "  HAProxy Stats:   http://localhost:8404/stats"
	@echo ""
	@echo "Useful Commands:"
	@echo "  make test        - Run all tests"
	@echo "  make health      - Check cluster health"
	@echo "  make connect-writer - Connect to writer endpoint"
	@echo "  make logs        - View all service logs"

version: ## Show version information
	@echo "SynapseDB v1.0.0"
	@echo "Distributed PostgreSQL with Vector Support"
	@echo ""
	@echo "Components:"
	@echo "  PostgreSQL: 15"
	@echo "  Python:     3.11"
	@echo "  HAProxy:    2.8"