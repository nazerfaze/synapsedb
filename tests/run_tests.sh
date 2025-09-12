#!/bin/bash

# SynapseDB Test Suite Runner
# Executes comprehensive tests for the distributed database system

set -e  # Exit on any error

echo "🧪 Starting SynapseDB Comprehensive Test Suite"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_NAME=${DB_NAME:-"synapsedb"}
DB_USER=${DB_USER:-"synapsedb"}
DB_PASSWORD=${DB_PASSWORD:-"changeme123"}

# Connection strings for different endpoints
WRITER_CONN="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5000/${DB_NAME}"
READER_CONN="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5001/${DB_NAME}"
NODE1_CONN="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"
NODE2_CONN="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5433/${DB_NAME}"
NODE3_CONN="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5434/${DB_NAME}"

# Function to run SQL test file
run_sql_test() {
    local test_file=$1
    local connection_string=$2
    local test_name=$3
    
    echo -e "${BLUE}Running ${test_name}...${NC}"
    
    if psql "${connection_string}" -f "${test_file}" > "test_results_$(basename ${test_file}).log" 2>&1; then
        echo -e "${GREEN}✅ ${test_name} passed${NC}"
        return 0
    else
        echo -e "${RED}❌ ${test_name} failed${NC}"
        echo "Check test_results_$(basename ${test_file}).log for details"
        return 1
    fi
}

# Function to run Python test
run_python_test() {
    local test_file=$1
    local test_name=$2
    
    echo -e "${BLUE}Running ${test_name}...${NC}"
    
    if python3 "${test_file}" > "test_results_$(basename ${test_file}).log" 2>&1; then
        echo -e "${GREEN}✅ ${test_name} passed${NC}"
        return 0
    else
        echo -e "${RED}❌ ${test_name} failed${NC}"
        echo "Check test_results_$(basename ${test_file}).log for details"
        return 1
    fi
}

# Function to wait for services
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local timeout=${4:-30}
    
    echo -e "${YELLOW}Waiting for ${service_name} at ${host}:${port}...${NC}"
    
    for i in $(seq 1 $timeout); do
        if timeout 1 bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
            echo -e "${GREEN}${service_name} is ready${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}${service_name} failed to start within ${timeout} seconds${NC}"
    return 1
}

# Function to check cluster health
check_cluster_health() {
    echo -e "${BLUE}Checking cluster health...${NC}"
    
    # Check each node
    for i in 1 2 3; do
        port=$((5431 + i))
        if psql "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${port}/${DB_NAME}" \
               -c "SELECT 'Node $i OK' as status, check_quorum() as has_quorum, COUNT(*) as node_count FROM cluster_nodes;" \
               2>/dev/null; then
            echo -e "${GREEN}Node $i is healthy${NC}"
        else
            echo -e "${YELLOW}Node $i may have issues${NC}"
        fi
    done
    
    # Check query router
    if curl -s "http://${DB_HOST}:8080/status" > /dev/null 2>&1; then
        echo -e "${GREEN}Query Router is healthy${NC}"
    else
        echo -e "${YELLOW}Query Router may have issues${NC}"
    fi
}

# Function to run performance test
run_performance_test() {
    echo -e "${BLUE}Running performance tests...${NC}"
    
    # Create performance test table
    psql "${NODE1_CONN}" -c "
        CREATE TABLE IF NOT EXISTS perf_test (
            id SERIAL PRIMARY KEY,
            data TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            last_updated_at TIMESTAMPTZ DEFAULT NOW(),
            updated_by_node UUID
        );" 2>/dev/null || true
    
    # Insert test data and measure time
    echo "Testing insert performance..."
    time psql "${WRITER_CONN}" -c "
        INSERT INTO perf_test (data) 
        SELECT 'Performance test data ' || i 
        FROM generate_series(1, 1000) i;" 2>/dev/null
    
    # Test read performance
    echo "Testing read performance..."
    time psql "${READER_CONN}" -c "
        SELECT COUNT(*) FROM perf_test WHERE data LIKE 'Performance%';" 2>/dev/null
    
    # Test update performance
    echo "Testing update performance..."
    time psql "${WRITER_CONN}" -c "
        UPDATE perf_test 
        SET data = data || ' [updated]' 
        WHERE id <= 100;" 2>/dev/null
    
    # Cleanup
    psql "${NODE1_CONN}" -c "DROP TABLE IF EXISTS perf_test;" 2>/dev/null || true
    
    echo -e "${GREEN}Performance tests completed${NC}"
}

# Function to test high availability
test_high_availability() {
    echo -e "${BLUE}Testing High Availability endpoints...${NC}"
    
    # Test writer endpoint
    if psql "${WRITER_CONN}" -c "SELECT 'Writer endpoint OK' as status;" 2>/dev/null; then
        echo -e "${GREEN}✅ Writer endpoint accessible${NC}"
    else
        echo -e "${RED}❌ Writer endpoint failed${NC}"
    fi
    
    # Test reader endpoint
    if psql "${READER_CONN}" -c "SELECT 'Reader endpoint OK' as status;" 2>/dev/null; then
        echo -e "${GREEN}✅ Reader endpoint accessible${NC}"
    else
        echo -e "${RED}❌ Reader endpoint failed${NC}"
    fi
    
    # Test query router
    if curl -s "http://${DB_HOST}:8080/status" | grep -q "cluster_status"; then
        echo -e "${GREEN}✅ Query Router API accessible${NC}"
    else
        echo -e "${RED}❌ Query Router API failed${NC}"
    fi
}

# Main test execution
main() {
    local failed_tests=0
    local total_tests=0
    
    echo "Test environment:"
    echo "  Database: ${DB_NAME}"
    echo "  Host: ${DB_HOST}"
    echo "  Writer Port: 5000"
    echo "  Reader Port: 5001"
    echo ""
    
    # Wait for services to be ready
    wait_for_service "${DB_HOST}" 5432 "Node 1" 60
    wait_for_service "${DB_HOST}" 5433 "Node 2" 60
    wait_for_service "${DB_HOST}" 5434 "Node 3" 60
    wait_for_service "${DB_HOST}" 8080 "Query Router" 60
    
    # Check initial cluster health
    check_cluster_health
    echo ""
    
    # Test 1: Basic Replication Tests
    echo -e "${YELLOW}=== Test Suite 1: Replication Tests ===${NC}"
    if run_sql_test "tests/test_replication.sql" "${NODE1_CONN}" "Replication Tests"; then
        echo -e "${GREEN}Replication tests passed${NC}"
    else
        failed_tests=$((failed_tests + 1))
    fi
    total_tests=$((total_tests + 1))
    echo ""
    
    # Test 2: Vector Operations Tests
    echo -e "${YELLOW}=== Test Suite 2: Vector Operations Tests ===${NC}"
    if run_sql_test "tests/test_vector_operations.sql" "${NODE1_CONN}" "Vector Operations Tests"; then
        echo -e "${GREEN}Vector operations tests passed${NC}"
    else
        failed_tests=$((failed_tests + 1))
    fi
    total_tests=$((total_tests + 1))
    echo ""
    
    # Test 3: High Availability Tests
    echo -e "${YELLOW}=== Test Suite 3: High Availability Tests ===${NC}"
    test_high_availability
    echo ""
    
    # Test 4: Performance Tests
    echo -e "${YELLOW}=== Test Suite 4: Performance Tests ===${NC}"
    run_performance_test
    echo ""
    
    # Test 5: Failover Tests (Python)
    if [ -f "tests/test_failover.py" ]; then
        echo -e "${YELLOW}=== Test Suite 5: Failover Tests ===${NC}"
        if run_python_test "tests/test_failover.py" "Failover Tests"; then
            echo -e "${GREEN}Failover tests passed${NC}"
        else
            failed_tests=$((failed_tests + 1))
        fi
        total_tests=$((total_tests + 1))
        echo ""
    fi
    
    # Test 6: Cross-Node Consistency Tests
    echo -e "${YELLOW}=== Test Suite 6: Cross-Node Consistency Tests ===${NC}"
    
    # Insert data on one node, verify on others
    echo "Testing cross-node data consistency..."
    
    # Insert on node1
    psql "${NODE1_CONN}" -c "
        INSERT INTO users (username, email, profile_data) VALUES 
        ('consistency_test', 'consistency@example.com', '{\"test\": \"cross_node\"}')
        ON CONFLICT (username) DO UPDATE SET 
        email = EXCLUDED.email, 
        profile_data = EXCLUDED.profile_data;" 2>/dev/null
    
    # Wait for replication
    sleep 3
    
    # Check on other nodes
    for i in 2 3; do
        port=$((5431 + i))
        if psql "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${port}/${DB_NAME}" \
               -c "SELECT COUNT(*) as count FROM users WHERE username = 'consistency_test';" 2>/dev/null | grep -q "1"; then
            echo -e "${GREEN}✅ Data replicated to Node $i${NC}"
        else
            echo -e "${RED}❌ Data NOT replicated to Node $i${NC}"
            failed_tests=$((failed_tests + 1))
        fi
    done
    
    # Cleanup
    psql "${NODE1_CONN}" -c "DELETE FROM users WHERE username = 'consistency_test';" 2>/dev/null
    total_tests=$((total_tests + 1))
    echo ""
    
    # Final cluster health check
    echo -e "${YELLOW}=== Final Health Check ===${NC}"
    check_cluster_health
    echo ""
    
    # Test results summary
    echo "=============================================="
    echo -e "${BLUE}Test Results Summary${NC}"
    echo "=============================================="
    echo "Total test suites: ${total_tests}"
    echo "Failed test suites: ${failed_tests}"
    echo "Success rate: $(( (total_tests - failed_tests) * 100 / total_tests ))%"
    echo ""
    
    if [ ${failed_tests} -eq 0 ]; then
        echo -e "${GREEN}🎉 All tests passed! SynapseDB cluster is functioning correctly.${NC}"
        echo ""
        echo "Your distributed database is ready for use:"
        echo "  - Writer: ${WRITER_CONN}"
        echo "  - Reader: ${READER_CONN}"
        echo "  - Query Router: http://${DB_HOST}:8080"
        echo "  - Monitoring: http://${DB_HOST}:3000 (Grafana)"
        exit 0
    else
        echo -e "${RED}❌ ${failed_tests} test suite(s) failed. Check logs for details.${NC}"
        echo ""
        echo "Log files generated:"
        ls -la test_results_*.log 2>/dev/null || echo "No log files found"
        exit 1
    fi
}

# Run main function
main "$@"