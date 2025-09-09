#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🧪 Testing microservices communication${NC}"

# Check if services are running
echo -e "${GREEN}📊 Checking service status...${NC}"
kubectl get pods -n rag-system

# Port forward services for testing
echo -e "${GREEN}🔌 Setting up port forwarding...${NC}"
kubectl port-forward -n rag-system service/query-service 8000:8000 &
QUERY_PID=$!
kubectl port-forward -n rag-system service/retrieval-service 8001:8001 &
RETRIEVAL_PID=$!
kubectl port-forward -n rag-system service/generation-service 8002:8002 &
GENERATION_PID=$!
kubectl port-forward -n rag-system service/frontend-service 8501:8501 &
FRONTEND_PID=$!

# Wait for port forwards to be ready
sleep 5

# Cleanup function
cleanup() {
    echo -e "${YELLOW}🧹 Cleaning up port forwards...${NC}"
    kill $QUERY_PID $RETRIEVAL_PID $GENERATION_PID $FRONTEND_PID 2>/dev/null || true
}
trap cleanup EXIT

# Test health endpoints
echo -e "${GREEN}🏥 Testing health endpoints...${NC}"

test_health() {
    local service=$1
    local port=$2
    local url="http://localhost:${port}/health"
    
    if curl -f -s "$url" > /dev/null; then
        echo -e "${GREEN}✅ $service health check passed${NC}"
        curl -s "$url" | python3 -m json.tool
    else
        echo -e "${RED}❌ $service health check failed${NC}"
        return 1
    fi
}

test_health "Query Service" 8000
test_health "Retrieval Service" 8001
test_health "Generation Service" 8002

# Test Streamlit health (different endpoint)
echo -e "${GREEN}🖥️ Testing Frontend Service...${NC}"
if curl -f -s "http://localhost:8501/_stcore/health" > /dev/null; then
    echo -e "${GREEN}✅ Frontend Service health check passed${NC}"
else
    echo -e "${RED}❌ Frontend Service health check failed${NC}"
fi

# Test API endpoints with sample data
echo -e "${GREEN}🔍 Testing API endpoints...${NC}"

# Test query service docs
echo -e "${YELLOW}📚 Query Service API Docs:${NC}"
echo -e "${YELLOW}http://localhost:8000/docs${NC}"

# Test a simple subgraph creation (if Neo4j is available)
echo -e "${GREEN}🧪 Testing subgraph creation...${NC}"
RESPONSE=$(curl -s -X POST "http://localhost:8000/subgraph/create" \
    -H "Content-Type: application/json" \
    -d '{
        "topic": "machine learning",
        "topic_name": "ml_test",
        "validate_relationships": true
    }' || echo "Service communication test")

echo -e "${GREEN}Response:${NC}"
echo "$RESPONSE" | python3 -c "import sys, json; print(json.dumps(json.load(sys.stdin), indent=2))" 2>/dev/null || echo "$RESPONSE"

echo -e "${GREEN}🎉 Service testing completed!${NC}"
echo -e "${YELLOW}💡 Access points:${NC}"
echo -e "${YELLOW}  Frontend: http://localhost:8501${NC}"
echo -e "${YELLOW}  Query API: http://localhost:8000/docs${NC}"
echo -e "${YELLOW}  Retrieval API: http://localhost:8001/docs${NC}"
echo -e "${YELLOW}  Generation API: http://localhost:8002/docs${NC}"

# Keep port forwards alive
echo -e "${GREEN}🔄 Port forwards are active. Press Ctrl+C to stop.${NC}"
wait