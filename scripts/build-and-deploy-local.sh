#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Building and deploying GraphRAG microservices locally${NC}"

# Clean up Docker space before deployment
echo -e "${YELLOW}🧹 Cleaning up Docker resources to free space...${NC}"
docker system prune -f --volumes 2>/dev/null || true
docker image prune -a -f 2>/dev/null || true
echo -e "${GREEN}✅ Docker cleanup completed${NC}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl is not installed. Please install kubectl first.${NC}"
    exit 1
fi

# Check if Kubernetes cluster is accessible
if ! kubectl cluster-info > /dev/null 2>&1; then
    echo -e "${RED}❌ Kubernetes cluster is not accessible. Please check your cluster.${NC}"
    echo -e "${YELLOW}💡 For local development, you can use:${NC}"
    echo -e "${YELLOW}   - Docker Desktop with Kubernetes enabled${NC}"
    echo -e "${YELLOW}   - minikube start${NC}"
    echo -e "${YELLOW}   - kind create cluster${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# Set up Docker environment for minikube
if kubectl config current-context | grep -q "minikube"; then
    echo -e "${GREEN}🐳 Using Minikube - setting Docker environment${NC}"
    eval $(minikube docker-env)
    
    # Mount host data directory to Minikube
    echo -e "${GREEN}📁 Setting up data volume mount...${NC}"
    # Kill any existing mount processes
    pkill -f "minikube mount" || true
    
    # Start background mount for data directory
    echo -e "${YELLOW}Mounting data directory to Minikube VM...${NC}"
    minikube mount "$(pwd)/data:/data" &
    MOUNT_PID=$!
    
    # Wait a moment for mount to establish
    sleep 5
    
    # Verify mount is working
    if minikube ssh "test -f /data/citation_nodes_full.csv"; then
        echo -e "${GREEN}✅ Data files successfully mounted in Minikube${NC}"
    else
        echo -e "${RED}❌ Data mount failed - files not accessible${NC}"
        echo -e "${YELLOW}💡 Retrying mount setup...${NC}"
        sleep 3
        if minikube ssh "test -f /data/citation_nodes_full.csv"; then
            echo -e "${GREEN}✅ Data files now accessible${NC}"
        else
            echo -e "${RED}❌ Mount still failed. Check if data files exist in $(pwd)/data/${NC}"
            ls -la "$(pwd)/data/"
        fi
    fi
    
    echo -e "${YELLOW}📝 Note: Mount process running in background (PID: $MOUNT_PID)${NC}"
    echo -e "${YELLOW}💡 To stop mount later: kill $MOUNT_PID${NC}"
fi

# Build all Docker images
services=("query-service" "retrieval-service" "generation-service" "frontend-service" "neo4j-service")

echo -e "${GREEN}📦 Building Docker images...${NC}"
for service in "${services[@]}"; do
    echo -e "${YELLOW}Building ${service}...${NC}"
    cd "services/${service}"

    # Adding common files like custom_logging.py
    mkdir -p shared
    cp ../../custom_logging.py shared/

    docker build -t "${service}:dev" .
    cd "../.."
    echo -e "${GREEN}✅ ${service} built successfully${NC}"
done

# Build all Docker images
data_services=("arxiv-ingestion-service")

echo -e "${GREEN}📦 Building Docker images...${NC}"
for service in "${data_services[@]}"; do
    echo -e "${YELLOW}Building ${service}...${NC}"
    cd "services/${service}"

    docker build -t "${service}:dev" .
    cd "../.."
    echo -e "${GREEN}✅ ${service} built successfully${NC}"
done

# Load images to kind cluster if using kind
if kubectl config current-context | grep -q "kind"; then
    echo -e "${GREEN}📥 Loading images to kind cluster...${NC}"
    for service in "${services[@]}"; do
        echo -e "${YELLOW}Loading ${service}:dev to kind...${NC}"
        kind load docker-image "${service}:dev"
    done
fi

# Apply Kubernetes manifests using kustomize
echo -e "${GREEN}🚀 Deploying to Kubernetes...${NC}"
cd infrastructure/kubernetes/environments/dev/
kubectl apply -k .
cd ../../../..

# Wait for deployments to be ready
echo -e "${GREEN}⏳ Waiting for deployments to be ready...${NC}"
kubectl wait --for=condition=available --timeout=120s deployment --all -n rag-system

# Show deployment status
echo -e "${GREEN}📊 Deployment Status:${NC}"
kubectl get pods -n rag-system
kubectl get services -n rag-system

# Get frontend service URL
echo -e "${GREEN}🌐 Access URLs:${NC}"
if kubectl get service frontend-service -n rag-system -o jsonpath='{.status.loadBalancer.ingress[0].ip}' > /dev/null 2>&1; then
    FRONTEND_IP=$(kubectl get service frontend-service -n rag-system -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    echo -e "${GREEN}Frontend: http://${FRONTEND_IP}:8501${NC}"
else
    echo -e "${YELLOW}Frontend: kubectl port-forward -n rag-system service/frontend-service 8501:8501${NC}"
    echo -e "${YELLOW}Then access: http://localhost:8501${NC}"
fi

echo -e "${GREEN}Query API: kubectl port-forward -n rag-system service/query-service 8000:8000${NC}"
echo -e "${GREEN}Then access: http://localhost:8000/docs${NC}"

echo -e "${GREEN}📊 Monitoring URLs:${NC}"
echo -e "${YELLOW}Grafana: kubectl port-forward -n rag-system service/grafana 3000:3000${NC}"
echo -e "${YELLOW}Then access: http://localhost:3000 (admin/admin123)${NC}"
echo -e "${YELLOW}Prometheus: kubectl port-forward -n rag-system service/prometheus 9090:9090${NC}"
echo -e "${YELLOW}Then access: http://localhost:9090${NC}"

echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo -e "${YELLOW}💡 To check logs: kubectl logs -f deployment/<service-name> -n rag-system${NC}"
echo -e "${YELLOW}💡 To clean up: kubectl delete namespace rag-system${NC}"