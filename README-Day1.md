# Day 1: Microservices & Kubernetes Setup

## 🎯 What We Built Today

### Microservice Architecture
- **Query Service** (Port 8000): API Gateway that orchestrates requests between services
- **Retrieval Service** (Port 8001): Handles Neo4j operations, graph analysis, paper retrieval
- **Generation Service** (Port 8002): Manages LLM interactions and text generation
- **Frontend Service** (Port 8501): Streamlit UI that communicates with Query Service

### Infrastructure
- ✅ Complete Kubernetes manifests with Kustomize
- ✅ Multi-stage Docker builds for all services
- ✅ CI/CD pipeline with GitHub Actions
- ✅ Local deployment automation scripts

## 🚀 Quick Start

### Prerequisites
1. **Docker**: Ensure Docker Desktop is running
2. **Kubernetes**: One of the following:
   - Docker Desktop with Kubernetes enabled
   - Minikube (`brew install minikube`)
   - Kind (`brew install kind`)

### Setup & Deployment

```bash
# 1. Setup local Kubernetes cluster
./scripts/setup-local-k8s.sh

# 2. Build and deploy all services
./scripts/build-and-deploy-local.sh

# 3. Test service communication
./scripts/test-services.sh
```

### Access Points
- **Frontend**: http://localhost:8501 (after port-forward)
- **Query API**: http://localhost:8000/docs
- **Retrieval API**: http://localhost:8001/docs  
- **Generation API**: http://localhost:8002/docs

## 🛠️ Manual Setup (if scripts don't work)

### 1. Build Docker Images
```bash
cd services/query-service && docker build -t query-service:dev .
cd ../retrieval-service && docker build -t retrieval-service:dev .
cd ../generation-service && docker build -t generation-service:dev .
cd ../frontend-service && docker build -t frontend-service:dev .
```

### 2. Load Images (for kind)
```bash
kind load docker-image query-service:dev
kind load docker-image retrieval-service:dev
kind load docker-image generation-service:dev
kind load docker-image frontend-service:dev
```

### 3. Deploy to Kubernetes
```bash
kubectl apply -k infrastructure/kubernetes/environments/dev/
kubectl wait --for=condition=available --timeout=300s deployment --all -n rag-system
```

### 4. Port Forward Services
```bash
kubectl port-forward -n rag-system service/frontend-service 8501:8501 &
kubectl port-forward -n rag-system service/query-service 8000:8000 &
```

## 🏗️ Architecture Overview

```
┌─────────────────┐    HTTP     ┌─────────────────┐
│  Frontend       │ ────────── │  Query Service  │
│  (Streamlit)    │             │  (FastAPI)      │
│  Port: 8501     │             │  Port: 8000     │
└─────────────────┘             └─────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
           ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
           │  Retrieval      │  │  Generation     │  │  External       │
           │  Service        │  │  Service        │  │  Dependencies   │
           │  (FastAPI)      │  │  (FastAPI)      │  │  - Neo4j        │
           │  Port: 8001     │  │  Port: 8002     │  │  - Google AI    │
           └─────────────────┘  └─────────────────┘  └─────────────────┘
```

## 🧪 Testing

### Health Checks
```bash
curl http://localhost:8000/health  # Query Service
curl http://localhost:8001/health  # Retrieval Service
curl http://localhost:8002/health  # Generation Service
curl http://localhost:8501/_stcore/health  # Frontend Service
```

### API Testing
```bash
# Test subgraph creation
curl -X POST "http://localhost:8000/subgraph/create" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "machine learning",
    "topic_name": "ml_test",
    "validate_relationships": true
  }'
```

## 🔧 Troubleshooting

### Common Issues

1. **Services not starting**: Check logs
   ```bash
   kubectl logs -f deployment/query-service -n rag-system
   ```

2. **Neo4j connection issues**: Update secrets in `infrastructure/kubernetes/shared/secrets.yaml`

3. **Image pull errors**: Ensure images are built and loaded to cluster

4. **Port conflicts**: Change port forwarding ports if 8000-8002, 8501 are in use

## 📊 Key Learning Outcomes

✅ **Microservices**: Decomposed monolith into focused, single-responsibility services  
✅ **Containerization**: Multi-stage Docker builds with security best practices  
✅ **Kubernetes**: Deployments, Services, Secrets, Namespaces, and Kustomize  
✅ **Service Discovery**: Inter-service communication via Kubernetes DNS  
✅ **CI/CD**: GitHub Actions with conditional builds and multi-service pipelines  
✅ **DevOps**: Automated deployment scripts and health monitoring  

## 🎯 Tomorrow's Focus (Day 2)
- Prometheus + Grafana monitoring stack
- Custom metrics and dashboards  
- Alerting and observability
- Application and business metrics tracking

## 📝 Notes
- All services use non-root users for security
- Health checks configured for proper K8s lifecycle management
- Resource limits set for all containers
- Secrets externalized for production readiness