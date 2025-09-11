# Day 3 Infrastructure Deployment Guide

## Overview
Day 3 introduces advanced data pipeline infrastructure with Apache Kafka, Apache Airflow, Redis, and a comprehensive data pipeline service for event-driven processing.

## New Services Added
1. **Apache Kafka** - Event streaming platform with Zookeeper
2. **Apache Airflow** - Workflow orchestration (webserver + scheduler)
3. **Redis** - Distributed caching layer
4. **Data Pipeline Service** - Event processing service
5. **Updated Prometheus** - Monitoring for new services

## Prerequisites
- Minikube running and stable
- Docker installed
- Kubectl configured for your cluster
- At least 8GB memory available for Minikube

## Deployment Steps

### 1. Build Docker Images
```bash
cd /Users/akashkumar/projects/ResearchQuest
./scripts/build-and-deploy-local.sh
```

### 2. Deploy Infrastructure
```bash
kubectl apply -k infrastructure/kubernetes/base/
```

### 3. Verify Deployments
```bash
# Check all pods are running
kubectl get pods -n rag-system

# Check services
kubectl get services -n rag-system
```

### 4. Access Services
```bash
# Airflow UI (admin/admin123)
kubectl port-forward svc/airflow-webserver 8080:8080 -n rag-system

# Prometheus
kubectl port-forward svc/prometheus 9090:9090 -n rag-system

# Grafana
kubectl port-forward svc/grafana 3000:3000 -n rag-system
```

## Service Architecture

### Data Flow
1. **arXiv Ingestion Service** (8004) - Downloads arXiv data from Kaggle
2. **Data Pipeline Service** (8005) - Processes events through Kafka topics:
   - `new-papers` → `semantic-enriched` → `quality-filtered` → `graph-updates`
3. **Apache Airflow** (8080) - Orchestrates daily ingestion and weekly maintenance
4. **Apache Kafka** (9092) - Event streaming backbone
5. **Redis** (6379) - Distributed caching for performance
6. **Prometheus** (9090) - Metrics collection from all services

### Airflow DAGs
- **arxiv_daily_ingestion**: Daily pipeline for new papers
- **graph_maintenance_weekly**: Weekly graph optimization

## Testing the Pipeline

### 1. Trigger Ingestion
```bash
# Port forward to arxiv service
kubectl port-forward svc/arxiv-ingestion-service 8004:8004 -n rag-system

# Trigger ingestion
curl -X POST http://localhost:8004/ingest \
  -H "Content-Type: application/json" \
  -d '{"force_download": false, "filter_years": 1}'
```

### 2. Check Data Pipeline
```bash
# Port forward to data pipeline service
kubectl port-forward svc/data-pipeline-service 8005:8005 -n rag-system

# Check health
curl http://localhost:8005/health

# Check metrics
curl http://localhost:8005/metrics
```

### 3. Monitor in Airflow
- Access Airflow UI at http://localhost:8080
- Username: admin, Password: admin123
- Check DAG status and logs

### 4. View Metrics
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Troubleshooting

### Common Issues
1. **Pod Restart Due to Memory**: Increase Minikube memory
   ```bash
   minikube config set memory 8192
   minikube delete && minikube start
   ```

2. **Kafka Connection Issues**: Ensure Zookeeper is healthy first
   ```bash
   kubectl logs -f zookeeper-0 -n rag-system
   ```

3. **Airflow DAG Issues**: Check scheduler logs
   ```bash
   kubectl logs -f deployment/airflow-scheduler -n rag-system
   ```

### Health Checks
```bash
# Check all service health endpoints
for service in query-service:8000 retrieval-service:8001 generation-service:8002 neo4j-service:8003 arxiv-ingestion-service:8004 data-pipeline-service:8005; do
  echo "Checking $service"
  kubectl port-forward svc/${service%%:*} ${service##*:}:${service##*:} -n rag-system &
  sleep 2
  curl -f http://localhost:${service##*:}/health || echo "FAILED"
  pkill -f "kubectl port-forward.*${service%%:*}"
  sleep 1
done
```

## Next Steps
Once all services are running and healthy:
1. Test the complete data pipeline from ingestion to graph updates
2. Configure Grafana dashboards for the new services
3. Set up alerting for pipeline failures
4. Prepare for Day 4 advanced features

## Resource Requirements
- **Minimum**: 6GB RAM, 4 CPU cores
- **Recommended**: 8GB RAM, 6 CPU cores
- **Storage**: 10GB free space for datasets and logs