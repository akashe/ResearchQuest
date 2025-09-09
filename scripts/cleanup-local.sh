#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🧹 Cleaning up local development environment${NC}"

# Stop minikube mount processes
echo -e "${YELLOW}Stopping Minikube mount processes...${NC}"
pkill -f "minikube mount" && echo -e "${GREEN}✅ Mount processes stopped${NC}" || echo -e "${YELLOW}No mount processes running${NC}"

# Delete the namespace (this removes all resources)
if kubectl get namespace rag-system > /dev/null 2>&1; then
    echo -e "${YELLOW}Deleting rag-system namespace...${NC}"
    kubectl delete namespace rag-system
    echo -e "${GREEN}✅ Namespace deleted${NC}"
else
    echo -e "${YELLOW}Namespace rag-system doesn't exist${NC}"
fi

# Optional: Stop minikube entirely
read -p "Do you want to stop Minikube entirely? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Stopping Minikube...${NC}"
    minikube stop
    echo -e "${GREEN}✅ Minikube stopped${NC}"
else
    echo -e "${YELLOW}Minikube left running${NC}"
fi

echo -e "${GREEN}🎉 Cleanup completed!${NC}"