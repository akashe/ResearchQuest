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

# # Clean up PVCs that might cause binding issues (with timeout to prevent hanging)
# echo -e "${YELLOW}🧹 Cleaning up PVCs to prevent binding issues...${NC}"

# # Clean up shared data PV/PVC (these can get stuck in Released state)
# if kubectl get pv shared-data-pv > /dev/null 2>&1; then
#     echo -e "${YELLOW}Deleting shared-data-pv with timeout...${NC}"
#     timeout 60 kubectl delete pv shared-data-pv || {
#         echo -e "${YELLOW}⚠️  PV deletion timed out, force deleting...${NC}"
#         kubectl delete pv shared-data-pv --force --grace-period=0 2>/dev/null || true
#         kubectl patch pv shared-data-pv -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
#     }
#     echo -e "${GREEN}✅ shared-data-pv cleaned up${NC}"
# else
#     echo -e "${YELLOW}shared-data-pv doesn't exist${NC}"
# fi

# # Delete PVCs in namespace before deleting namespace (with timeout)
# if kubectl get namespace rag-system > /dev/null 2>&1; then
#     echo -e "${YELLOW}Cleaning up PVCs in rag-system namespace with timeout...${NC}"
#     timeout 120 kubectl delete pvc --all -n rag-system 2>/dev/null || {
#         echo -e "${YELLOW}⚠️  PVC deletion timed out, force deleting...${NC}"
#         kubectl delete pvc --all -n rag-system --force --grace-period=0 2>/dev/null || true
#         for pvc in $(kubectl get pvc -n rag-system -o name 2>/dev/null); do
#             kubectl patch $pvc -n rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
#         done
#     }
#     echo -e "${GREEN}✅ PVCs cleaned up${NC}"
# fi

# Delete the namespace (this removes all remaining resources)
if kubectl get namespace rag-system > /dev/null 2>&1; then
    echo -e "${YELLOW}Deleting rag-system namespace with timeout...${NC}"
    timeout 60 kubectl delete namespace rag-system || {
        echo -e "${YELLOW}⚠️  Namespace deletion timed out, force deleting...${NC}"
        kubectl delete namespace rag-system --force --grace-period=0 2>/dev/null || true
        kubectl patch namespace rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
    }
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