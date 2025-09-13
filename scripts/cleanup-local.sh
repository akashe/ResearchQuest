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


# Clean up individual service PVCs (current architecture: arxiv-ingestion + merged data-pipeline-graph-builder)
if kubectl get namespace rag-system > /dev/null 2>&1; then
    echo -e "${YELLOW}Cleaning up service PVCs...${NC}"

    # Current PVCs in merged architecture
    CURRENT_PVCS=("arxiv-data-pvc" "data-pipeline-graph-builder-pvc" "neo4j-data-pvc")

    # Legacy PVCs to clean up from old architecture
    LEGACY_PVCS=("data-pipeline-pvc" "graph-builder-pvc")

    # Clean up current PVCs
    for pvc in "${CURRENT_PVCS[@]}"; do
        if kubectl get pvc $pvc -n rag-system > /dev/null 2>&1; then
            echo -e "${YELLOW}Deleting current PVC: $pvc${NC}"
            if [[ "$pvc" == "data-pipeline-graph-builder-pvc" ]]; then
                # This PVC contains large files like references_complete.jsonl (9.9GB)
                timeout 300 kubectl delete pvc $pvc -n rag-system || {
                    echo -e "${YELLOW}⚠️  Large PVC deletion timed out, force deleting...${NC}"
                    kubectl delete pvc $pvc -n rag-system --force --grace-period=0 2>/dev/null || true
                    kubectl patch pvc $pvc -n rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
                }
            else
                timeout 120 kubectl delete pvc $pvc -n rag-system || {
                    kubectl delete pvc $pvc -n rag-system --force --grace-period=0 2>/dev/null || true
                    kubectl patch pvc $pvc -n rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
                }
            fi
        fi
    done

    # Clean up legacy PVCs from old architecture
    for pvc in "${LEGACY_PVCS[@]}"; do
        if kubectl get pvc $pvc -n rag-system > /dev/null 2>&1; then
            echo -e "${YELLOW}Deleting legacy PVC: $pvc${NC}"
            timeout 120 kubectl delete pvc $pvc -n rag-system || {
                kubectl delete pvc $pvc -n rag-system --force --grace-period=0 2>/dev/null || true
                kubectl patch pvc $pvc -n rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
            }
        fi
    done

    # Clean up any remaining PVCs not caught above
    REMAINING_PVCS=$(kubectl get pvc -n rag-system -o name 2>/dev/null | wc -l)
    if [ "$REMAINING_PVCS" -gt 0 ]; then
        echo -e "${YELLOW}Cleaning up $REMAINING_PVCS remaining PVCs...${NC}"
        timeout 60 kubectl delete pvc --all -n rag-system 2>/dev/null || {
            echo -e "${YELLOW}⚠️  Remaining PVC deletion timed out, force deleting...${NC}"
            kubectl delete pvc --all -n rag-system --force --grace-period=0 2>/dev/null || true
            for pvc in $(kubectl get pvc -n rag-system -o name 2>/dev/null); do
                kubectl patch $pvc -n rag-system -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
            done
        }
    fi

    echo -e "${GREEN}✅ All PVCs cleaned up${NC}"
fi

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
    minikube delete --all --purge 
    echo -e "${GREEN}✅ Minikube stopped${NC}"
else
    echo -e "${YELLOW}Minikube left running${NC}"
fi

echo -e "${GREEN}🎉 Cleanup completed!${NC}"