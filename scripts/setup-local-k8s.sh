#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🛠️ Setting up local Kubernetes cluster${NC}"

# Check if any k8s solution is available
DOCKER_DESKTOP_K8S=false
MINIKUBE_AVAILABLE=false
KIND_AVAILABLE=false

# Check Docker Desktop Kubernetes
if docker info > /dev/null 2>&1 && docker system info | grep -q "Kubernetes"; then
    if kubectl config get-contexts | grep -q "docker-desktop"; then
        DOCKER_DESKTOP_K8S=true
    fi
fi

# Check minikube
if command -v minikube &> /dev/null; then
    MINIKUBE_AVAILABLE=true
fi

# Check kind
if command -v kind &> /dev/null; then
    KIND_AVAILABLE=true
fi

echo -e "${GREEN}Available Kubernetes options:${NC}"
if $DOCKER_DESKTOP_K8S; then
    echo -e "${GREEN}✅ Docker Desktop Kubernetes${NC}"
fi
if $MINIKUBE_AVAILABLE; then
    echo -e "${GREEN}✅ Minikube${NC}"
fi
if $KIND_AVAILABLE; then
    echo -e "${GREEN}✅ Kind${NC}"
fi

# Auto-select or prompt user
if $DOCKER_DESKTOP_K8S; then
    echo -e "${GREEN}🐳 Using Docker Desktop Kubernetes${NC}"
    kubectl config use-context docker-desktop
elif $KIND_AVAILABLE; then
    echo -e "${GREEN}🔧 Setting up Kind cluster...${NC}"
    
    # Check if cluster already exists
    if kind get clusters | grep -q "kind"; then
        echo -e "${YELLOW}Kind cluster already exists${NC}"
    else
        # Create kind cluster with config
        cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: rag-system
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
  - containerPort: 8501
    hostPort: 8501
    protocol: TCP
EOF
    fi
    
    # Set context
    kubectl config use-context kind-rag-system
    
elif $MINIKUBE_AVAILABLE; then
    echo -e "${GREEN}⚡ Setting up Minikube cluster...${NC}"
    
    if minikube status > /dev/null 2>&1; then
        echo -e "${YELLOW}Minikube cluster already running${NC}"
    else
        minikube start --driver=docker --kubernetes-version=v1.28.0
    fi
    
    # Enable addons
    minikube addons enable ingress
    minikube addons enable dashboard
    
else
    echo -e "${RED}❌ No Kubernetes solution available${NC}"
    echo -e "${YELLOW}Please install one of the following:${NC}"
    echo -e "${YELLOW}1. Docker Desktop with Kubernetes enabled${NC}"
    echo -e "${YELLOW}2. Minikube: brew install minikube (macOS) or see https://minikube.sigs.k8s.io/docs/start/${NC}"
    echo -e "${YELLOW}3. Kind: brew install kind (macOS) or see https://kind.sigs.k8s.io/docs/user/quick-start/${NC}"
    exit 1
fi

# Verify cluster is working
echo -e "${GREEN}🔍 Verifying cluster...${NC}"
kubectl cluster-info
kubectl get nodes

echo -e "${GREEN}🎉 Kubernetes cluster is ready!${NC}"
echo -e "${GREEN}Current context: $(kubectl config current-context)${NC}"

# Optional: Install ingress controller for kind
if kubectl config current-context | grep -q "kind"; then
    echo -e "${GREEN}📡 Setting up ingress controller for Kind...${NC}"
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
    echo -e "${YELLOW}⏳ Waiting for ingress controller to be ready...${NC}"
    kubectl wait --namespace ingress-nginx \
      --for=condition=ready pod \
      --selector=app.kubernetes.io/component=controller \
      --timeout=90s
fi