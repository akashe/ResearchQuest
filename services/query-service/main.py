from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import requests
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import time

app = FastAPI(title="Query Service", version="1.0.0")

# Metrics
REQUEST_COUNT = Counter('query_service_requests_total', 'Total requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('query_service_request_duration_seconds', 'Request duration')
PAPERS_PROCESSED = Counter('query_service_papers_processed_total', 'Total papers processed', ['topic'])

# Request/Response models
class SubgraphRequest(BaseModel):
    topic: str
    topic_name: str
    validate_relationships: bool = True

class QueryRequest(BaseModel):
    topic_name: str
    year_cutoff: Optional[int] = None
    num_chunks: Optional[int] = 20
    from_year: Optional[int] = 2022

class CustomQuestionRequest(BaseModel):
    question: str
    topic_name: str
    year_cutoff: int
    num_chunks: int = 4

class TopicEvolutionRequest(BaseModel):
    topic_name: str
    num_papers: int = 20
    from_year: int = 2022

# Service URLs - will be configured via environment
RETRIEVAL_SERVICE_URL = "http://retrieval-service:8001"
GENERATION_SERVICE_URL = "http://generation-service:8002"

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "query-service"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/subgraph/create")
async def create_subgraph(request: SubgraphRequest):
    """Create a topic subgraph via retrieval service"""
    try:
        response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/subgraph/create",
            json=request.dict(),
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Retrieval service error: {str(e)}")

@app.post("/papers/top-recent")
async def get_top_recent_papers(request: QueryRequest):
    """Get top papers from recent years"""
    start_time = time.time()
    REQUEST_COUNT.labels(method='POST', endpoint='/papers/top-recent').inc()
    try:
        response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/papers/top-recent",
            json=request.dict(),
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        REQUEST_DURATION.observe(time.time() - start_time)
        return result
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Retrieval service error: {str(e)}")

@app.post("/papers/state-of-art")
async def get_state_of_art_papers(request: QueryRequest):
    """Get raw papers for state of the art analysis"""
    start_time = time.time()
    REQUEST_COUNT.labels(method='POST', endpoint='/papers/state-of-art').inc()
    try:
        response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/papers/state-of-art",
            json=request.dict(),
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Retrieval service error: {str(e)}")

@app.post("/analysis/state-of-art")
async def analyze_state_of_art(request: QueryRequest):
    """Get state of the art analysis"""
    start_time = time.time()
    REQUEST_COUNT.labels(method='POST', endpoint='/analysis/state-of-art').inc()
    try:
        # Get papers from retrieval service
        papers_response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/papers/state-of-art",
            json={
                "topic_name": request.topic_name,
                "year_cutoff": request.year_cutoff,
                "num_papers": 5000
            },
            timeout=60
        )
        papers_response.raise_for_status()
        papers_data = papers_response.json()
        
        # Generate analysis via generation service
        analysis_response = requests.post(
            f"{GENERATION_SERVICE_URL}/analyze/state-of-art",
            json={
                "papers": papers_data.get("papers", []),
                "topic_name": request.topic_name,
                "year_cutoff": request.year_cutoff,
                "num_chunks": request.num_chunks
            },
            timeout=240
        )
        analysis_response.raise_for_status()
        result = analysis_response.json()
        PAPERS_PROCESSED.labels(topic=request.topic_name).inc(len(result.get('papers', [])))
        REQUEST_DURATION.observe(time.time() - start_time)
        return result
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Service communication error: {str(e)}")

@app.get("/papers/year-distribution/{topic_name}")
async def get_year_distribution(topic_name: str):
    """Get year-wise distribution of papers"""
    try:
        response = requests.get(
            f"{RETRIEVAL_SERVICE_URL}/papers/year-distribution/{topic_name}",
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Retrieval service error: {str(e)}")

@app.post("/analysis/topic-evolution")
async def analyze_topic_evolution(request: TopicEvolutionRequest):
    """Generate topic evolution summary"""
    try:
        # Get papers from retrieval service
        papers_response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/papers/top-recent",
            json=request.dict(),
            timeout=60
        )
        papers_response.raise_for_status()
        papers_data = papers_response.json()
        
        # Generate evolution analysis via generation service
        analysis_response = requests.post(
            f"{GENERATION_SERVICE_URL}/analyze/topic-evolution",
            json={
                "papers": papers_data.get("papers", []),
                "topic_name": request.topic_name
            },
            timeout=120
        )
        analysis_response.raise_for_status()
        return analysis_response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Service communication error: {str(e)}")

@app.post("/question/custom")
async def ask_custom_question(request: CustomQuestionRequest):
    """Ask a custom question about the research domain"""
    try:
        # Get relevant papers from retrieval service
        papers_response = requests.post(
            f"{RETRIEVAL_SERVICE_URL}/papers/state-of-art",
            json={
                "topic_name": request.topic_name,
                "year_cutoff": request.year_cutoff,
                "num_papers": 5000
            },
            timeout=60
        )
        papers_response.raise_for_status()
        papers_data = papers_response.json()
        
        # Generate answer via generation service
        answer_response = requests.post(
            f"{GENERATION_SERVICE_URL}/question/answer",
            json={
                "question": request.question,
                "papers": papers_data.get("papers", []),
                "topic_name": request.topic_name,
                "year_cutoff": request.year_cutoff,
                "num_chunks": request.num_chunks
            },
            timeout=300
        )
        answer_response.raise_for_status()
        return answer_response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Service communication error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)