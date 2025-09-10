from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import sys
import os
from custom_logging import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import time

# Add the parent directory to sys.path to import original modules
# sys.path.append('/Users/akashkumar/projects/ResearchQuest')
from neo4j_operations import (
    create_topic_subgraph, 
    check_top_papers_from_last_3_years,
    get_year_wise_distribution,
    get_state_of_the_art_analysis
)

app = FastAPI(title="Retrieval Service", version="1.0.0")

# Metrics
NEO4J_QUERY_DURATION = Histogram('retrieval_service_neo4j_query_duration_seconds', 'Neo4j query duration', ['query_type'])
PAPERS_RETRIEVED = Counter('retrieval_service_papers_retrieved_total', 'Total papers retrieved', ['topic', 'operation'])
SUBGRAPH_CREATION_DURATION = Histogram('retrieval_service_subgraph_creation_duration_seconds', 'Subgraph creation duration')
REQUEST_COUNT = Counter('retrieval_service_requests_total', 'Total requests', ['endpoint'])

class SubgraphRequest(BaseModel):
    topic: str
    topic_name: str
    validate_relationships: bool = True

class QueryRequest(BaseModel):
    topic_name: str
    year_cutoff: Optional[int] = None
    num_papers: Optional[int] = 20
    from_year: Optional[int] = 2022

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "retrieval-service"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/subgraph/create")
async def create_subgraph_endpoint(request: SubgraphRequest):
    """Create a topic subgraph and compute PageRank"""
    start_time = time.time()
    REQUEST_COUNT.labels(endpoint='subgraph_create').inc()
    try:
        graph_name = f"subgraph_{request.topic_name.replace(' ', '_')}"
        logger.info(f"Creating subgraph '{graph_name}' ")
        create_topic_subgraph(
            request.topic, 
            request.topic_name, 
            graph_name, 
            request.validate_relationships
        )
        SUBGRAPH_CREATION_DURATION.observe(time.time() - start_time)
        return {
            "status": "success",
            "message": f"Subgraph '{graph_name}' created successfully",
            "graph_name": graph_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create subgraph: {str(e)}")

@app.post("/papers/top-recent")
async def get_top_recent_papers(request: QueryRequest):
    """Get top papers from recent years"""
    try:
        papers = check_top_papers_from_last_3_years(
            request.topic_name,
            no_of_papers=request.num_papers,
            from_year=request.from_year
        )
        PAPERS_RETRIEVED.labels(topic=request.topic_name, operation='top_recent').inc(len(papers))
        return {
            "papers": papers,
            "count": len(papers),
            "topic_name": request.topic_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve papers: {str(e)}")

@app.get("/papers/year-distribution/{topic_name}")
async def get_year_distribution(topic_name: str):
    """Get year-wise distribution of papers"""
    start_time = time.time()
    REQUEST_COUNT.labels(endpoint='year_distribution').inc()
    try:
        distribution = get_year_wise_distribution(topic_name)
        NEO4J_QUERY_DURATION.labels(query_type='year_distribution').observe(time.time() - start_time)
        return {
            "distribution": distribution,
            "topic_name": topic_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get distribution: {str(e)}")

@app.post("/papers/state-of-art")
async def get_state_of_art_papers(request: QueryRequest):
    """Get papers for state of the art analysis"""
    start_time = time.time()
    REQUEST_COUNT.labels(endpoint='state_of_art').inc()
    try:
        papers = get_state_of_the_art_analysis(
            request.year_cutoff or 2022,
            request.topic_name,
            top_papers_each_year=request.num_papers or 500
        )
        PAPERS_RETRIEVED.labels(topic=request.topic_name, operation='state_of_art').inc(len(papers))
        NEO4J_QUERY_DURATION.labels(query_type='state_of_art_papers').observe(time.time() - start_time)
        return {
            "papers": papers,
            "count": len(papers),
            "topic_name": request.topic_name,
            "year_cutoff": request.year_cutoff
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve papers: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)