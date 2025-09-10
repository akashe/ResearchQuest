from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any
import os
import time
from data_loader import (
    check_data_presence,
    load_initial_data,
    check_neo4j_connection,
    get_database_stats
)
from custom_logging import logger
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Neo4j Service", version="1.0.0")

# Metrics
DATA_LOADING_DURATION = Histogram('neo4j_service_data_loading_duration_seconds', 'Data loading duration')
NODES_LOADED = Gauge('neo4j_service_nodes_loaded_total', 'Total nodes loaded')
RELATIONSHIPS_LOADED = Gauge('neo4j_service_relationships_loaded_total', 'Total relationships loaded')
DATA_LOADING_PROGRESS = Gauge('neo4j_service_data_loading_progress_percent', 'Data loading progress percentage')

# Global state
data_loading_status = {
    "status": "not_started",  # not_started, loading, completed, failed
    "message": "",
    "progress": 0,
    "start_time": None,
    "end_time": None
}

class DataLoadRequest(BaseModel):
    force_reload: bool = False

@app.get("/health")
async def health_check():
    """Health check for the service"""
    try:
        connection_status = check_neo4j_connection()
        return {
            "status": "healthy",
            "service": "neo4j-service",
            "neo4j_connection": connection_status,
            "data_loading_status": data_loading_status["status"]
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "neo4j-service",
            "error": str(e)
        }

@app.get("/metrics")
async def metrics():
    # Update node/relationship metrics before returning
    try:
        stats = get_database_stats()
        if stats and 'statistics' in stats:
            node_count = stats['statistics'].get('node_count', 0)
            relationship_count = stats['statistics'].get('edge_count', 0)
            # Set the metrics to current values using Gauge.set()
            NODES_LOADED.set(node_count)
            RELATIONSHIPS_LOADED.set(relationship_count)
    except Exception as e:
        logger.warning(f"Could not update node/relationship metrics: {e}")
    
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health/database")
async def database_health():
    """Detailed database health check"""
    try:
        connection_ok = check_neo4j_connection()
        if not connection_ok:
            raise HTTPException(status_code=503, detail="Cannot connect to Neo4j database")
        
        has_data = check_data_presence()
        stats = get_database_stats()
        
        return {
            "status": "healthy",
            "connection": True,
            "has_data": has_data,
            "statistics": stats
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database health check failed: {str(e)}")

@app.get("/data/status")
async def get_data_status():
    """Get current data loading status"""
    try:
        has_data = check_data_presence()
        return {
            "has_data": has_data,
            "loading_status": data_loading_status
        }
    except Exception as e:
        return {
            "has_data": False,
            "loading_status": data_loading_status,
            "error": str(e)
        }

@app.post("/data/load-initial")
async def load_initial_data_endpoint(
    request: DataLoadRequest,
    background_tasks: BackgroundTasks
):
    """Load initial CSV data into Neo4j"""
    global data_loading_status
    
    try:
        # Check if data already exists
        if not request.force_reload:
            has_data = check_data_presence()
            if has_data:
                return {
                    "status": "skipped",
                    "message": "Data already exists. Use force_reload=true to reload.",
                    "data_present": True
                }
        
        # Check if loading is already in progress
        if data_loading_status["status"] == "loading":
            return {
                "status": "in_progress",
                "message": "Data loading is already in progress",
                "progress": data_loading_status["progress"]
            }
        
        # Start background loading
        background_tasks.add_task(run_data_loading)
        
        data_loading_status = {
            "status": "loading",
            "message": "Data loading started",
            "progress": 0,
            "start_time": time.time(),
            "end_time": None
        }
        
        return {
            "status": "started",
            "message": "Initial data loading started in background"
        }
        
    except Exception as e:
        logger.error(f"Failed to start data loading: {str(e)}")
        data_loading_status = {
            "status": "failed",
            "message": f"Failed to start: {str(e)}",
            "progress": 0,
            "start_time": None,
            "end_time": time.time()
        }
        raise HTTPException(status_code=500, detail=f"Failed to start data loading: {str(e)}")

async def run_data_loading():
    """Background task to load data"""
    global data_loading_status
    
    # Start timing the data loading
    start_time = time.time()
    
    try:
        logger.info("Starting initial data loading...")
        data_loading_status["message"] = "Loading nodes..."
        data_loading_status["progress"] = 10
        DATA_LOADING_PROGRESS.set(10)
        
        # Load the data
        load_initial_data()
        
        # Get final database stats for metrics
        try:
            stats = get_database_stats()
            if stats and 'statistics' in stats:
                node_count = stats['statistics'].get('node_count', 0)
                relationship_count = stats['statistics'].get('edge_count', 0)
                NODES_LOADED.set(node_count)
                RELATIONSHIPS_LOADED.set(relationship_count)
                logger.info(f"Updated metrics: {node_count} nodes, {relationship_count} relationships")
        except Exception as e:
            logger.warning(f"Could not update node/relationship metrics: {e}")
        
        # Record completion metrics
        end_time = time.time()
        loading_duration = end_time - start_time
        DATA_LOADING_DURATION.observe(loading_duration)
        DATA_LOADING_PROGRESS.set(100)
        
        data_loading_status = {
            "status": "completed",
            "message": "Initial data loading completed successfully",
            "progress": 100,
            "start_time": data_loading_status["start_time"],
            "end_time": end_time
        }
        
        logger.info(f"Initial data loading completed successfully in {loading_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        data_loading_status = {
            "status": "failed",
            "message": f"Loading failed: {str(e)}",
            "progress": 0,
            "start_time": data_loading_status["start_time"],
            "end_time": time.time()
        }

@app.get("/metrics/database")
async def get_database_metrics():
    """Get database metrics and statistics"""
    try:
        stats = get_database_stats()
        return {
            "metrics": stats,
            "timestamp": time.time()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)