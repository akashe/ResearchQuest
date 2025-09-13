import asyncio
import json
import logging
import os
import pandas as pd
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/data/graph-builder.log')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Graph Builder Service", version="1.0.0")

# Prometheus metrics
GRAPH_BUILD_COUNTER = Counter('graph_builds_total', 'Total graph builds')
GRAPH_BUILD_ERRORS = Counter('graph_build_errors_total', 'Graph build errors', ['error_type'])
GRAPH_BUILD_DURATION = Histogram('graph_build_duration_seconds', 'Graph build duration')
CPP_EXECUTION_COUNTER = Counter('cpp_executions_total', 'C++ executions')

# Configuration
DATA_PATH = Path("/data")
BUILD_GRAPH_PATH = Path("/app/build_graph")
NEO4J_SERVICE_URL = os.getenv("NEO4J_SERVICE_URL", "http://neo4j-service:8003")

# Error tracking
class ErrorTracker:
    def __init__(self):
        self.errors = {
            'missing_semantic_ids': [],
            'missing_references': [],
            'api_failures': [],
            'processing_failures': []
        }
        self.error_file = DATA_PATH / "graph_builder_errors.json"
    
    def log_error(self, error_type: str, paper_id: str, details: str):
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'paper_id': paper_id,
            'details': details
        }
        self.errors[error_type].append(error_entry)
        logger.error(f"{error_type}: {paper_id} - {details}")
        GRAPH_BUILD_ERRORS.labels(error_type=error_type).inc()
        
        # Save to file for persistence
        with open(self.error_file, 'w') as f:
            json.dump(self.errors, f, indent=2)
    
    def get_error_summary(self) -> Dict:
        return {
            'missing_semantic_ids': len(self.errors['missing_semantic_ids']),
            'missing_references': len(self.errors['missing_references']),
            'api_failures': len(self.errors['api_failures']),
            'processing_failures': len(self.errors['processing_failures']),
            'total_errors': sum(len(v) for v in self.errors.values())
        }

error_tracker = ErrorTracker()

class GraphBuildRequest(BaseModel):
    force_rebuild: bool = False
    timeout_minutes: int = 120  # 2 hours default

class GraphBuildStatus(BaseModel):
    status: str
    message: str
    progress: Dict
    errors: Dict
    start_time: Optional[str]
    end_time: Optional[str]

class CitationsBatch(BaseModel):
    citations: List[Dict]
    source_service: str = "data-pipeline-service"

class ProcessedPapersBatch(BaseModel):
    paper_ids: List[str]
    batch_size: int = 1000

# Global status tracking
build_status = {
    'status': 'idle',
    'message': 'Ready to build graph',
    'progress': {},
    'errors': {},
    'start_time': None,
    'end_time': None,
    'current_task': None
}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "graph-builder"}

@app.get("/metrics")
async def metrics():
    return generate_latest()

@app.get("/status")
async def get_status():
    return GraphBuildStatus(**build_status)

@app.get("/errors")
async def get_errors():
    return error_tracker.get_error_summary()

@app.get("/errors/detailed")
async def get_detailed_errors():
    return error_tracker.errors

def load_existing_data() -> Tuple[Set[str], Set[str]]:
    """Load existing processed papers AND fetch data from other services via APIs"""
    try:
        # Load local arXiv papers first
        arxiv_data_path = DATA_PATH / "arxiv_data.pkl"
        existing_arxiv_ids = set()
        if arxiv_data_path.exists():
            df = pd.read_pickle(arxiv_data_path)
            existing_arxiv_ids = set(df['id'].astype(str))
            logger.info(f"Loaded {len(existing_arxiv_ids)} existing local arXiv IDs")
        
        # Also fetch arXiv paper IDs from arxiv-ingestion-service
        try:
            arxiv_service_ids = fetch_arxiv_paper_ids_from_service()
            if arxiv_service_ids:
                existing_arxiv_ids.update(arxiv_service_ids)
                logger.info(f"Fetched {len(arxiv_service_ids)} paper IDs from arxiv-ingestion-service")
        except Exception as e:
            logger.warning(f"Could not fetch arXiv paper IDs from service: {e}")
        
        # Load local processed references if available
        references_path = DATA_PATH / "references_complete.jsonl"
        processed_semantic_ids = set()
        if references_path.exists():
            with open(references_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        citation = json.loads(line)
                        if 'citingPaperId' in citation:
                            processed_semantic_ids.add(citation['citingPaperId'])
                        if line_num % 100000 == 0:
                            logger.info(f"Processed {line_num} reference lines")
                    except json.JSONDecodeError:
                        error_tracker.log_error('processing_failures', f'line_{line_num}', 'Invalid JSON in references file')
                        continue
            logger.info(f"Loaded {len(processed_semantic_ids)} local processed semantic IDs")
        
        # PRIMARY: Fetch processed semantic IDs from data-pipeline-service (the authoritative source)
        try:
            pipeline_semantic_ids = fetch_processed_semantic_ids_from_service()
            if pipeline_semantic_ids:
                # Replace local set with authoritative data from data-pipeline-service
                processed_semantic_ids = pipeline_semantic_ids
                logger.info(f"Using {len(pipeline_semantic_ids)} processed semantic IDs from data-pipeline-service (authoritative)")
        except Exception as e:
            logger.warning(f"Could not fetch processed semantic IDs from service, using local data: {e}")
        
        return existing_arxiv_ids, processed_semantic_ids
    
    except Exception as e:
        logger.error(f"Error loading existing data: {e}")
        error_tracker.log_error('processing_failures', 'data_loading', str(e))
        return set(), set()

def fetch_arxiv_paper_ids_from_service() -> Set[str]:
    """Fetch paper IDs from arxiv-ingestion-service via batch API"""
    paper_ids = set()
    offset = 0
    limit = 1000
    
    while True:
        try:
            response = requests.get(
                f"http://arxiv-ingestion-service:8004/batch/paper-ids",
                params={"limit": limit, "offset": offset},
                timeout=30
            )
            
            if response.status_code != 200:
                logger.warning(f"Error fetching paper IDs: {response.status_code}")
                break
                
            data = response.json()
            batch_ids = data.get('paper_ids', [])
            
            if not batch_ids:
                break
                
            paper_ids.update(batch_ids)
            
            if not data.get('has_more', False):
                break
                
            offset += limit
            
        except Exception as e:
            logger.error(f"Error fetching paper IDs batch at offset {offset}: {e}")
            break
    
    return paper_ids

def fetch_processed_semantic_ids_from_service() -> Set[str]:
    """Fetch processed semantic IDs from data-pipeline-service via batch API"""
    semantic_ids = set()
    offset = 0
    limit = 1000
    
    while True:
        try:
            response = requests.get(
                f"http://data-pipeline-service:8005/batch/processed-ids",
                params={"limit": limit, "offset": offset},
                timeout=30
            )
            
            if response.status_code != 200:
                logger.warning(f"Error fetching processed semantic IDs: {response.status_code}")
                break
                
            data = response.json()
            batch_ids = data.get('semantic_ids', [])
            
            if not batch_ids:
                break
                
            semantic_ids.update(batch_ids)
            
            if not data.get('has_more', False):
                break
                
            offset += limit
            
        except Exception as e:
            logger.error(f"Error fetching semantic IDs batch at offset {offset}: {e}")
            break
    
    return semantic_ids

def update_status(status: str, message: str, current_task: str = None, progress: Dict = None):
    """Update global build status"""
    build_status['status'] = status
    build_status['message'] = message
    build_status['current_task'] = current_task
    if progress:
        build_status['progress'].update(progress)
    build_status['errors'] = error_tracker.get_error_summary()
    logger.info(f"Status: {status} - {message}")

async def run_cpp_graph_builder(timeout_minutes: int = 120) -> Tuple[bool, str]:
    """Execute C++ graph builder with timeout handling"""
    try:
        update_status('running', 'Executing C++ graph builder', 'cpp_execution')
        
        # Prepare input file
        input_csv = DATA_PATH / "semantic_scholar_paper_details_pruned_for_c_code.csv"
        if not input_csv.exists():
            error_msg = f"Input CSV file not found: {input_csv}"
            error_tracker.log_error('processing_failures', 'cpp_input', error_msg)
            return False, error_msg
        
        # Copy main.cpp to data directory for execution
        cpp_source = BUILD_GRAPH_PATH / "main.cpp"
        cpp_target = DATA_PATH / "main.cpp"
        
        if cpp_source.exists():
            import shutil
            shutil.copy2(cpp_source, cpp_target)
        
        # Compile C++ code (assuming dependencies are installed)
        compile_cmd = [
            "g++", "-O3", "-std=c++17",
            str(cpp_target),
            "-lboost_system", "-lboost_graph", "-lsqlite3",
            "-I/usr/include/eigen3",
            "-o", str(DATA_PATH / "graph_builder")
        ]
        
        logger.info(f"Compiling C++ code: {' '.join(compile_cmd)}")
        compile_result = subprocess.run(
            compile_cmd, 
            cwd=str(DATA_PATH),
            capture_output=True, 
            text=True, 
            timeout=300  # 5 minutes for compilation
        )
        
        if compile_result.returncode != 0:
            error_msg = f"C++ compilation failed: {compile_result.stderr}"
            error_tracker.log_error('processing_failures', 'cpp_compilation', error_msg)
            return False, error_msg
        
        # Execute compiled binary
        execute_cmd = [str(DATA_PATH / "graph_builder")]
        logger.info(f"Executing graph builder: {' '.join(execute_cmd)}")
        
        start_time = time.time()
        execute_result = subprocess.run(
            execute_cmd,
            cwd=str(DATA_PATH),
            capture_output=True,
            text=True,
            timeout=timeout_minutes * 60
        )
        
        execution_time = time.time() - start_time
        GRAPH_BUILD_DURATION.observe(execution_time)
        CPP_EXECUTION_COUNTER.inc()
        
        if execute_result.returncode != 0:
            error_msg = f"C++ execution failed: {execute_result.stderr}"
            error_tracker.log_error('processing_failures', 'cpp_execution', error_msg)
            return False, error_msg
        
        # Verify output files were created
        nodes_csv = DATA_PATH / "citation_nodes.csv"
        edges_csv = DATA_PATH / "citation_edges.csv"
        
        if not nodes_csv.exists() or not edges_csv.exists():
            error_msg = "C++ execution completed but output CSV files not found"
            error_tracker.log_error('processing_failures', 'cpp_output', error_msg)
            return False, error_msg
        
        logger.info(f"C++ graph builder completed successfully in {execution_time:.2f} seconds")
        return True, f"Graph built successfully. Nodes: {nodes_csv}, Edges: {edges_csv}"
        
    except subprocess.TimeoutExpired:
        error_msg = f"C++ execution timed out after {timeout_minutes} minutes"
        error_tracker.log_error('processing_failures', 'cpp_timeout', error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"C++ execution error: {str(e)}"
        error_tracker.log_error('processing_failures', 'cpp_error', error_msg)
        return False, error_msg

async def trigger_neo4j_import() -> Tuple[bool, str]:
    """Trigger Neo4j service to import the generated CSV files"""
    try:
        update_status('running', 'Triggering Neo4j import via service endpoints', 'neo4j_import')
        
        # Check if CSV files exist
        nodes_csv = DATA_PATH / "citation_nodes.csv"
        edges_csv = DATA_PATH / "citation_edges.csv"
        
        if not nodes_csv.exists() or not edges_csv.exists():
            error_msg = "CSV files not found for Neo4j import"
            error_tracker.log_error('processing_failures', 'neo4j_csv_missing', error_msg)
            return False, error_msg
        
        # Trigger Neo4j service import endpoint
        import_payload = {
            "nodes_csv_path": str(nodes_csv),
            "edges_csv_path": str(edges_csv),
            "force_rebuild": True
        }
        
        logger.info(f"Calling Neo4j service import endpoint: {NEO4J_SERVICE_URL}/import-graph")
        response = requests.post(
            f"{NEO4J_SERVICE_URL}/import-graph",
            json=import_payload,
            timeout=1800  # 30 minutes timeout
        )
        
        if response.status_code != 200:
            error_msg = f"Neo4j import failed: {response.status_code} - {response.text}"
            error_tracker.log_error('api_failures', 'neo4j_import', error_msg)
            return False, error_msg
        
        result = response.json()
        logger.info(f"Neo4j import successful: {result}")
        return True, f"Neo4j import completed: {result.get('message', 'Success')}"
        
    except requests.exceptions.Timeout:
        error_msg = "Neo4j import request timed out"
        error_tracker.log_error('api_failures', 'neo4j_timeout', error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Neo4j import error: {str(e)}"
        error_tracker.log_error('processing_failures', 'neo4j_import', error_msg)
        return False, error_msg

@app.post("/build-graph")
async def build_graph(request: GraphBuildRequest, background_tasks: BackgroundTasks):
    """Trigger graph building process"""
    if build_status['status'] in ['running', 'building']:
        raise HTTPException(status_code=409, detail="Graph build already in progress")
    
    background_tasks.add_task(execute_graph_build, request.timeout_minutes)
    return {"message": "Graph build started", "timeout_minutes": request.timeout_minutes}

async def execute_graph_build(timeout_minutes: int):
    """Execute the complete graph building pipeline"""
    try:
        build_status['start_time'] = datetime.now().isoformat()
        build_status['end_time'] = None
        GRAPH_BUILD_COUNTER.inc()
        
        update_status('building', 'Starting graph build process', 'initialization')
        
        # Step 1: Load existing data (SINGLE LOAD)
        existing_arxiv_ids, processed_semantic_ids = load_existing_data()
        update_status('building', 'Loaded existing data', 'data_loading', {
            'existing_arxiv_papers': len(existing_arxiv_ids),
            'processed_semantic_ids': len(processed_semantic_ids)
        })
        
        # Step 2: Execute C++ graph builder
        cpp_success, cpp_message = await run_cpp_graph_builder(timeout_minutes)
        if not cpp_success:
            update_status('failed', f'C++ execution failed: {cpp_message}', 'cpp_execution')
            return
        
        update_status('building', cpp_message, 'cpp_execution')
        
        # Step 3: Trigger Neo4j service import (using existing endpoints)
        neo4j_success, neo4j_message = await trigger_neo4j_import()
        if not neo4j_success:
            update_status('failed', f'Neo4j import failed: {neo4j_message}', 'neo4j_import')
            return
        
        # Success
        build_status['end_time'] = datetime.now().isoformat()
        update_status('completed', f'Graph build completed successfully. {neo4j_message}', 'completed')
        
    except Exception as e:
        build_status['end_time'] = datetime.now().isoformat()
        error_msg = f"Graph build failed with error: {str(e)}"
        error_tracker.log_error('processing_failures', 'graph_build', error_msg)
        update_status('failed', error_msg, 'error')

# Batch processing endpoints for handling large datasets
@app.post("/batch/citations")
async def receive_citations_batch(batch: CitationsBatch):
    """Receive batch of citations from data pipeline service"""
    if len(batch.citations) > 1000:
        raise HTTPException(status_code=400, detail="Batch too large. Maximum 1000 citations per batch.")
    
    try:
        # Append citations to references file
        citations_file = DATA_PATH / "references_complete.jsonl"
        
        with open(citations_file, 'a') as f:
            for citation in batch.citations:
                f.write(json.dumps(citation) + '\n')
        
        logger.info(f"Received and stored {len(batch.citations)} citations from {batch.source_service}")
        
        return {
            "status": "received",
            "citations_count": len(batch.citations),
            "source_service": batch.source_service,
            "message": f"Successfully stored {len(batch.citations)} citations"
        }
        
    except Exception as e:
        logger.error(f"Error receiving citations batch: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to store citations: {str(e)}")

@app.get("/batch/processed-papers")
async def get_processed_papers_batch(limit: int = 1000, offset: int = 0):
    """Get batch of processed paper IDs for other services"""
    try:
        existing_arxiv_ids, _ = load_existing_data()
        paper_ids_list = list(existing_arxiv_ids)
        
        # Apply pagination
        start_idx = offset
        end_idx = min(offset + limit, len(paper_ids_list))
        batch = paper_ids_list[start_idx:end_idx]
        
        return {
            "paper_ids": batch,
            "total_count": len(paper_ids_list),
            "batch_size": len(batch),
            "offset": offset,
            "has_more": end_idx < len(paper_ids_list)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving processed papers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve processed papers: {str(e)}")

@app.post("/batch/request-semantic-ids")
async def request_semantic_ids_batch(request: ProcessedPapersBatch):
    """Request semantic scholar IDs for a batch of paper IDs from data pipeline service"""
    if len(request.paper_ids) > 1000:
        raise HTTPException(status_code=400, detail="Batch too large. Maximum 1000 paper IDs per request.")
    
    try:
        # Call data pipeline service to get semantic IDs
        data_pipeline_url = "http://data-pipeline-service:8005"
        
        payload = {
            "semantic_ids": request.paper_ids,  # These are actually arxiv IDs that need semantic mapping
            "batch_size": min(request.batch_size, 1000)
        }
        
        response = requests.post(
            f"{data_pipeline_url}/batch/semantic-ids/transfer",
            json=payload,
            timeout=300
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=f"Data pipeline service error: {response.text}")
        
        result = response.json()
        logger.info(f"Requested semantic IDs for {len(request.paper_ids)} papers")
        
        return {
            "status": "requested",
            "paper_ids_count": len(request.paper_ids),
            "data_pipeline_response": result,
            "message": f"Successfully requested semantic IDs for {len(request.paper_ids)} papers"
        }
        
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Request to data pipeline service timed out")
    except Exception as e:
        logger.error(f"Error requesting semantic IDs batch: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to request semantic IDs: {str(e)}")

@app.get("/data/files")
async def get_data_files():
    """Get list of data files available for graph building"""
    try:
        files_info = []
        
        for file_path in DATA_PATH.iterdir():
            if file_path.is_file():
                stat = file_path.stat()
                files_info.append({
                    "name": file_path.name,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
        
        return {
            "data_directory": str(DATA_PATH),
            "files": files_info,
            "total_files": len(files_info)
        }
        
    except Exception as e:
        logger.error(f"Error listing data files: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list data files: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006)