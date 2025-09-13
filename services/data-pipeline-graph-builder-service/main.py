from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Set, Tuple
import json
import os
import time
import logging
import random
import pandas as pd
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from kafka import KafkaProducer, KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import redis
import asyncio
import requests
from concurrent.futures import ThreadPoolExecutor
import traceback

app = FastAPI(title="Data Pipeline Graph Builder Service", version="1.0.0")

# Configure extensive logging
logging.basicConfig(
    level=logging.DEBUG,  # More detailed logging
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/data/data-pipeline-graph-builder.log'),
        logging.FileHandler('/data/data-pipeline-graph-builder-debug.log')  # Separate debug log
    ]
)
logger = logging.getLogger(__name__)

# Add performance logging
perf_logger = logging.getLogger('performance')
perf_handler = logging.FileHandler('/data/performance.log')
perf_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
perf_logger.addHandler(perf_handler)
perf_logger.setLevel(logging.INFO)

logger.info("=== Data Pipeline Graph Builder Service Starting ===")
logger.info("Logging configuration completed - DEBUG level enabled")

# Semantic Scholar API Configuration (from original build_graph files)
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_FIELDS = "url,year,citationCount,tldr"
REFERENCES_FIELDS = "paperId,title,contexts,year,citationCount,abstract"

# Data paths (using individual PVC mount)
DATA_PATH = Path("/data")
REFERENCES_FILE = DATA_PATH / "references_complete.jsonl"
# NOTE: arxiv_data.pkl stays with arxiv-ingestion-service - we get paper data from Kafka
ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE = DATA_PATH / "arxiv_papers_with_semantic_scholar_ids.csv"
SEMANTIC_PAPER_DETAILS_FOR_C_FILE = DATA_PATH / "semantic_scholar_paper_details_for_c_code.csv"
SEMANTIC_PAPER_DETAILS_PRUNED_FOR_C_FILE = DATA_PATH / "semantic_scholar_paper_details_pruned_for_c_code.csv"
BUILD_GRAPH_PATH = Path("/app/build_graph")

# Log file paths for debugging
logger.info(f"Data paths configured:")
logger.info(f"  DATA_PATH: {DATA_PATH}")
logger.info(f"  REFERENCES_FILE: {REFERENCES_FILE}")
logger.info(f"  BUILD_GRAPH_PATH: {BUILD_GRAPH_PATH}")
logger.info(f"  Files will be processed via Kafka events, not direct file access")

# Metrics - Pipeline
KAFKA_MESSAGES_PRODUCED = Counter('pipeline_kafka_messages_produced_total', 'Total messages produced to Kafka', ['topic'])
KAFKA_MESSAGES_CONSUMED = Counter('pipeline_kafka_messages_consumed_total', 'Total messages consumed from Kafka', ['topic'])
PROCESSING_DURATION = Histogram('pipeline_processing_duration_seconds', 'Processing duration', ['operation'])
PIPELINE_ERRORS = Counter('pipeline_errors_total', 'Pipeline errors', ['stage', 'error_type'])
PAPERS_IN_QUEUE = Gauge('pipeline_papers_in_queue', 'Papers waiting in processing queue')

# Metrics - Graph Builder
GRAPH_BUILD_COUNTER = Counter('graph_builds_total', 'Total graph builds')
GRAPH_BUILD_ERRORS = Counter('graph_build_errors_total', 'Graph build errors', ['error_type'])
GRAPH_BUILD_DURATION = Histogram('graph_build_duration_seconds', 'Graph build duration')
CPP_EXECUTION_COUNTER = Counter('cpp_executions_total', 'C++ executions')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))

# Initialize connections (will be done in startup)
kafka_producer = None
redis_client = None
executor = ThreadPoolExecutor(max_workers=4)

# Pipeline state
pipeline_status = {
    "status": "idle",  # idle, processing, error
    "last_processed": None,
    "papers_processed": 0,
    "errors": []
}

# Graph build state
build_status = {
    'status': 'idle',
    'message': 'Ready to build graph',
    'progress': {},
    'errors': {},
    'start_time': None,
    'end_time': None,
    'current_task': None
}

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
        try:
            with open(self.error_file, 'w') as f:
                json.dump(self.errors, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving error tracker file: {e}")
    
    def get_error_summary(self) -> Dict:
        return {
            'missing_semantic_ids': len(self.errors['missing_semantic_ids']),
            'missing_references': len(self.errors['missing_references']),
            'api_failures': len(self.errors['api_failures']),
            'processing_failures': len(self.errors['processing_failures']),
            'total_errors': sum(len(v) for v in self.errors.values())
        }

error_tracker = ErrorTracker()

# Data Models
class PaperEvent(BaseModel):
    id: str
    title: str
    authors: List[str]
    categories: str
    abstract: str
    update_date: str
    event_type: str = "new_paper"
    timestamp: datetime = None

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

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka and Redis connections"""
    global kafka_producer, redis_client
    
    try:
        # Initialize Kafka Producer
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            retry_backoff_ms=1000,
            retries=3
        )
        logger.info("Kafka producer initialized successfully")
        
        # Initialize Redis
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        
        # Test Redis connection
        redis_client.ping()
        logger.info("Redis connection established successfully")
        
        # Start background consumers
        asyncio.create_task(consume_new_papers())
        asyncio.create_task(consume_enriched_papers())
        asyncio.create_task(consume_quality_filtered_papers())
        
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        PIPELINE_ERRORS.labels(stage='startup', error_type='connection').inc()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    kafka_healthy = kafka_producer is not None
    redis_healthy = False
    
    if redis_client:
        try:
            redis_client.ping()
            redis_healthy = True
        except:
            pass
    
    return {
        "status": "healthy" if (kafka_healthy and redis_healthy) else "degraded",
        "service": "data-pipeline-graph-builder-service",
        "kafka": "connected" if kafka_healthy else "disconnected",
        "redis": "connected" if redis_healthy else "disconnected",
        "pipeline_status": pipeline_status["status"],
        "graph_build_status": build_status["status"]
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
async def get_pipeline_status():
    """Get current pipeline status"""
    return pipeline_status

@app.get("/graph/status")
async def get_graph_build_status():
    """Get current graph build status"""
    return GraphBuildStatus(**build_status)

@app.get("/errors")
async def get_errors():
    """Get error summary"""
    return error_tracker.get_error_summary()

@app.get("/errors/detailed")
async def get_detailed_errors():
    """Get detailed errors"""
    return error_tracker.errors

# Kafka Consumer - processes individual papers from arxiv-ingestion-service
async def consume_new_papers():
    """Background consumer for new papers topic - processes individual papers"""
    logger.info("=== Setting up Kafka consumer for new-papers topic ===")
    
    try:
        consumer = KafkaConsumer(
            'new-papers',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='data-pipeline-graph-builder-service',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("✓ Kafka consumer initialized successfully")
        logger.info(f"  Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"  Group ID: data-pipeline-graph-builder-service")
        logger.info("🎧 Started consuming from new-papers topic - waiting for messages...")
        
        for message in consumer:
            start_time = time.time()
            try:
                paper_data = message.value
                paper_id = paper_data.get('id', 'unknown')
                
                logger.info(f"📨 Received paper from Kafka: {paper_id}")
                logger.debug(f"Paper data keys: {list(paper_data.keys())}")
                
                KAFKA_MESSAGES_CONSUMED.labels(topic='new-papers').inc()
                
                # Process this individual paper through semantic enrichment
                await process_individual_paper(paper_data)
                
                processing_time = time.time() - start_time
                perf_logger.info(f"Paper {paper_id} processed in {processing_time:.2f}s")
                logger.info(f"✓ Successfully processed paper {paper_id} in {processing_time:.2f}s")
                
            except Exception as e:
                error_paper_id = message.value.get('id', 'unknown') if isinstance(message.value, dict) else 'parse_error'
                logger.error(f"❌ Error processing paper {error_paper_id}: {e}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
                PIPELINE_ERRORS.labels(stage='paper_processing', error_type='processing_error').inc()
                
    except Exception as e:
        logger.error(f"💥 Critical error in Kafka consumer: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        PIPELINE_ERRORS.labels(stage='consume', error_type='kafka_error').inc()

async def process_individual_paper(paper_data: Dict):
    """Process a single paper through semantic enrichment (called per paper from Kafka)"""
    paper_id = paper_data.get('id', 'unknown')
    logger.info(f"🔍 Starting semantic enrichment for paper: {paper_id}")
    
    try:
        # Step 1: Get semantic scholar ID for this paper
        logger.debug(f"Step 1: Getting Semantic Scholar ID for {paper_id}")
        semantic_id = await get_semantic_scholar_id_for_paper(paper_data)
        
        if not semantic_id:
            logger.warning(f"⚠️  No Semantic Scholar ID found for paper {paper_id}")
            return
            
        # Step 2: Get citation details if not already processed
        logger.debug(f"Step 2: Getting citation details for semantic ID: {semantic_id}")
        await get_citation_details_for_paper(semantic_id, paper_id)
        
        # Update CSV with this paper's semantic data
        logger.debug(f"Step 3: Updating CSV with semantic data for {paper_id}")
        await update_papers_csv_with_semantic_data(paper_data, semantic_id)
        
        logger.info(f"✓ Completed semantic enrichment for paper {paper_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in semantic enrichment for paper {paper_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

async def get_semantic_scholar_id_for_paper(paper_data: Dict) -> Optional[str]:
    """Get semantic scholar ID for a single paper from Kafka data"""
    paper_id = paper_data.get('id', 'unknown')
    arxiv_id = f"ARXIV:{paper_id}"
    
    logger.debug(f"🔍 Fetching Semantic Scholar ID for arXiv ID: {arxiv_id}")
    
    try:
        url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/batch/"
        params = {'fields': SEMANTIC_SCHOLAR_FIELDS}
        json_data = {"ids": [arxiv_id]}
        
        logger.debug(f"API Request: {url} with params: {params}")
        
        response = requests.post(url, params=params, json=json_data, timeout=30)
        logger.debug(f"API Response status: {response.status_code}")
        
        if response.status_code == 200:
            results = response.json()
            logger.debug(f"API Response data: {results}")
            
            if results and len(results) > 0 and results[0] is not None:
                paper_info = results[0]
                semantic_id = paper_info.get('paperId')
                logger.info(f"✓ Found Semantic Scholar ID for {paper_id}: {semantic_id}")
                return semantic_id
            else:
                logger.warning(f"⚠️  No Semantic Scholar data found for {paper_id}")
                return None
        else:
            logger.error(f"❌ API request failed with status {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching Semantic Scholar ID for {paper_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None

async def get_citation_details_for_paper(semantic_id: str, paper_id: str):
    """Get citation details for a single paper"""
    logger.debug(f"🔍 Checking if citations already processed for semantic ID: {semantic_id}")
    
    # Check if already processed
    processed_ids = await load_processed_paper_ids()
    if semantic_id in processed_ids:
        logger.info(f"✓ Citations already processed for semantic ID: {semantic_id}")
        return
    
    logger.info(f"📚 Fetching new citations for semantic ID: {semantic_id}")
    
    try:
        references = await fetch_references(semantic_id)
        
        if references == -1:
            logger.error(f"❌ Failed to fetch references for semantic ID: {semantic_id}")
            return
            
        if len(references) == 0:
            logger.info(f"📝 No references found for semantic ID: {semantic_id}")
            # Save to no-references tracking file
            await save_paper_with_no_references(semantic_id)
        else:
            logger.info(f"📚 Found {len(references)} references for semantic ID: {semantic_id}")
            # Add citing paper ID and save
            for reference in references:
                reference['citingPaperId'] = semantic_id
            
            await save_references_to_jsonl(references)
            logger.info(f"💾 Saved {len(references)} references for semantic ID: {semantic_id}")
            
    except Exception as e:
        logger.error(f"❌ Error getting citations for semantic ID {semantic_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

async def update_papers_csv_with_semantic_data(paper_data: Dict, semantic_id: str):
    """Update CSV file with semantic scholar data for this paper"""
    paper_id = paper_data.get('id', 'unknown')
    logger.debug(f"📊 Updating CSV with semantic data for paper: {paper_id}")
    
    try:
        # Create row with paper data + semantic ID
        row_data = {
            'id': paper_id,
            'paperId': semantic_id,
            'title': paper_data.get('title', ''),
            'authors': paper_data.get('authors', []),
            'categories': paper_data.get('categories', ''),
            'abstract': paper_data.get('abstract', ''),
            'update_date': paper_data.get('update_date', ''),
            # Will be filled with semantic scholar data later
            'citationCount': 0,
            'year': 0,
            'url': '',
            'tldr': 'Processing...'
        }
        
        # Append to CSV (in production, this might be batched)
        csv_file = ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE
        df = pd.DataFrame([row_data])
        
        if csv_file.exists():
            df.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_file, index=False)
            
        logger.debug(f"💾 Updated CSV with data for paper: {paper_id}")
        
    except Exception as e:
        logger.error(f"❌ Error updating CSV for paper {paper_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

async def save_paper_with_no_references(semantic_id: str):
    """Save paper ID to no-references tracking file"""
    try:
        no_refs_file = DATA_PATH / "semantic_ids_with_no_references.json"
        existing = []
        
        if no_refs_file.exists():
            with open(no_refs_file, "r") as f:
                existing = json.load(f)
        
        if semantic_id not in existing:
            existing.append(semantic_id)
            with open(no_refs_file, "w") as f:
                json.dump(existing, f)
            logger.debug(f"📝 Added {semantic_id} to no-references tracking file")
        
    except Exception as e:
        logger.error(f"❌ Error saving no-references tracking for {semantic_id}: {e}")

# Airflow-triggerable endpoints (replacing sequential pipeline)
@app.post("/airflow/step1-process-papers")
async def airflow_step1_process_papers():
    """Airflow endpoint: Process papers that have been received via Kafka"""
    logger.info("🚀 Airflow Step 1: Processing papers received via Kafka")
    
    try:
        # This step is actually handled by the Kafka consumer
        # We just return the current status
        processed_ids = await load_processed_paper_ids()
        
        return {
            "status": "success",
            "message": "Papers are being processed via Kafka consumer",
            "processed_count": len(processed_ids),
            "note": "This step runs continuously via Kafka consumer"
        }
        
    except Exception as e:
        logger.error(f"❌ Airflow Step 1 error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/airflow/step2-prune-data")
async def airflow_step2_prune_data():
    """Airflow endpoint: Prune data based on quality filters"""
    logger.info("🚀 Airflow Step 2: Starting data pruning")
    
    try:
        await prune_data()
        return {
            "status": "success", 
            "message": "Data pruning completed",
            "pruned_file": str(SEMANTIC_PAPER_DETAILS_PRUNED_FOR_C_FILE)
        }
        
    except Exception as e:
        logger.error(f"❌ Airflow Step 2 error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/airflow/step3-build-graph")
async def airflow_step3_build_graph():
    """Airflow endpoint: Build graph using C++ code"""
    logger.info("🚀 Airflow Step 3: Starting graph building")
    
    try:
        success, message = await run_cpp_graph_builder()
        
        if success:
            return {
                "status": "success",
                "message": message,
                "output_files": {
                    "nodes": str(DATA_PATH / "citation_nodes.csv"),
                    "edges": str(DATA_PATH / "citation_edges.csv")
                }
            }
        else:
            raise HTTPException(status_code=500, detail=message)
            
    except Exception as e:
        logger.error(f"❌ Airflow Step 3 error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

# NOTE: The old batch processing functions are removed since we now process papers 
# individually via Kafka. The CSV building happens incrementally as papers are processed.

async def load_processed_paper_ids():
    """Load processed paper IDs from references file"""
    processed_ids = set()
    if REFERENCES_FILE.exists():
        try:
            with open(REFERENCES_FILE, 'r') as f:
                for line in f:
                    citation = json.loads(line)
                    if 'citingPaperId' in citation:
                        processed_ids.add(citation['citingPaperId'])
        except Exception as e:
            logger.error(f"Error loading processed paper IDs: {e}")
    return processed_ids

async def fetch_references(paper_id, max_retries=8):
    """Fetch references for a paper (from original script)"""
    references = []
    offset = 0
    limit = 1000
    
    while True:
        url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/{paper_id}/references?offset={offset}&limit={limit}&fields={REFERENCES_FIELDS}"
        
        success = False
        backoff_base = 2
        
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(random.uniform(1, 2))  # Rate limiting
                response = requests.get(url, timeout=60)
                response_json = response.json()
                
                if 'message' in response_json or 'error' in response_json:
                    raise Exception(f"API error: {response_json}")
                
                references.extend(response_json['data'])
                next_offset = response_json.get('next', None)
                success = True
                break
                
            except Exception as e:
                logger.warning(f"Error fetching references for paper {paper_id} (attempt {attempt + 1}): {e}")
                wait_time = backoff_base ** attempt
                await asyncio.sleep(wait_time)
        
        if not success:
            if offset == 9000:  # Hit max citations limit
                logger.info(f'Paper {paper_id} has more than 10k references')
                # Save to tracking file
                tracking_file = DATA_PATH / "papers_with_more_than_10k_references.json"
                existing = []
                if tracking_file.exists():
                    with open(tracking_file, "r") as f:
                        existing = json.load(f)
                existing.append(paper_id)
                with open(tracking_file, "w") as f:
                    json.dump(existing, f)
                return references
            
            logger.error(f"Failed to fetch references for paper {paper_id} after {max_retries} attempts")
            return -1
        
        if not next_offset:
            break
            
        offset = next_offset
        if offset >= 9000:  # Semantic Scholar API limit
            break
    
    return references

async def save_references_to_jsonl(references):
    """Save references to JSONL files"""
    # Save to main references file
    with open(REFERENCES_FILE, 'a') as f:
        for reference in references:
            if reference:  # Skip empty references
                f.write(json.dumps(reference) + '\n')

async def prune_data():
    """Prune data based on quality filters (from build_graph/prune_data.py)"""
    try:
        logger.info("Pruning data based on quality filters")
        
        if not REFERENCES_FILE.exists():
            logger.warning("No references_complete.jsonl file found, skipping pruning")
            return
        
        # Load cited paper IDs from JSONL
        cited_paper_ids = {}
        with open(REFERENCES_FILE, 'r') as f:
            for line in f:
                citation = json.loads(line)
                if 'citingPaperId' in citation and 'citedPaper' in citation:
                    cited_paper = citation['citedPaper']
                    if 'paperId' in cited_paper:
                        paper_id = cited_paper['paperId']
                        citation_count = cited_paper.get('citationCount', 0)
                        year = cited_paper.get('year', 2024)
                        cited_paper_ids[paper_id] = (citation_count, year)
        
        logger.info(f'Loaded cited paper IDs from jsonl: {len(cited_paper_ids)}')
        
        # Read CSV and update with missing entries
        if ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE.exists():
            arxiv_paper_details = pd.read_csv(ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE)
            for _, row in arxiv_paper_details.iterrows():
                paper_id = row['paperId']
                citation_count = row['citationCount']
                year = row['year']
                if paper_id not in cited_paper_ids:
                    cited_paper_ids[paper_id] = (citation_count, year)
        
        total_unique_papers = len(cited_paper_ids)
        logger.info(f"Total unique papers with citations: {total_unique_papers}")
        
        # Create DataFrame from dictionary
        df = pd.DataFrame(
            [(pid, vals[0], vals[1]) for pid, vals in cited_paper_ids.items()],
            columns=['paperId', 'citationCount', 'year']
        )
        
        # Apply quality filters (from original script)
        current_year = datetime.now().year
        
        def keep_paper(row):
            age = current_year - row['year']
            if age == 0:
                return row['citationCount'] >= 0
            elif age == 1:
                return row['citationCount'] > 5
            elif age == 2:
                return row['citationCount'] > 25
            elif age == 3:
                return row['citationCount'] > 100
            elif age == 4:
                return row['citationCount'] > 250
            else:
                return row['citationCount'] > 500
        
        df = df[df.apply(keep_paper, axis=1)]
        logger.info(f'After quality filtering: {len(df)} papers')
        
        # Prune by year (keep papers from 2018 onwards)
        prune_year = 2018
        df = df[df['year'] >= prune_year]
        logger.info(f'After year pruning (>={prune_year}): {len(df)} papers')
        
        # Get shortlisted paper IDs
        shortlisted_ids = set(df['paperId'].astype(str))
        
        # Create pruned CSV for C++ code
        if ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE.exists():
            arxiv_df = pd.read_csv(ARXIV_PAPERS_WITH_SEMANTIC_IDS_FILE)
            arxiv_df = arxiv_df[arxiv_df['paperId'].astype(str).isin(shortlisted_ids)]
            
            df_for_c_code = arxiv_df[['paperId', 'url', 'title', 'year', 'citationCount', 'abstract']]
            df_for_c_code.to_csv(SEMANTIC_PAPER_DETAILS_PRUNED_FOR_C_FILE, index=False)
            logger.info(f"Saved pruned CSV for C++ code: {len(df_for_c_code)} papers")
        
        # Create pruned JSONL file
        pruned_jsonl = DATA_PATH / "references_complete_pruned.jsonl"
        input_line_count = 0
        output_line_count = 0
        
        with open(REFERENCES_FILE, 'r') as fin, open(pruned_jsonl, 'w') as fout:
            for line in fin:
                try:
                    input_line_count += 1
                    citation = json.loads(line)
                    citing_id = citation.get('citingPaperId')
                    cited_paper = citation.get('citedPaper', {})
                    cited_id = cited_paper.get('paperId')
                    
                    if citing_id in shortlisted_ids and cited_id in shortlisted_ids:
                        fout.write(line)
                        output_line_count += 1
                        
                except Exception as e:
                    continue  # Skip malformed lines
        
        logger.info(f"Filtered {input_line_count} lines to {output_line_count} lines in pruned JSONL")
        
    except Exception as e:
        error_msg = f"Error in prune_data: {str(e)}"
        logger.error(error_msg)
        error_tracker.log_error('processing_failures', 'prune_data', error_msg)
        raise

# Dummy consumers for other topics (to maintain Kafka architecture)
async def consume_enriched_papers():
    """Dummy consumer for semantic-enriched papers"""
    pass

async def consume_quality_filtered_papers():
    """Dummy consumer for quality-filtered papers"""
    pass

# Graph Builder functionality
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
    """Execute C++ graph builder with timeout handling (Airflow will handle Neo4j import)"""
    logger.info("🏗️ Starting C++ graph builder compilation and execution")
    
    try:
        update_status('running', 'Executing C++ graph builder', 'cpp_execution')
        
        # Use the pruned file for C++ processing
        input_csv = SEMANTIC_PAPER_DETAILS_PRUNED_FOR_C_FILE
        logger.info(f"📁 Input CSV file: {input_csv}")
        
        if not input_csv.exists():
            error_msg = f"❌ Input CSV file not found: {input_csv}"
            logger.error(error_msg)
            error_tracker.log_error('processing_failures', 'cpp_input', error_msg)
            return False, error_msg
        
        # Log input file stats
        import os
        file_size = os.path.getsize(input_csv) / (1024 * 1024)  # MB
        logger.info(f"📊 Input CSV size: {file_size:.2f} MB")
        
        # Copy main.cpp to data directory for execution
        cpp_source = BUILD_GRAPH_PATH / "main.cpp"
        cpp_target = DATA_PATH / "citation_network_new"
        
        logger.info(f"📋 Copying C++ source: {cpp_source} -> {cpp_target}")
        
        if cpp_source.exists():
            import shutil
            shutil.copy2(cpp_source, cpp_target)
            logger.info("✓ C++ source file copied successfully")
        else:
            error_msg = f"❌ C++ source file not found: {cpp_source}"
            logger.error(error_msg)
            error_tracker.log_error('processing_failures', 'cpp_source_missing', error_msg)
            return False, error_msg
        
        # Compile C++ code
        compile_cmd = [
            "g++", "-O3", "-std=c++17",
            str(cpp_target),
            "-lboost_system", "-lboost_graph", "-lsqlite3",
            "-I/usr/include/eigen3",
            "-o", str(DATA_PATH / "graph_builder")
        ]
        
        logger.info("🔨 Compiling C++ code...")
        logger.info(f"Compile command: {' '.join(compile_cmd)}")
        
        compile_start = time.time()
        compile_result = subprocess.run(
            compile_cmd, 
            cwd=str(DATA_PATH),
            capture_output=True, 
            text=True, 
            timeout=300  # 5 minutes for compilation
        )
        compile_time = time.time() - compile_start
        
        logger.info(f"⏱️ Compilation took {compile_time:.2f} seconds")
        
        if compile_result.returncode != 0:
            error_msg = f"❌ C++ compilation failed (exit code: {compile_result.returncode})"
            logger.error(error_msg)
            logger.error(f"Compilation stderr: {compile_result.stderr}")
            logger.error(f"Compilation stdout: {compile_result.stdout}")
            error_tracker.log_error('processing_failures', 'cpp_compilation', error_msg)
            return False, error_msg
        
        logger.info("✓ C++ compilation successful")
        
        # Execute compiled binary
        execute_cmd = [str(DATA_PATH / "graph_builder")]
        logger.info("🚀 Executing graph builder...")
        logger.info(f"Execute command: {' '.join(execute_cmd)}")
        
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
        
        logger.info(f"⏱️ C++ execution took {execution_time:.2f} seconds")
        perf_logger.info(f"CPP_EXECUTION_TIME: {execution_time:.2f}s")
        
        if execute_result.returncode != 0:
            error_msg = f"❌ C++ execution failed (exit code: {execute_result.returncode})"
            logger.error(error_msg)
            logger.error(f"Execution stderr: {execute_result.stderr}")
            logger.error(f"Execution stdout: {execute_result.stdout}")
            error_tracker.log_error('processing_failures', 'cpp_execution', error_msg)
            return False, error_msg
        
        logger.info("✓ C++ execution successful")
        logger.info(f"C++ stdout: {execute_result.stdout}")
        
        # Verify output files were created
        nodes_csv = DATA_PATH / "citation_nodes.csv"
        edges_csv = DATA_PATH / "citation_edges.csv"
        
        logger.info("🔍 Verifying output files...")
        logger.info(f"Checking for nodes file: {nodes_csv}")
        logger.info(f"Checking for edges file: {edges_csv}")
        
        if not nodes_csv.exists() or not edges_csv.exists():
            error_msg = f"❌ C++ execution completed but output CSV files not found. Nodes exists: {nodes_csv.exists()}, Edges exists: {edges_csv.exists()}"
            logger.error(error_msg)
            error_tracker.log_error('processing_failures', 'cpp_output', error_msg)
            return False, error_msg
        
        # Log output file stats
        nodes_size = os.path.getsize(nodes_csv) / (1024 * 1024)  # MB
        edges_size = os.path.getsize(edges_csv) / (1024 * 1024)  # MB
        logger.info(f"📊 Output file sizes - Nodes: {nodes_size:.2f} MB, Edges: {edges_size:.2f} MB")
        
        success_msg = f"🎉 Graph built successfully in {execution_time:.2f}s. Output files ready for Neo4j import."
        logger.info(success_msg)
        logger.info(f"  📁 Nodes CSV: {nodes_csv} ({nodes_size:.2f} MB)")
        logger.info(f"  📁 Edges CSV: {edges_csv} ({edges_size:.2f} MB)")
        logger.info("  ⏳ Airflow will handle Neo4j import in the next step")
        
        return True, success_msg
        
    except subprocess.TimeoutExpired:
        error_msg = f"❌ C++ execution timed out after {timeout_minutes} minutes"
        logger.error(error_msg)
        error_tracker.log_error('processing_failures', 'cpp_timeout', error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"❌ C++ execution error: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Full traceback: {traceback.format_exc()}")
        error_tracker.log_error('processing_failures', 'cpp_error', error_msg)
        return False, error_msg

# NOTE: Neo4j import is now handled by Airflow, not by this service directly
# The old build-graph endpoint is removed since Airflow handles orchestration

@app.get("/topics/status")
async def get_topics_status():
    """Get Kafka topics status"""
    try:
        return {
            "topics": [
                "new-papers",
                "semantic-enriched", 
                "quality-filtered",
                "graph-updates"
            ],
            "status": "active"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get topics status: {str(e)}")

@app.get("/data/files")
async def get_data_files():
    """Get list of data files available for processing"""
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
    uvicorn.run(app, host="0.0.0.0", port=8005)