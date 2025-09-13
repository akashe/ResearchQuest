from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Set
import json
import os
import time
import logging
import random
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import redis
import asyncio
import requests
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="Data Pipeline Service", version="1.0.0")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/shared-data/data-pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

# Semantic Scholar API Configuration
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_FIELDS = "url,year,citationCount,tldr"
REFERENCES_FIELDS = "paperId,title,contexts,year,citationCount,abstract"

# Shared data paths
SHARED_DATA_PATH = "/shared-data"
REFERENCES_FILE = f"{SHARED_DATA_PATH}/references_complete.jsonl"
ARXIV_DATA_FILE = f"{SHARED_DATA_PATH}/arxiv_data.pkl"

# Metrics
KAFKA_MESSAGES_PRODUCED = Counter('pipeline_kafka_messages_produced_total', 'Total messages produced to Kafka', ['topic'])
KAFKA_MESSAGES_CONSUMED = Counter('pipeline_kafka_messages_consumed_total', 'Total messages consumed from Kafka', ['topic'])
PROCESSING_DURATION = Histogram('pipeline_processing_duration_seconds', 'Processing duration', ['operation'])
PIPELINE_ERRORS = Counter('pipeline_errors_total', 'Pipeline errors', ['stage', 'error_type'])
PAPERS_IN_QUEUE = Gauge('pipeline_papers_in_queue', 'Papers waiting in processing queue')

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

class PaperEvent(BaseModel):
    id: str
    title: str
    authors: List[str]
    categories: str
    abstract: str
    update_date: str
    event_type: str = "new_paper"  # new_paper, updated_paper
    timestamp: datetime = None

class SemanticEnrichment(BaseModel):
    paper_id: str
    semantic_scholar_id: Optional[str] = None
    references: List[str] = []
    citations: List[str] = []
    influence_score: Optional[float] = None

class QualityFilter(BaseModel):
    paper_id: str
    citation_count: int
    age_days: int
    quality_score: float
    passed_filter: bool
    reasons: List[str] = []

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
        "service": "data-pipeline-service",
        "kafka": "connected" if kafka_healthy else "disconnected",
        "redis": "connected" if redis_healthy else "disconnected",
        "pipeline_status": pipeline_status["status"]
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
async def get_pipeline_status():
    """Get current pipeline status"""
    return pipeline_status

@app.post("/events/new-papers")
async def publish_new_papers(papers: List[PaperEvent]):
    """Publish new papers to Kafka for processing"""
    try:
        start_time = time.time()
        
        for paper in papers:
            if not paper.timestamp:
                paper.timestamp = datetime.utcnow()
            
            # Publish to new-papers topic
            kafka_producer.send(
                'new-papers',
                key=paper.id,
                value=paper.dict()
            )
            
            KAFKA_MESSAGES_PRODUCED.labels(topic='new-papers').inc()
        
        kafka_producer.flush()  # Ensure messages are sent
        
        PROCESSING_DURATION.labels(operation='publish_new_papers').observe(time.time() - start_time)
        
        logger.info(f"Published {len(papers)} new papers to Kafka")
        return {"status": "success", "papers_published": len(papers)}
        
    except Exception as e:
        logger.error(f"Failed to publish papers: {e}")
        PIPELINE_ERRORS.labels(stage='publish', error_type='kafka_error').inc()
        raise HTTPException(status_code=500, detail=f"Failed to publish papers: {str(e)}")

async def consume_new_papers():
    """Background consumer for new papers topic"""
    try:
        consumer = KafkaConsumer(
            'new-papers',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='semantic-enrichment-service',
            auto_offset_reset='latest'
        )
        
        logger.info("Started consuming from new-papers topic")
        
        for message in consumer:
            try:
                paper_data = message.value
                KAFKA_MESSAGES_CONSUMED.labels(topic='new-papers').inc()
                
                # Process paper for semantic enrichment
                enrichment = await enrich_paper_semantics(paper_data)
                
                # Publish to semantic-enriched topic
                kafka_producer.send(
                    'semantic-enriched',
                    key=paper_data['id'],
                    value=enrichment.dict()
                )
                
                KAFKA_MESSAGES_PRODUCED.labels(topic='semantic-enriched').inc()
                
                # Cache enrichment data
                redis_client.setex(
                    f"semantic:{paper_data['id']}",
                    86400,  # 24 hours TTL
                    json.dumps(enrichment.dict())
                )
                
            except Exception as e:
                logger.error(f"Error processing paper {message.key}: {e}")
                PIPELINE_ERRORS.labels(stage='semantic_enrichment', error_type='processing_error').inc()
                
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        PIPELINE_ERRORS.labels(stage='consume', error_type='kafka_error').inc()

async def enrich_paper_semantics(paper_data: Dict) -> SemanticEnrichment:
    """Enrich paper with Semantic Scholar data using real API calls"""
    paper_id = paper_data['id']
    arxiv_id = f"ARXIV:{paper_id}"
    
    try:
        # Step 1: Get Semantic Scholar paper ID (from get_semantic_paper_ids_for_arxiv_papers.py logic)
        semantic_scholar_id = await get_semantic_scholar_id(arxiv_id)
        
        if not semantic_scholar_id:
            logger.warning(f"No Semantic Scholar ID found for arXiv paper: {paper_id}")
            PIPELINE_ERRORS.labels(stage='semantic_enrichment', error_type='missing_semantic_id').inc()
            return SemanticEnrichment(paper_id=paper_id)
        
        # Step 2: Check if we already processed references for this paper
        processed_semantic_ids = await load_processed_semantic_ids()
        
        references = []
        if semantic_scholar_id not in processed_semantic_ids:
            # Step 3: Fetch references (from get_citation_details.py logic)
            references = await fetch_paper_references(semantic_scholar_id)
            
            # Step 4: Append new references to references_complete.jsonl
            if references:
                await append_references_to_file(references, paper_id)
        else:
            logger.info(f"References already processed for semantic ID: {semantic_scholar_id}")
        
        return SemanticEnrichment(
            paper_id=paper_id,
            semantic_scholar_id=semantic_scholar_id,
            references=references,
            influence_score=len(references) / 100.0  # Simple influence score based on reference count
        )
        
    except Exception as e:
        logger.error(f"Error enriching paper {paper_id}: {e}")
        PIPELINE_ERRORS.labels(stage='semantic_enrichment', error_type='api_error').inc()
        return SemanticEnrichment(paper_id=paper_id)

async def get_semantic_scholar_id(arxiv_id: str, max_retries: int = 5) -> Optional[str]:
    """Get Semantic Scholar paper ID for arXiv paper with exponential backoff"""
    url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/batch/"
    params = {'fields': SEMANTIC_SCHOLAR_FIELDS}
    json_data = {"ids": [arxiv_id]}
    
    backoff_base = 2
    
    for attempt in range(max_retries):
        try:
            # Add jitter to prevent thundering herd
            await asyncio.sleep(random.uniform(0.1, 0.5))
            
            response = requests.post(url, params=params, json=json_data, timeout=30)
            
            if response.status_code == 200:
                results = response.json()
                if results and len(results) > 0 and results[0] is not None:
                    paper_info = results[0]
                    return paper_info.get('paperId')
            
            elif response.status_code == 429:  # Rate limited
                wait_time = backoff_base ** attempt + random.uniform(0, 1)
                logger.warning(f"Rate limited, waiting {wait_time:.2f}s (attempt {attempt + 1})")
                await asyncio.sleep(wait_time)
                continue
            
            else:
                logger.warning(f"Unexpected status code {response.status_code} for {arxiv_id}")
                
        except requests.exceptions.Timeout:
            wait_time = backoff_base ** attempt
            logger.warning(f"Timeout for {arxiv_id}, retrying in {wait_time}s (attempt {attempt + 1})")
            await asyncio.sleep(wait_time)
            continue
            
        except Exception as e:
            logger.error(f"Error fetching Semantic Scholar ID for {arxiv_id}: {e}")
            await asyncio.sleep(backoff_base ** attempt)
            continue
    
    return None

async def fetch_paper_references(semantic_scholar_id: str, max_retries: int = 8) -> List[str]:
    """Fetch paper references from Semantic Scholar API with pagination"""
    references = []
    offset = 0
    limit = 1000
    
    while True:
        url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/{semantic_scholar_id}/references"
        params = {
            'offset': offset,
            'limit': limit,
            'fields': REFERENCES_FIELDS
        }
        
        success = False
        backoff_base = 2
        
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(random.uniform(1, 2))  # Rate limiting
                
                response = requests.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        batch_references = data['data']
                        references.extend(batch_references)
                        
                        next_offset = data.get('next')
                        success = True
                        break
                        
                elif response.status_code == 429:
                    wait_time = backoff_base ** attempt + random.uniform(0, 2)
                    logger.warning(f"Rate limited fetching references for {semantic_scholar_id}, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    continue
                    
                else:
                    logger.warning(f"Error fetching references: {response.status_code}")
                    break
                    
            except Exception as e:
                wait_time = backoff_base ** attempt
                logger.error(f"Error fetching references for {semantic_scholar_id} (attempt {attempt + 1}): {e}")
                await asyncio.sleep(wait_time)
        
        if not success:
            logger.error(f"Failed to fetch references for {semantic_scholar_id} after {max_retries} attempts")
            break
            
        if not next_offset:
            break
            
        offset = next_offset
        if offset >= 9000:  # Semantic Scholar API limit
            break
    
    logger.info(f"Fetched {len(references)} references for {semantic_scholar_id}")
    return references

async def load_processed_semantic_ids() -> Set[str]:
    """Load already processed semantic scholar IDs from references file"""
    processed_ids = set()
    
    if os.path.exists(REFERENCES_FILE):
        try:
            with open(REFERENCES_FILE, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        citation = json.loads(line)
                        if 'citingPaperId' in citation:
                            processed_ids.add(citation['citingPaperId'])
                    except json.JSONDecodeError:
                        if line_num % 10000 == 0:
                            logger.warning(f"Invalid JSON at line {line_num} in references file")
                        continue
        except Exception as e:
            logger.error(f"Error loading processed semantic IDs: {e}")
    
    logger.info(f"Loaded {len(processed_ids)} processed semantic scholar IDs")
    return processed_ids

async def append_references_to_file(references: List[str], citing_paper_id: str):
    """Append new references to the references_complete.jsonl file"""
    try:
        with open(REFERENCES_FILE, 'a') as f:
            for reference in references:
                if reference:  # Skip empty references
                    reference['citingPaperId'] = citing_paper_id
                    f.write(json.dumps(reference) + '\n')
        
        logger.info(f"Appended {len(references)} references for paper {citing_paper_id}")
        
    except Exception as e:
        logger.error(f"Error appending references to file: {e}")
        PIPELINE_ERRORS.labels(stage='file_write', error_type='append_error').inc()

async def consume_enriched_papers():
    """Background consumer for semantic-enriched papers"""
    try:
        consumer = KafkaConsumer(
            'semantic-enriched',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='quality-filter-service',
            auto_offset_reset='latest'
        )
        
        logger.info("Started consuming from semantic-enriched topic")
        
        for message in consumer:
            try:
                enrichment_data = message.value
                KAFKA_MESSAGES_CONSUMED.labels(topic='semantic-enriched').inc()
                
                # Apply quality filters
                quality_result = await apply_quality_filters(enrichment_data)
                
                # Publish to quality-filtered topic
                kafka_producer.send(
                    'quality-filtered',
                    key=enrichment_data['paper_id'],
                    value=quality_result.dict()
                )
                
                KAFKA_MESSAGES_PRODUCED.labels(topic='quality-filtered').inc()
                
            except Exception as e:
                logger.error(f"Error in quality filtering: {e}")
                PIPELINE_ERRORS.labels(stage='quality_filter', error_type='processing_error').inc()
                
    except Exception as e:
        logger.error(f"Quality filter consumer error: {e}")

async def apply_quality_filters(enrichment_data: Dict) -> QualityFilter:
    """Apply quality heuristics to papers"""
    await asyncio.sleep(0.05)  # Simulate processing
    
    citation_count = len(enrichment_data.get('citations', []))
    age_days = 30  # Mock age
    
    # Simple quality scoring
    quality_score = min(1.0, (citation_count * 0.1) + (enrichment_data.get('influence_score', 0) * 0.5))
    passed_filter = quality_score > 0.3
    
    reasons = []
    if citation_count < 5:
        reasons.append("Low citation count")
    if quality_score < 0.3:
        reasons.append("Below quality threshold")
    
    return QualityFilter(
        paper_id=enrichment_data['paper_id'],
        citation_count=citation_count,
        age_days=age_days,
        quality_score=quality_score,
        passed_filter=passed_filter,
        reasons=reasons
    )

async def consume_quality_filtered_papers():
    """Background consumer for quality-filtered papers"""
    try:
        consumer = KafkaConsumer(
            'quality-filtered',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='graph-update-service',
            auto_offset_reset='latest'
        )
        
        logger.info("Started consuming from quality-filtered topic")
        
        for message in consumer:
            try:
                quality_data = message.value
                KAFKA_MESSAGES_CONSUMED.labels(topic='quality-filtered').inc()
                
                if quality_data['passed_filter']:
                    # Publish to graph-updates topic for Neo4j updates
                    kafka_producer.send(
                        'graph-updates',
                        key=quality_data['paper_id'],
                        value={
                            'paper_id': quality_data['paper_id'],
                            'operation': 'add_paper',
                            'quality_score': quality_data['quality_score'],
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    )
                    
                    KAFKA_MESSAGES_PRODUCED.labels(topic='graph-updates').inc()
                    pipeline_status["papers_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error in graph update processing: {e}")
                PIPELINE_ERRORS.labels(stage='graph_update', error_type='processing_error').inc()
                
    except Exception as e:
        logger.error(f"Graph update consumer error: {e}")

@app.get("/topics/status")
async def get_topics_status():
    """Get Kafka topics status"""
    try:
        # This would normally use Kafka admin client
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)