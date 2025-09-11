from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import redis
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="Data Pipeline Service", version="1.0.0")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    """Simulate semantic scholar enrichment (replace with actual API calls)"""
    # Simulate processing time
    await asyncio.sleep(0.1)
    
    # Mock semantic scholar data
    return SemanticEnrichment(
        paper_id=paper_data['id'],
        semantic_scholar_id=f"ss_{paper_data['id']}",
        references=[f"ref_{i}" for i in range(5)],  # Mock references
        citations=[f"cite_{i}" for i in range(3)],   # Mock citations
        influence_score=0.75
    )

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