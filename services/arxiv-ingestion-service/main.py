from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Set
import os
import json
import pandas as pd
import pickle
import time
import zipfile
from datetime import datetime, timedelta
import logging
import threading
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from kafka import KafkaProducer
import asyncio

# Kaggle API imports
from kaggle.api.kaggle_api_extended import KaggleApi

# ML-related categories for filtering (from build_graph/get_arxiv_metadata_from_kaggle.py)
RELEVANT_CATEGORIES = {"cs.CV", "cs.AI", "cs.LG", "cs.CL", "cs.NE", "stat.ML", "cs.IR"}

app = FastAPI(title="ArXiv Ingestion Service", version="1.0.0")

def init_kafka_producer():
    """Initialize Kafka producer with retry logic"""
    global kafka_producer
    max_retries = 3
    for attempt in range(max_retries):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=5,
                retry_backoff_ms=1000
            )
            logger.info(f"✅ Kafka producer initialized successfully")
            return
        except Exception as e:
            logger.warning(f"❌ Kafka producer init attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("🚨 Failed to initialize Kafka producer after all retries")
                kafka_producer = None
            else:
                time.sleep(2 ** attempt)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    init_kafka_producer()
    # Migrate existing data if needed
    migrate_legacy_files()

def migrate_legacy_files():
    """Migrate from old file structure to new clean structure"""
    legacy_pkl = "/data/arxiv_data.pkl"
    if os.path.exists(legacy_pkl) and not os.path.exists(MASTER_PAPERS_FILE):
        logger.info("🔄 Migrating legacy arxiv_data.pkl to new structure")
        try:
            df = pd.read_pickle(legacy_pkl)
            with file_lock:
                df.to_pickle(MASTER_PAPERS_FILE)
            logger.info(f"✅ Migrated {len(df)} papers to new master file")
            ingestion_status["papers_count"] = len(df)
        except Exception as e:
            logger.error(f"❌ Failed to migrate legacy file: {e}")

def load_master_papers() -> pd.DataFrame:
    """Load master papers dataset with concurrent read support"""
    with file_lock:
        if os.path.exists(MASTER_PAPERS_FILE):
            return pd.read_pickle(MASTER_PAPERS_FILE)
        else:
            return pd.DataFrame()

def save_master_papers(df: pd.DataFrame):
    """Save master papers dataset with write lock"""
    with file_lock:
        df.to_pickle(MASTER_PAPERS_FILE)
        logger.info(f"💾 Saved {len(df)} papers to master file")

def save_new_papers(df: pd.DataFrame):
    """Save new papers from latest ingestion"""
    with file_lock:
        df.to_pickle(NEW_PAPERS_FILE)
        logger.info(f"📄 Saved {len(df)} new papers to new papers file")

def load_new_papers() -> pd.DataFrame:
    """Load new papers from latest ingestion"""
    with file_lock:
        if os.path.exists(NEW_PAPERS_FILE):
            return pd.read_pickle(NEW_PAPERS_FILE)
        else:
            return pd.DataFrame()

async def publish_papers_to_kafka(papers: List[Dict]):
    """Publish papers to Kafka for downstream processing"""
    if not kafka_producer:
        logger.warning("⚠️ Kafka producer not available, skipping paper publishing")
        return
    
    try:
        for paper in papers:
            # Create Kafka message
            message = {
                'id': paper['id'],
                'title': paper.get('title', ''),
                'authors': paper.get('authors', ''),
                'categories': paper.get('categories', ''),
                'abstract': paper.get('abstract', ''),
                'update_date': paper.get('update_date', ''),
                'event_type': 'new_paper',
                'timestamp': datetime.now().isoformat()
            }
            
            # Send to Kafka
            future = kafka_producer.send(
                KAFKA_TOPIC, 
                key=paper['id'],
                value=message
            )
            
        # Ensure all messages are sent
        kafka_producer.flush()
        logger.info(f"📤 Published {len(papers)} papers to Kafka topic '{KAFKA_TOPIC}'")
        
    except Exception as e:
        logger.error(f"❌ Failed to publish papers to Kafka: {e}")

# Metrics
KAGGLE_DOWNLOADS = Counter('arxiv_kaggle_downloads_total', 'Total Kaggle downloads')
PAPERS_PROCESSED = Counter('arxiv_papers_processed_total', 'Papers processed', ['status'])
DOWNLOAD_DURATION = Histogram('arxiv_download_duration_seconds', 'Download duration')
NEW_PAPERS_FOUND = Gauge('arxiv_new_papers_found', 'New papers discovered')
DATA_FRESHNESS = Gauge('arxiv_data_freshness_hours', 'Hours since last update')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# File management - Only 2 files for clean architecture
MASTER_PAPERS_FILE = "/data/arxiv_papers_master.pkl"  # All papers (master dataset)
NEW_PAPERS_FILE = "/data/arxiv_papers_new.pkl"      # Papers added in latest ingestion

# File locks for concurrent access
file_lock = threading.Lock()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'arxiv-papers'

# Global state
ingestion_status = {
    "status": "idle",  # idle, downloading, processing, completed, failed
    "last_update": None,
    "papers_count": 0,
    "new_papers": 0,
    "message": ""
}

# Kafka producer (initialized on startup)
kafka_producer = None

class IngestionRequest(BaseModel):
    force_download: bool = False
    filter_years: int = 5  # Only papers from last N years

class IngestionResponse(BaseModel):
    status: str
    message: str
    papers_found: int
    new_papers: int
    download_time: Optional[float] = None

@app.get("/health")
async def health_check():
    kafka_status = "connected" if kafka_producer else "disconnected"
    return {
        "status": "healthy",
        "service": "arxiv-ingestion-service",
        "ingestion_status": ingestion_status["status"],
        "kafka_status": kafka_status,
        "master_papers_count": len(load_master_papers()),
        "new_papers_count": len(load_new_papers())
    }

@app.get("/metrics")
async def metrics():
    # Update data freshness metric
    if ingestion_status["last_update"]:
        last_update = datetime.fromisoformat(ingestion_status["last_update"])
        hours_since_update = (datetime.utcnow() - last_update).total_seconds() / 3600
        DATA_FRESHNESS.set(hours_since_update)
    
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
async def get_ingestion_status():
    """Get current ingestion status"""
    return ingestion_status

def setup_kaggle_api():
    """Setup Kaggle API with authentication"""
    try:
        api = KaggleApi()
        api.authenticate()
        logger.info("Kaggle API authenticated successfully")
        return api
    except Exception as e:
        logger.error(f"Failed to authenticate Kaggle API: {e}")
        raise HTTPException(status_code=500, detail=f"Kaggle authentication failed: {str(e)}")

def download_arxiv_metadata(api: KaggleApi, force_download: bool = False) -> str:
    """Download arXiv metadata from Kaggle"""
    dataset_path = "/data/kaggle-arxiv"
    metadata_file = f"{dataset_path}/arxiv-metadata-oai-snapshot.json"
    
    # Create data directory
    os.makedirs(dataset_path, exist_ok=True)
    
    # Check if we need to download
    if not force_download and os.path.exists(metadata_file):
        # Check file age
        file_age = time.time() - os.path.getmtime(metadata_file)
        if file_age < 24 * 3600:  # Less than 24 hours old
            logger.info("Using existing metadata file (less than 24h old)")
            return metadata_file
    
    try:
        logger.info("Downloading arXiv metadata from Kaggle...")
        start_time = time.time()
        
        # Download the dataset (compressed)
        api.dataset_download_files(
            'Cornell-University/arxiv',
            path=dataset_path,
            unzip=False,  # We'll handle unzipping manually
            quiet=False
        )
        
        # Manually extract the ZIP file (with progress to keep health checks alive)
        zip_file = f"{dataset_path}/arxiv.zip"
        if os.path.exists(zip_file):
            logger.info("Extracting arxiv.zip...")
            
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                members = zip_ref.infolist()
                total_files = len(members)
                
                for i, member in enumerate(members):
                    zip_ref.extract(member, dataset_path)
                    # Yield control every 10000 files to keep health checks responsive
                    if i % 10000 == 0:
                        logger.info(f"Extracted {i}/{total_files} files ({i/total_files*100:.1f}%)")
                        time.sleep(0.1)  # Brief pause to allow health checks
            
            # Remove the zip file to save space
            os.remove(zip_file)
            logger.info("Extraction completed and zip file removed")
        
        download_time = time.time() - start_time
        DOWNLOAD_DURATION.observe(download_time)
        KAGGLE_DOWNLOADS.inc()
        
        logger.info(f"Download completed in {download_time:.2f} seconds")
        return metadata_file
        
    except Exception as e:
        logger.error(f"Failed to download from Kaggle: {e}")
        raise

def process_arxiv_metadata(metadata_file: str, filter_years: int = 5) -> Dict[str, Any]:
    """Process arXiv metadata and extract new papers"""
    logger.info(f"Processing metadata file: {metadata_file}")
    
    # Load existing papers from master file (clean architecture)
    existing_df = load_master_papers()
    existing_papers = set(existing_df['id'].astype(str)) if not existing_df.empty else set()
    
    logger.info(f"📊 Found {len(existing_papers)} existing papers in master file")
    
    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=filter_years * 365)
    
    new_papers = []
    total_papers = 0
    
    try:
        logger.info("Parsing arXiv metadata...")
        with open(metadata_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line_num % 100000 == 0:
                    logger.info(f"Processed {line_num} lines...")
                
                try:
                    paper = json.loads(line.strip())
                    total_papers += 1
                    
                    # Check if paper is within time range
                    update_date = datetime.strptime(paper['update_date'], '%Y-%m-%d')
                    if update_date < cutoff_date:
                        continue
                    
                    # Filter by ML-related categories (from build_graph logic)
                    categories = paper.get('categories', '')
                    if not any(category in RELEVANT_CATEGORIES for category in categories.split()):
                        continue
                    
                    paper_id = paper['id']
                    
                    # Check if this is a new paper (avoid reprocessing)
                    if paper_id not in existing_papers:
                        new_papers.append({
                            'id': paper_id,
                            'title': paper.get('title', ''),
                            'authors': paper.get('authors_parsed', []),
                            'categories': paper.get('categories', ''),
                            'abstract': paper.get('abstract', ''),
                            'update_date': paper['update_date'],
                            'journal_ref': paper.get('journal-ref'),
                            'doi': paper.get('doi')
                        })
                        
                        PAPERS_PROCESSED.labels(status='new').inc()
                    else:
                        PAPERS_PROCESSED.labels(status='existing').inc()
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"Error processing line {line_num}: {e}")
                    continue
        
        logger.info(f"Processing complete. Total papers: {total_papers}, New papers: {len(new_papers)}")
        
        # Save results using clean file architecture (only 2 files!)
        all_papers = list(existing_df.to_dict('records')) if not existing_df.empty else []
        all_papers.extend(new_papers)
        
        # Update master file with all papers
        master_df = pd.DataFrame(all_papers)
        save_master_papers(master_df)
        
        # Save only new papers to separate file
        if new_papers:
            new_papers_df = pd.DataFrame(new_papers)
            save_new_papers(new_papers_df)
            logger.info(f"📝 Saved {len(new_papers)} new papers to new papers file")
        
        NEW_PAPERS_FOUND.set(len(new_papers))
        
        return {
            'total_papers': total_papers,
            'new_papers': len(new_papers),
            'master_papers_count': len(all_papers)
        }
        
    except Exception as e:
        logger.error(f"Error processing metadata: {e}")
        raise

async def run_ingestion(force_download: bool = False, filter_years: int = 5):
    """Background task to run the ingestion process"""
    global ingestion_status
    
    try:
        ingestion_status.update({
            "status": "downloading",
            "message": "Setting up Kaggle API...",
            "papers_count": 0,
            "new_papers": 0
        })
        
        # Setup Kaggle API
        api = setup_kaggle_api()
        
        ingestion_status["message"] = "Downloading metadata from Kaggle..."
        metadata_file = download_arxiv_metadata(api, force_download)
        
        ingestion_status.update({
            "status": "processing", 
            "message": "Processing arXiv metadata..."
        })
        
        # Process metadata
        result = process_arxiv_metadata(metadata_file, filter_years)
        
        # Publish new papers to Kafka for downstream processing
        if result['new_papers'] > 0:
            ingestion_status["message"] = "Publishing new papers to Kafka..."
            new_papers_df = load_new_papers()
            new_papers_list = new_papers_df.to_dict('records')
            await publish_papers_to_kafka(new_papers_list)
        
        ingestion_status.update({
            "status": "completed",
            "message": f"Ingestion and Kafka publishing completed successfully. Found {result['new_papers']} new papers.",
            "papers_count": result['total_papers'],
            "new_papers": result['new_papers'],
            "last_update": datetime.now().isoformat()
        })
        
        logger.info("🎉 Ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        ingestion_status.update({
            "status": "failed",
            "message": f"Ingestion failed: {str(e)}",
            "last_update": datetime.now().isoformat()
        })

@app.post("/ingest", response_model=IngestionResponse)
async def start_ingestion(
    request: IngestionRequest,
    background_tasks: BackgroundTasks
):
    """Start arXiv data ingestion process"""
    global ingestion_status
    
    if ingestion_status["status"] in ["downloading", "processing"]:
        raise HTTPException(
            status_code=409, 
            detail="Ingestion already in progress"
        )
    
    # Start background ingestion
    background_tasks.add_task(
        run_ingestion, 
        request.force_download, 
        request.filter_years
    )
    
    return IngestionResponse(
        status="started",
        message="Ingestion process started in background",
        papers_found=0,
        new_papers=0
    )


# Clean API endpoints - only what's needed for the new architecture

@app.get("/papers")
async def get_all_papers(limit: int = 1000, offset: int = 0):
    """Get all papers from master dataset"""
    try:
        master_df = load_master_papers()
        
        if master_df.empty:
            return {
                "papers": [],
                "total_count": 0,
                "batch_size": 0,
                "offset": offset,
                "has_more": False,
                "message": "No papers available yet"
            }
        
        # Apply pagination
        start_idx = offset
        end_idx = min(offset + limit, len(master_df))
        batch_df = master_df.iloc[start_idx:end_idx]
        papers = batch_df.to_dict('records')
        
        return {
            "papers": papers,
            "total_count": len(master_df),
            "batch_size": len(papers),
            "offset": offset,
            "has_more": end_idx < len(master_df)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving papers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve papers: {str(e)}")

@app.get("/papers/new") 
async def get_new_papers_from_latest_ingestion():
    """Get papers from latest ingestion (for debugging/monitoring)"""
    try:
        new_df = load_new_papers()
        
        if new_df.empty:
            return {
                "papers": [],
                "count": 0,
                "message": "No new papers from latest ingestion"
            }
        
        papers = new_df.to_dict('records')
        
        return {
            "papers": papers,
            "count": len(papers),
            "message": f"Found {len(papers)} new papers from latest ingestion"
        }
        
    except Exception as e:
        logger.error(f"Error retrieving new papers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve new papers: {str(e)}")

@app.get("/data/files")
async def get_data_files():
    """Get list of data files in the individual PVC"""
    try:
        data_dir = "/data"
        files_info = []
        
        if not os.path.exists(data_dir):
            return {"data_directory": data_dir, "files": [], "total_files": 0, "message": "Data directory not found"}
        
        for filename in os.listdir(data_dir):
            file_path = os.path.join(data_dir, filename)
            if os.path.isfile(file_path):
                stat = os.stat(file_path)
                files_info.append({
                    "name": filename,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
        
        return {
            "data_directory": data_dir,
            "files": files_info,
            "total_files": len(files_info)
        }
        
    except Exception as e:
        logger.error(f"Error listing data files: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list data files: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)