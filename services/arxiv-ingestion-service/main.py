from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Set
import os
import json
import pandas as pd
import time
import zipfile
from datetime import datetime, timedelta
import logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

# Kaggle API imports
from kaggle.api.kaggle_api_extended import KaggleApi

# ML-related categories for filtering (from build_graph/get_arxiv_metadata_from_kaggle.py)
RELEVANT_CATEGORIES = {"cs.CV", "cs.AI", "cs.LG", "cs.CL", "cs.NE", "stat.ML", "cs.IR"}

app = FastAPI(title="ArXiv Ingestion Service", version="1.0.0")

# Metrics
KAGGLE_DOWNLOADS = Counter('arxiv_kaggle_downloads_total', 'Total Kaggle downloads')
PAPERS_PROCESSED = Counter('arxiv_papers_processed_total', 'Papers processed', ['status'])
DOWNLOAD_DURATION = Histogram('arxiv_download_duration_seconds', 'Download duration')
NEW_PAPERS_FOUND = Gauge('arxiv_new_papers_found', 'New papers discovered')
DATA_FRESHNESS = Gauge('arxiv_data_freshness_hours', 'Hours since last update')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
ingestion_status = {
    "status": "idle",  # idle, downloading, processing, completed, failed
    "last_update": None,
    "papers_count": 0,
    "new_papers": 0,
    "message": ""
}

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
    return {
        "status": "healthy",
        "service": "arxiv-ingestion-service",
        "ingestion_status": ingestion_status["status"]
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
    
    # Load existing processed data from shared volume
    processed_file = "/shared-data/processed_papers.json"
    arxiv_data_file = "/shared-data/arxiv_data.pkl"
    existing_papers = set()
    
    # First try lightweight processed_file for quick ID checks
    if os.path.exists(processed_file):
        try:
            with open(processed_file, 'r') as f:
                existing_data = json.load(f)
                existing_papers = set(existing_data.get('paper_ids', []))
                logger.info(f"Found {len(existing_papers)} existing papers in processed_papers.json")
        except Exception as e:
            logger.warning(f"Could not load processed_papers.json: {e}")
    
    # Fallback to arxiv_data.pkl if processed_file doesn't exist
    if not existing_papers and os.path.exists(arxiv_data_file):
        try:
            df = pd.read_pickle(arxiv_data_file)
            existing_papers = set(df['id'].astype(str))
            logger.info(f"Fallback: Found {len(existing_papers)} existing papers in arxiv_data.pkl")
        except Exception as e:
            logger.error(f"Error loading existing arxiv_data.pkl: {e}")
    
    if not existing_papers:
        logger.info("No existing papers found, processing all papers")
    
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
        
        # Update processed papers list for quick future lookups
        all_paper_ids = list(existing_papers) + [p['id'] for p in new_papers]
        processed_data = {
            'last_update': datetime.now().isoformat(),
            'total_papers': len(all_paper_ids),
            'new_papers_found': len(new_papers),
            'paper_ids': all_paper_ids
        }
        
        # Save updated processed_file for next incremental run
        with open(processed_file, 'w') as f:
            json.dump(processed_data, f)
        
        logger.info(f"Ingestion summary: {processed_data}")
        
        # Save new papers if any
        if new_papers:
            new_papers_file = f"/data/new_papers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(new_papers_file, 'w') as f:
                json.dump(new_papers, f, indent=2)
            logger.info(f"Saved {len(new_papers)} new papers to {new_papers_file}")
        
        NEW_PAPERS_FOUND.set(len(new_papers))
        
        return {
            'total_papers': total_papers,
            'new_papers': len(new_papers),
            'new_papers_file': new_papers_file if new_papers else None,
            'processed_file': processed_file
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
        
        ingestion_status.update({
            "status": "completed",
            "message": f"Ingestion completed successfully. Found {result['new_papers']} new papers.",
            "papers_count": result['total_papers'],
            "new_papers": result['new_papers'],
            "last_update": datetime.now().isoformat()
        })
        
        logger.info("Ingestion completed successfully")
        
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

@app.get("/data/new-papers")
async def get_new_papers():
    """Get ALL recently discovered new papers (no limit for full fidelity)"""
    try:
        # Find the most recent new papers file
        data_dir = "/data"
        new_papers_files = [f for f in os.listdir(data_dir) if f.startswith("new_papers_")]
        
        if not new_papers_files:
            return {"papers": [], "total_count": 0, "message": "No new papers found"}
        
        # Get the most recent file
        latest_file = max(new_papers_files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
        file_path = os.path.join(data_dir, latest_file)
        
        with open(file_path, 'r') as f:
            papers = json.load(f)
        
        # Return ALL papers for full fidelity
        logger.info(f"Returning {len(papers)} new papers from {latest_file}")
        return {
            "papers": papers,
            "total_count": len(papers),
            "file": latest_file
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving papers: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)