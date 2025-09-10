from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import sys
import os
import traceback
import time

# Add the parent directory to sys.path to import original modules
# sys.path.append('/Users/akashkumar/projects/ResearchQuest')
from genai import (
    summarize_topic_evolution,
    summarize_state_of_art,
    combine_summaries,
    ask_custom_question,
    combine_answers
)
import pandas as pd
from custom_logging import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Generation Service", version="1.0.0")

# Metrics
LLM_REQUESTS = Counter('generation_service_llm_requests_total', 'Total LLM requests', ['operation'])
LLM_TOKENS = Counter('generation_service_llm_tokens_total', 'Total LLM tokens', ['type'])
LLM_COST = Counter('generation_service_llm_cost_usd_total', 'Total LLM cost in USD')
REQUEST_DURATION = Histogram('generation_service_request_duration_seconds', 'Request duration')

class TopicEvolutionRequest(BaseModel):
    papers: List[Dict[str, Any]]
    topic_name: str

class StateOfArtRequest(BaseModel):
    papers: List[Dict[str, Any]]
    topic_name: str
    year_cutoff: int
    num_chunks: int = 4
    chunk_size: int = 250

class CustomQuestionRequest(BaseModel):
    question: str
    papers: List[Dict[str, Any]]
    topic_name: str
    year_cutoff: int
    num_chunks: int = 4
    chunk_size: int = 250

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "generation-service"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/analyze/topic-evolution")
async def analyze_topic_evolution(request: TopicEvolutionRequest):
    """Generate topic evolution summary from papers"""
    try:
        df = pd.DataFrame(request.papers)
        if df.empty:
            raise HTTPException(status_code=400, detail="No papers provided")
        
        summary = summarize_topic_evolution(df, request.topic_name)
        return {
            "summary": summary,
            "topic_name": request.topic_name,
            "papers_count": len(request.papers)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate evolution summary: {str(e)}")

@app.post("/analyze/state-of-art")
async def analyze_state_of_art(request: StateOfArtRequest):
    """Generate state of the art analysis"""
    try:
        logger.info(f"Starting state-of-art analysis for topic: {request.topic_name}")
        logger.info(f"Received {len(request.papers)} papers for analysis")
        logger.info(f"Year cutoff: {request.year_cutoff}")
        
        df = pd.DataFrame(request.papers)
        if df.empty:
            logger.error("No papers provided in request")
            raise HTTPException(status_code=400, detail="No papers provided")
        
        logger.info(f"DataFrame created with shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        
        # Process in chunks to avoid token limits
        chunk_size = request.chunk_size
        results = []
        
        logger.info(f"Processing {len(df)} papers in chunks of {chunk_size}")
        
        for i in range(0, request.num_chunks):
            chunk_start = i* chunk_size
            chunk_end = min((i + 1)*chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]
            
            logger.info(f"Processing chunk {i + 1}:")
            
            if not chunk_df.empty:
                try:
                    logger.info(f"Calling summarize_state_of_art with {len(chunk_df)} papers")
                    result = summarize_state_of_art(chunk_df, request.year_cutoff, request.topic_name)
                    results.append(result)
                    logger.info(f"Chunk {i + 1} processed successfully")
                    time.sleep(1)  # To avoid rate limits for free tier
                except Exception as chunk_error:
                    logger.error(f"Error processing chunk {i + 1}: {str(chunk_error)}")
                    logger.error(f"Chunk error traceback: {traceback.format_exc()}")
                    raise chunk_error
        
        logger.info(f"Processed {len(results)} chunks successfully")
        
        # Combine results if multiple chunks
        if len(results) > 1:
            logger.info("Combining multiple chunk results")
            final_summary = combine_summaries(results, request.topic_name, request.year_cutoff)
        else:
            logger.info("Using single chunk result")
            final_summary = results[0] if results else "No analysis generated"
        
        logger.info("State-of-art analysis completed successfully")
        
        return {
            "analysis": final_summary,
            "topic_name": request.topic_name,
            "year_cutoff": request.year_cutoff,
            "papers_processed": len(request.papers),
            "chunks_processed": len(results)
        }
    except Exception as e:
        logger.error(f"Failed to generate state-of-art analysis: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to generate analysis: {str(e)}")

@app.post("/question/answer")
async def answer_custom_question(request: CustomQuestionRequest):
    """Answer custom question based on papers"""
    try:
        logger.info(f"Starting custom question analysis for topic: {request.topic_name}")
        logger.info(f"Question: {request.question}")
        logger.info(f"Received {len(request.papers)} papers for analysis")
        logger.info(f"Year cutoff: {request.year_cutoff}, Num chunks: {request.num_chunks}")
        
        df = pd.DataFrame(request.papers)
        if df.empty:
            logger.error("No papers provided in request")
            raise HTTPException(status_code=400, detail="No papers provided")
        
        logger.info(f"DataFrame created with shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        
        # Process in chunks
        chunk_size = request.chunk_size
        results = []
        
        logger.info(f"Processing {len(df)} papers in {request.num_chunks} chunks of {chunk_size}")
        
        for i in range(0, request.num_chunks):
            chunk_start = i* chunk_size
            chunk_end = min((i + 1)*chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]
            
            logger.info(f"Processing chunk {i+1}")
            
            if not chunk_df.empty:
                try:
                    logger.info(f"Calling ask_custom_question with {len(chunk_df)} papers")
                    result = ask_custom_question(
                        request.question, 
                        chunk_df, 
                        request.year_cutoff, 
                        request.topic_name
                    )
                    results.append(result)
                    logger.info(f"Chunk {i+1} processed successfully")
                    time.sleep(1)  # To avoid rate limits for free tier
                except Exception as chunk_error:
                    logger.error(f"Error processing chunk {i + 1}: {str(chunk_error)}")
                    logger.error(f"Chunk error traceback: {traceback.format_exc()}")
                    raise chunk_error
        
        logger.info(f"Processed {len(results)} chunks successfully")
        
        # Combine answers if multiple chunks
        if len(results) > 1:
            logger.info("Combining multiple chunk answers")
            final_answer = combine_answers(
                results, 
                request.question, 
                request.topic_name, 
                request.year_cutoff
            )
        else:
            logger.info("Using single chunk answer")
            final_answer = results[0] if results else "No answer generated"
        
        logger.info("Custom question analysis completed successfully")
        
        return {
            "answer": final_answer,
            "question": request.question,
            "topic_name": request.topic_name,
            "year_cutoff": request.year_cutoff,
            "papers_processed": len(request.papers),
            "chunks_processed": len(results)
        }
    except Exception as e:
        logger.error(f"Failed to answer custom question: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to answer question: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)