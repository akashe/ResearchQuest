from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import sys
import os

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

app = FastAPI(title="Generation Service", version="1.0.0")

class TopicEvolutionRequest(BaseModel):
    papers: List[Dict[str, Any]]
    topic_name: str

class StateOfArtRequest(BaseModel):
    papers: List[Dict[str, Any]]
    topic_name: str
    year_cutoff: int

class CustomQuestionRequest(BaseModel):
    question: str
    papers: List[Dict[str, Any]]
    topic_name: str
    year_cutoff: int
    num_chunks: int = 4

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "generation-service"}

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
        df = pd.DataFrame(request.papers)
        if df.empty:
            raise HTTPException(status_code=400, detail="No papers provided")
        
        # Process in chunks to avoid token limits
        chunk_size = 500
        results = []
        
        for i in range(0, len(df), chunk_size):
            chunk_df = df.iloc[i:i+chunk_size]
            if not chunk_df.empty:
                result = summarize_state_of_art(chunk_df, request.year_cutoff, request.topic_name)
                results.append(result)
        
        # Combine results if multiple chunks
        if len(results) > 1:
            final_summary = combine_summaries(results, request.topic_name, request.year_cutoff)
        else:
            final_summary = results[0] if results else "No analysis generated"
        
        return {
            "analysis": final_summary,
            "topic_name": request.topic_name,
            "year_cutoff": request.year_cutoff,
            "papers_processed": len(request.papers),
            "chunks_processed": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate analysis: {str(e)}")

@app.post("/question/answer")
async def answer_custom_question(request: CustomQuestionRequest):
    """Answer custom question based on papers"""
    try:
        df = pd.DataFrame(request.papers)
        if df.empty:
            raise HTTPException(status_code=400, detail="No papers provided")
        
        # Process in chunks
        chunk_size = 500
        results = []
        
        for i in range(0, min(request.num_chunks * chunk_size, len(df)), chunk_size):
            chunk_df = df.iloc[i:i+chunk_size]
            if not chunk_df.empty:
                result = ask_custom_question(
                    request.question, 
                    chunk_df, 
                    request.year_cutoff, 
                    request.topic_name
                )
                results.append(result)
        
        # Combine answers if multiple chunks
        if len(results) > 1:
            final_answer = combine_answers(
                results, 
                request.question, 
                request.topic_name, 
                request.year_cutoff
            )
        else:
            final_answer = results[0] if results else "No answer generated"
        
        return {
            "answer": final_answer,
            "question": request.question,
            "topic_name": request.topic_name,
            "year_cutoff": request.year_cutoff,
            "papers_processed": len(request.papers),
            "chunks_processed": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to answer question: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)