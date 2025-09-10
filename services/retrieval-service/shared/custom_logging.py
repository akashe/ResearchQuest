import os
import logging
from datetime import datetime
import sqlite3
from dotenv import load_dotenv


os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s\n",
    handlers=[logging.FileHandler("logs/llm_usage.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def log_llm_usage(topic_name, prompt, response, tokens_in, tokens_out):
    total_tokens = tokens_in + tokens_out
    cost_usd_input = tokens_in * float(os.environ.get("COST_PER_INPUT_TOKEN", 0))
    cost_usd_output = tokens_out * float(os.environ.get("COST_PER_OUTPUT_TOKEN", 0))

    logger.info(f"Logging LLM usage for topic '{topic_name}': {tokens_in} input tokens, {tokens_out} output tokens, total {total_tokens} tokens, cost ${cost_usd_input + cost_usd_output:.6f}")

    with sqlite3.connect("logs/llm_usage.db") as conn:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            topic TEXT,
            prompt TEXT,
            summary TEXT,
            tokens_in INTEGER,
            tokens_out INTEGER,
            total_tokens INTEGER,
            cost_usd_input REAL,
            cost_usd_output REAL
        );
        """)
        conn.commit()
        
        cursor.execute("""
        INSERT INTO usage_log (
            timestamp, topic, prompt, summary, tokens_in, tokens_out, total_tokens, cost_usd_input, cost_usd_output
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        topic_name,
        prompt,
        response,
        tokens_in,
        tokens_out,
        total_tokens,
        cost_usd_input,
        cost_usd_output
    ))
        conn.commit()