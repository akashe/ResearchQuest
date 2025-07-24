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
    handlers=[logging.FileHandler("data/llm_usage.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def log_llm_usage(topic_name, prompt, response, tokens_in, tokens_out):
    total_tokens = tokens_in + tokens_out
    cost_usd_input = tokens_in * float(os.environ.get("COST_PER_INPUT_TOKEN", 0))
    cost_usd_output = tokens_out * float(os.environ.get("COST_PER_OUTPUT_TOKEN", 0))

    with sqlite3.connect("data/llm_usage.db") as conn:
        cursor = conn.cursor()
        
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